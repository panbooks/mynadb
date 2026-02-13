//! Built-in system metrics collector
//!
//! Collects host CPU, memory, disk, and network metrics using the `sysinfo` crate
//! and writes them into the configured database (default: `host_metrics`) via the
//! DataCache write path. This gives users a zero-install monitoring experience.
//!
//! Measurements (Telegraf-compatible field names):
//! - `cpu`    — per-CPU and aggregate: usage_percent, frequency, user/system/idle/iowait/nice/irq/softirq/steal (Linux)
//! - `mem`    — total, used, available, swap
//! - `disk`   — per-mount usage
//! - `diskio` — per-disk read/write bytes and rates
//! - `net`    — per-interface bytes/packets in/out (rates) + errors
//! - `system` — uptime, load averages, process count
//! - `temp`   — hardware component temperatures (CPU, GPU, etc.)

use crate::config::Config;
use crate::storage::cache::DataCache;
use crate::storage::line_protocol::{FieldValue, Point};
use std::collections::HashMap;
use std::sync::Arc;
use sysinfo::{Components, Disks, Networks, System};
use tokio::time::{interval, Duration};
use tracing::{debug, warn};

/// Raw CPU time counters from /proc/stat (Linux)
#[derive(Clone, Default)]
struct CpuTimes {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,
}

impl CpuTimes {
    fn total(&self) -> u64 {
        self.user + self.nice + self.system + self.idle + self.iowait + self.irq + self.softirq + self.steal
    }

    /// Compute percentage breakdown from delta between two snapshots
    fn pct_fields(&self, prev: &CpuTimes) -> HashMap<String, FieldValue> {
        let dt = self.total().saturating_sub(prev.total()) as f64;
        if dt == 0.0 {
            return HashMap::new();
        }
        let mut f = HashMap::new();
        f.insert("usage_user".to_string(), FieldValue::Float((self.user.saturating_sub(prev.user)) as f64 / dt * 100.0));
        f.insert("usage_nice".to_string(), FieldValue::Float((self.nice.saturating_sub(prev.nice)) as f64 / dt * 100.0));
        f.insert("usage_system".to_string(), FieldValue::Float((self.system.saturating_sub(prev.system)) as f64 / dt * 100.0));
        f.insert("usage_idle".to_string(), FieldValue::Float((self.idle.saturating_sub(prev.idle)) as f64 / dt * 100.0));
        f.insert("usage_iowait".to_string(), FieldValue::Float((self.iowait.saturating_sub(prev.iowait)) as f64 / dt * 100.0));
        f.insert("usage_irq".to_string(), FieldValue::Float((self.irq.saturating_sub(prev.irq)) as f64 / dt * 100.0));
        f.insert("usage_softirq".to_string(), FieldValue::Float((self.softirq.saturating_sub(prev.softirq)) as f64 / dt * 100.0));
        f.insert("usage_steal".to_string(), FieldValue::Float((self.steal.saturating_sub(prev.steal)) as f64 / dt * 100.0));
        f
    }
}

/// Parse /proc/stat → HashMap<cpu_name, CpuTimes>. Returns empty on non-Linux.
fn read_proc_stat() -> HashMap<String, CpuTimes> {
    let mut result = HashMap::new();
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/stat") {
            for line in content.lines() {
                if !line.starts_with("cpu") {
                    continue;
                }
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() < 8 {
                    continue;
                }
                let name = parts[0].to_string(); // "cpu", "cpu0", "cpu1", ...
                let parse = |i: usize| -> u64 { parts.get(i).and_then(|s| s.parse().ok()).unwrap_or(0) };
                result.insert(name, CpuTimes {
                    user: parse(1),
                    nice: parse(2),
                    system: parse(3),
                    idle: parse(4),
                    iowait: parse(5),
                    irq: parse(6),
                    softirq: parse(7),
                    steal: parse(8),
                });
            }
        }
    }
    result
}

/// Entry point — spawned as a background task from standalone.rs
pub async fn start(cache: Arc<DataCache>, config: Arc<Config>) {
    let interval_secs = config.system_metrics.interval_secs;
    let database = config.system_metrics.database.clone();
    let collect_disk = config.system_metrics.collect_disk;
    let collect_net = config.system_metrics.collect_net;

    let mut sys = System::new_all();
    let mut disks = if collect_disk {
        Some(Disks::new_with_refreshed_list())
    } else {
        None
    };
    let mut networks = if collect_net {
        Some(Networks::new_with_refreshed_list())
    } else {
        None
    };

    let mut components = Components::new_with_refreshed_list();

    // Previous network counters for rate calculation
    let mut prev_net: HashMap<String, (u64, u64, u64, u64)> = HashMap::new();
    // Previous disk I/O counters for rate calculation
    let mut prev_diskio: HashMap<String, (u64, u64)> = HashMap::new();
    // Previous CPU time counters for breakdown percentages (Linux /proc/stat)
    let mut prev_cpu_times: HashMap<String, CpuTimes>;

    let hostname = System::host_name().unwrap_or_else(|| "unknown".to_string());

    // Short baseline warmup (2s) instead of full interval — first data appears in ~2+interval_secs
    // sysinfo needs at least one prior refresh to compute CPU deltas accurately.
    tokio::time::sleep(Duration::from_secs(2)).await;
    sys.refresh_all();
    if let Some(ref mut d) = disks {
        d.refresh(false);
        // Seed previous disk I/O counters
        for disk in d.iter() {
            let name = disk.name().to_string_lossy().to_string();
            let usage = disk.usage();
            prev_diskio.insert(name, (usage.total_read_bytes, usage.total_written_bytes));
        }
    }
    if let Some(ref mut n) = networks {
        n.refresh(false);
        // Seed previous network counters
        for (name, data) in n.iter() {
            prev_net.insert(
                name.to_string(),
                (
                    data.total_received(),
                    data.total_transmitted(),
                    data.total_packets_received(),
                    data.total_packets_transmitted(),
                ),
            );
        }
    }

    // Seed CPU times baseline
    prev_cpu_times = read_proc_stat();

    let mut ticker = interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // consume immediate first tick

    loop {
        ticker.tick().await;

        // Refresh system information
        sys.refresh_all();

        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let date = (now_ns / 86_400_000_000_000) as i32; // days since epoch

        let mut points: Vec<(&str, Vec<Point>)> = Vec::new();

        // ── CPU metrics ──
        let cur_cpu_times = read_proc_stat();
        let mut cpu_points = Vec::new();
        let cpus = sys.cpus();
        let mut total_usage = 0.0_f64;
        let mut total_freq = 0_u64;
        for (i, cpu) in cpus.iter().enumerate() {
            let usage = cpu.cpu_usage() as f64;
            let freq = cpu.frequency();
            total_usage += usage;
            total_freq += freq;
            let cpu_tag = format!("cpu{}", i);
            let mut tags = HashMap::new();
            tags.insert("host".to_string(), hostname.clone());
            tags.insert("cpu".to_string(), cpu_tag.clone());
            let mut fields = HashMap::new();
            fields.insert("usage_percent".to_string(), FieldValue::Float(usage));
            fields.insert("frequency_mhz".to_string(), FieldValue::Integer(freq as i64));
            // Merge user/system/idle/iowait/nice/irq/softirq/steal from /proc/stat
            if let Some(cur) = cur_cpu_times.get(&cpu_tag) {
                let prev = prev_cpu_times.get(&cpu_tag).cloned().unwrap_or_default();
                fields.extend(cur.pct_fields(&prev));
            }
            cpu_points.push(Point {
                measurement: "cpu".to_string(),
                tags,
                fields,
                timestamp: Some(now_ns),
                date,
                service: "default".to_string(),
            });
        }
        // Aggregate CPU point
        let n_cpus = cpus.len();
        if n_cpus > 0 {
            let avg = total_usage / n_cpus as f64;
            let avg_freq = total_freq / n_cpus as u64;
            let mut tags = HashMap::new();
            tags.insert("host".to_string(), hostname.clone());
            tags.insert("cpu".to_string(), "cpu-total".to_string());
            let mut fields = HashMap::new();
            fields.insert("usage_percent".to_string(), FieldValue::Float(avg));
            fields.insert("num_cpus".to_string(), FieldValue::Integer(n_cpus as i64));
            fields.insert(
                "frequency_mhz".to_string(),
                FieldValue::Integer(avg_freq as i64),
            );
            // Aggregate breakdown from /proc/stat "cpu" line (total across all cores)
            if let Some(cur) = cur_cpu_times.get("cpu") {
                let prev = prev_cpu_times.get("cpu").cloned().unwrap_or_default();
                fields.extend(cur.pct_fields(&prev));
            }
            cpu_points.push(Point {
                measurement: "cpu".to_string(),
                tags,
                fields,
                timestamp: Some(now_ns),
                date,
                service: "default".to_string(),
            });
        }
        prev_cpu_times = cur_cpu_times;
        points.push(("cpu", cpu_points));

        // ── Memory metrics ──
        let total_mem = sys.total_memory();
        let used_mem = sys.used_memory();
        let available_mem = sys.available_memory();
        let total_swap = sys.total_swap();
        let used_swap = sys.used_swap();
        let usage_pct = if total_mem > 0 {
            (used_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), hostname.clone());
        let mut fields = HashMap::new();
        fields.insert("total".to_string(), FieldValue::Integer(total_mem as i64));
        fields.insert("used".to_string(), FieldValue::Integer(used_mem as i64));
        fields.insert(
            "available".to_string(),
            FieldValue::Integer(available_mem as i64),
        );
        fields.insert("usage_percent".to_string(), FieldValue::Float(usage_pct));
        fields.insert(
            "swap_total".to_string(),
            FieldValue::Integer(total_swap as i64),
        );
        fields.insert(
            "swap_used".to_string(),
            FieldValue::Integer(used_swap as i64),
        );
        points.push((
            "mem",
            vec![Point {
                measurement: "mem".to_string(),
                tags,
                fields,
                timestamp: Some(now_ns),
                date,
                service: "default".to_string(),
            }],
        ));

        // ── Disk metrics ──
        if let Some(ref mut d) = disks {
            d.refresh(false);
            let mut disk_points = Vec::new();
            let mut diskio_points = Vec::new();
            for disk in d.iter() {
                let mount = disk.mount_point().to_string_lossy().to_string();
                let disk_name = disk.name().to_string_lossy().to_string();
                let total = disk.total_space();
                let available = disk.available_space();
                let used = total.saturating_sub(available);
                let pct = if total > 0 {
                    (used as f64 / total as f64) * 100.0
                } else {
                    0.0
                };

                let mut tags = HashMap::new();
                tags.insert("host".to_string(), hostname.clone());
                tags.insert("mount".to_string(), mount);
                tags.insert(
                    "fstype".to_string(),
                    disk.file_system().to_string_lossy().to_string(),
                );
                let mut fields = HashMap::new();
                fields.insert("total".to_string(), FieldValue::Integer(total as i64));
                fields.insert("used".to_string(), FieldValue::Integer(used as i64));
                fields.insert(
                    "available".to_string(),
                    FieldValue::Integer(available as i64),
                );
                fields.insert("usage_percent".to_string(), FieldValue::Float(pct));
                disk_points.push(Point {
                    measurement: "disk".to_string(),
                    tags,
                    fields,
                    timestamp: Some(now_ns),
                    date,
                    service: "default".to_string(),
                });

                // ── Disk I/O metrics ──
                let usage = disk.usage();
                let read_bytes = usage.total_read_bytes;
                let write_bytes = usage.total_written_bytes;

                let (read_rate, write_rate) =
                    if let Some(&(prev_r, prev_w)) = prev_diskio.get(&disk_name) {
                        let dt = interval_secs as f64;
                        (
                            read_bytes.saturating_sub(prev_r) as f64 / dt,
                            write_bytes.saturating_sub(prev_w) as f64 / dt,
                        )
                    } else {
                        (0.0, 0.0)
                    };

                prev_diskio.insert(disk_name.clone(), (read_bytes, write_bytes));

                let mut tags = HashMap::new();
                tags.insert("host".to_string(), hostname.clone());
                tags.insert("name".to_string(), disk_name);
                let mut fields = HashMap::new();
                fields.insert(
                    "read_bytes".to_string(),
                    FieldValue::Integer(read_bytes as i64),
                );
                fields.insert(
                    "write_bytes".to_string(),
                    FieldValue::Integer(write_bytes as i64),
                );
                fields.insert(
                    "reads_per_sec".to_string(),
                    FieldValue::Integer(usage.read_bytes as i64),
                );
                fields.insert(
                    "writes_per_sec".to_string(),
                    FieldValue::Integer(usage.written_bytes as i64),
                );
                fields.insert(
                    "read_bytes_rate".to_string(),
                    FieldValue::Float(read_rate),
                );
                fields.insert(
                    "write_bytes_rate".to_string(),
                    FieldValue::Float(write_rate),
                );
                diskio_points.push(Point {
                    measurement: "diskio".to_string(),
                    tags,
                    fields,
                    timestamp: Some(now_ns),
                    date,
                    service: "default".to_string(),
                });
            }
            points.push(("disk", disk_points));
            points.push(("diskio", diskio_points));
        }

        // ── Network metrics ──
        if let Some(ref mut n) = networks {
            n.refresh(false);
            let mut net_points = Vec::new();
            for (name, data) in n.iter() {
                let rx_bytes = data.total_received();
                let tx_bytes = data.total_transmitted();
                let rx_packets = data.total_packets_received();
                let tx_packets = data.total_packets_transmitted();

                // Calculate rates from previous counters
                let (rx_rate, tx_rate) = if let Some(&(prev_rx, prev_tx, _, _)) =
                    prev_net.get(name.as_str())
                {
                    let dt = interval_secs as f64;
                    (
                        (rx_bytes.saturating_sub(prev_rx)) as f64 / dt,
                        (tx_bytes.saturating_sub(prev_tx)) as f64 / dt,
                    )
                } else {
                    (0.0, 0.0)
                };

                prev_net.insert(
                    name.to_string(),
                    (rx_bytes, tx_bytes, rx_packets, tx_packets),
                );

                let mut tags = HashMap::new();
                tags.insert("host".to_string(), hostname.clone());
                tags.insert("interface".to_string(), name.to_string());
                let mut fields = HashMap::new();
                fields.insert(
                    "bytes_recv".to_string(),
                    FieldValue::Integer(rx_bytes as i64),
                );
                fields.insert(
                    "bytes_sent".to_string(),
                    FieldValue::Integer(tx_bytes as i64),
                );
                fields.insert(
                    "packets_recv".to_string(),
                    FieldValue::Integer(rx_packets as i64),
                );
                fields.insert(
                    "packets_sent".to_string(),
                    FieldValue::Integer(tx_packets as i64),
                );
                fields.insert(
                    "bytes_recv_rate".to_string(),
                    FieldValue::Float(rx_rate),
                );
                fields.insert(
                    "bytes_sent_rate".to_string(),
                    FieldValue::Float(tx_rate),
                );
                fields.insert(
                    "errors_recv".to_string(),
                    FieldValue::Integer(data.total_errors_on_received() as i64),
                );
                fields.insert(
                    "errors_sent".to_string(),
                    FieldValue::Integer(data.total_errors_on_transmitted() as i64),
                );
                net_points.push(Point {
                    measurement: "net".to_string(),
                    tags,
                    fields,
                    timestamp: Some(now_ns),
                    date,
                    service: "default".to_string(),
                });
            }
            points.push(("net", net_points));
        }

        // ── System metrics (uptime, load, processes) ──
        let load_avg = System::load_average();
        let process_count = sys.processes().len();
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), hostname.clone());
        let mut fields = HashMap::new();
        fields.insert(
            "uptime".to_string(),
            FieldValue::Integer(System::uptime() as i64),
        );
        fields.insert("load1".to_string(), FieldValue::Float(load_avg.one));
        fields.insert("load5".to_string(), FieldValue::Float(load_avg.five));
        fields.insert("load15".to_string(), FieldValue::Float(load_avg.fifteen));
        fields.insert(
            "n_cpus".to_string(),
            FieldValue::Integer(sys.cpus().len() as i64),
        );
        fields.insert(
            "processes_total".to_string(),
            FieldValue::Integer(process_count as i64),
        );
        points.push((
            "system",
            vec![Point {
                measurement: "system".to_string(),
                tags,
                fields,
                timestamp: Some(now_ns),
                date,
                service: "default".to_string(),
            }],
        ));

        // ── Temperature metrics (hardware components) ──
        components.refresh(false);
        let mut temp_points = Vec::new();
        for comp in components.iter() {
            if let Some(temp) = comp.temperature() {
                let label = comp.label().to_string();
                let mut tags = HashMap::new();
                tags.insert("host".to_string(), hostname.clone());
                tags.insert("sensor".to_string(), label);
                let mut fields = HashMap::new();
                fields.insert("temperature".to_string(), FieldValue::Float(temp as f64));
                if let Some(max) = comp.max() {
                    fields.insert("max".to_string(), FieldValue::Float(max as f64));
                }
                temp_points.push(Point {
                    measurement: "temp".to_string(),
                    tags,
                    fields,
                    timestamp: Some(now_ns),
                    date,
                    service: "default".to_string(),
                });
            }
        }
        if !temp_points.is_empty() {
            points.push(("temp", temp_points));
        }

        // ── Write all points to cache ──
        let mut total_written = 0usize;
        for (table, pts) in points {
            let count = pts.len();
            if let Err(e) = cache.add_points(&database, "default", table, pts).await {
                warn!("System metrics: failed to write {}: {}", table, e);
            } else {
                total_written += count;
            }
        }

        debug!(
            "System metrics: wrote {} points to '{}'",
            total_written, database
        );
    }
}
