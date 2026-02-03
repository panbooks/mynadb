//! Quick 10M entry sled benchmark
use std::time::Instant;

fn main() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = sled::Config::new()
        .path(dir.path())
        .cache_capacity(512 * 1024 * 1024) // 512MB cache
        .flush_every_ms(Some(1000))
        .open()
        .unwrap();

    println!("=== Sled 10M Entry Benchmark ===\n");

    // Insert 10M entries
    println!("Inserting 10M entries...");
    let start = Instant::now();
    for i in 0..10_000_000u64 {
        let key = format!(
            "/tables/catalog_{}/schema_{}/table_{}",
            i % 100,
            i % 1000,
            i
        );
        let value = format!(
            r#"{{"owner":"node-{}","version":{},"bytes":{}}}"#,
            i % 16,
            i % 100,
            i * 1024
        );
        let _ = db.insert(key.as_bytes(), value.as_bytes());

        if i > 0 && i % 1_000_000 == 0 {
            let elapsed = start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!(
                "  {}M entries: {:.1}s ({:.0}K inserts/sec)",
                i / 1_000_000,
                elapsed.as_secs_f64(),
                rate / 1000.0
            );
        }
    }
    let _ = db.flush();
    let insert_time = start.elapsed();
    println!("\nInsert complete:");
    println!("  Total time: {:.2}s", insert_time.as_secs_f64());
    println!(
        "  Throughput: {:.0}K inserts/sec",
        10_000.0 / insert_time.as_secs_f64()
    );
    println!(
        "  Per-entry: {:.2} µs",
        insert_time.as_micros() as f64 / 10_000_000.0
    );

    // Random lookups
    println!("\nRunning 10K random lookups...");
    let lookup_keys: Vec<String> = (0..10_000u64)
        .map(|i| {
            let idx = (i * 997) % 10_000_000;
            format!(
                "/tables/catalog_{}/schema_{}/table_{}",
                idx % 100,
                idx % 1000,
                idx
            )
        })
        .collect();

    let start = Instant::now();
    for key in &lookup_keys {
        let _ = db.get(key.as_bytes());
    }
    let lookup_time = start.elapsed();
    println!("\nLookup complete:");
    println!(
        "  10K lookups: {:.2} ms",
        lookup_time.as_micros() as f64 / 1000.0
    );
    println!(
        "  Throughput: {:.2}M lookups/sec",
        10.0 / lookup_time.as_secs_f64()
    );
    println!(
        "  Per-lookup: {:.2} µs",
        lookup_time.as_micros() as f64 / 10_000.0
    );

    // Prefix scan - all tables for one catalog (~100K entries)
    println!("\nRunning prefix scan (catalog_0, ~100K entries)...");
    let prefix = "/tables/catalog_0/";
    let start = Instant::now();
    let mut count = 0;
    for item in db.scan_prefix(prefix.as_bytes()) {
        if item.is_ok() {
            count += 1;
        }
    }
    let scan_time = start.elapsed();
    println!("\nPrefix scan complete:");
    println!("  Found: {} entries", count);
    println!("  Time: {:.2} ms", scan_time.as_micros() as f64 / 1000.0);
    println!(
        "  Throughput: {:.2}M entries/sec",
        count as f64 / scan_time.as_secs_f64() / 1_000_000.0
    );

    // Full scan - all 10M entries
    println!("\nRunning full scan (10M entries)...");
    let start = Instant::now();
    let mut count = 0;
    for item in db.scan_prefix("/tables/".as_bytes()) {
        if item.is_ok() {
            count += 1;
        }
    }
    let full_scan_time = start.elapsed();
    println!("\nFull scan complete:");
    println!("  Found: {} entries", count);
    println!("  Time: {:.2} ms", full_scan_time.as_millis());
    println!(
        "  Throughput: {:.2}M entries/sec",
        count as f64 / full_scan_time.as_secs_f64() / 1_000_000.0
    );

    // DB size
    drop(db);
    let mut total_size = 0u64;
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        if let Ok(entry) = entry {
            if let Ok(metadata) = entry.metadata() {
                total_size += metadata.len();
            }
        }
    }
    println!("\nDB Size:");
    println!("  Total: {:.2} MB", total_size as f64 / (1024.0 * 1024.0));
    println!("  Per-entry: {:.0} bytes", total_size as f64 / 10_000_000.0);
}
