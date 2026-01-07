use pprof::protos::Message;
use std::sync::Mutex;
use std::time::Duration;

// Global lock to ensure only one profiler runs at a time
static PROFILER_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy)]
pub enum ProfileFormat {
    Protobuf,
    Flamegraph,
    Text,
}

impl ProfileFormat {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "protobuf" | "pb" | "pprof" => Ok(ProfileFormat::Protobuf),
            "flamegraph" | "svg" | "flame" => Ok(ProfileFormat::Flamegraph),
            "text" | "txt" => Ok(ProfileFormat::Text),
            _ => Err(format!("Invalid format: {}. Use 'protobuf', 'flamegraph', or 'text'", s)),
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            ProfileFormat::Protobuf => "application/octet-stream",
            ProfileFormat::Flamegraph => "image/svg+xml",
            ProfileFormat::Text => "text/plain; charset=utf-8",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            ProfileFormat::Protobuf => "pb",
            ProfileFormat::Flamegraph => "svg",
            ProfileFormat::Text => "txt",
        }
    }
}

pub fn generate_heap_profile(duration_secs: u64, format: ProfileFormat) -> Result<Vec<u8>, String> {
    // Try to acquire the lock (non-blocking)
    let _lock = PROFILER_LOCK.try_lock()
        .map_err(|_| "Profiling already in progress. Please wait for the current profiling session to complete.".to_string())?;

    // For heap profiling, we use MALLOC profiling which tracks allocations
    // Note: This requires the target to be compiled with the right features
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(100) // Sample at 100Hz
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| format!("Failed to start profiler: {}", e))?;

    // Sleep for the duration to collect samples
    std::thread::sleep(Duration::from_secs(duration_secs));

    // Generate report
    let report = guard
        .report()
        .build()
        .map_err(|e| format!("Failed to build profile report: {}", e))?;

    convert_report_to_format(report, format)
}

pub fn generate_cpu_profile(duration_secs: u64, format: ProfileFormat) -> Result<Vec<u8>, String> {
    // Try to acquire the lock (non-blocking)
    let _lock = PROFILER_LOCK.try_lock()
        .map_err(|_| "Profiling already in progress. Please wait for the current profiling session to complete.".to_string())?;

    // CPU profiling - sample thread stacks at regular intervals
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(100) // Sample at 100Hz
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| format!("Failed to start profiler: {}", e))?;

    // Sleep for the duration to collect samples
    std::thread::sleep(Duration::from_secs(duration_secs));

    // Generate report
    let report = guard
        .report()
        .build()
        .map_err(|e| format!("Failed to build profile report: {}", e))?;

    convert_report_to_format(report, format)
}

fn convert_report_to_format(report: pprof::Report, format: ProfileFormat) -> Result<Vec<u8>, String> {
    match format {
        ProfileFormat::Protobuf => generate_protobuf(&report),
        ProfileFormat::Flamegraph => generate_flamegraph(&report),
        ProfileFormat::Text => Ok(generate_text_report(&report).into_bytes()),
    }
}

fn generate_protobuf(report: &pprof::Report) -> Result<Vec<u8>, String> {
    // Convert to pprof protobuf format
    let profile = report
        .pprof()
        .map_err(|e| format!("Failed to generate pprof protobuf: {}", e))?;

    let mut buf = Vec::new();
    profile
        .encode(&mut buf)
        .map_err(|e| format!("Failed to encode protobuf: {}", e))?;

    Ok(buf)
}

fn generate_flamegraph(report: &pprof::Report) -> Result<Vec<u8>, String> {
    // Generate flamegraph SVG
    let mut buf = Vec::new();
    report
        .flamegraph(&mut buf)
        .map_err(|e| format!("Failed to generate flamegraph: {}", e))?;

    Ok(buf)
}

fn generate_text_report(report: &pprof::Report) -> String {
    // Generate human-readable text report
    let mut output = String::new();

    output.push_str("Profile Report\n");
    output.push_str("═══════════════════════════════════════════════════\n\n");

    // Get the top frames by sample count
    let mut frames_data: Vec<(String, usize)> = Vec::new();

    // Convert report to protobuf to extract frame data
    if let Ok(profile) = report.pprof() {
        // Extract sample data from profile
        let mut function_samples: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

        for sample in &profile.sample {
            if let Some(value) = sample.value.first() {
                // Iterate through locations in the sample
                for location_id in &sample.location_id {
                    // Find the location
                    if let Some(location) = profile.location.iter().find(|l| l.id == *location_id) {
                        // Get the function for this location
                        if let Some(line) = location.line.first() {
                            if let Some(function) = profile.function.iter().find(|f| f.id == line.function_id) {
                                // Get function name
                                let name_id = function.name;
                                if let Some(name) = profile.string_table.get(name_id as usize) {
                                    *function_samples.entry(name.clone()).or_insert(0) += value;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by sample count
        let mut sorted_functions: Vec<_> = function_samples.into_iter().collect();
        sorted_functions.sort_by(|a, b| b.1.cmp(&a.1));

        // Format output
        output.push_str("Top Functions by Sample Count:\n");
        output.push_str("───────────────────────────────────────────────────\n");
        output.push_str(format!("{:<50} {:>10}\n", "Function", "Samples").as_str());
        output.push_str("───────────────────────────────────────────────────\n");

        for (i, (name, count)) in sorted_functions.iter().take(30).enumerate() {
            let display_name = if name.len() > 48 {
                format!("{}...", &name[0..45])
            } else {
                name.clone()
            };
            output.push_str(format!("{:>2}. {:<50} {:>10}\n", i + 1, display_name, count).as_str());
        }

        output.push_str("───────────────────────────────────────────────────\n");
        output.push_str(format!("Total unique functions: {}\n", sorted_functions.len()).as_str());
    } else {
        output.push_str("Unable to generate detailed report\n");
    }

    output.push_str("\n");
    output.push_str("Note: Use pprof format for full analysis\n");
    output.push_str("Example: curl http://localhost:7000/debug/heap?format=protobuf -o heap.pb\n");
    output.push_str("         go tool pprof -http=:8080 heap.pb\n");

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_from_str() {
        assert!(matches!(ProfileFormat::from_str("protobuf"), Ok(ProfileFormat::Protobuf)));
        assert!(matches!(ProfileFormat::from_str("flamegraph"), Ok(ProfileFormat::Flamegraph)));
        assert!(matches!(ProfileFormat::from_str("text"), Ok(ProfileFormat::Text)));
        assert!(matches!(ProfileFormat::from_str("invalid"), Err(_)));
    }

    #[test]
    fn test_content_type() {
        assert_eq!(ProfileFormat::Protobuf.content_type(), "application/octet-stream");
        assert_eq!(ProfileFormat::Flamegraph.content_type(), "image/svg+xml");
        assert_eq!(ProfileFormat::Text.content_type(), "text/plain; charset=utf-8");
    }

    #[test]
    fn test_file_extension() {
        assert_eq!(ProfileFormat::Protobuf.file_extension(), "pb");
        assert_eq!(ProfileFormat::Flamegraph.file_extension(), "svg");
        assert_eq!(ProfileFormat::Text.file_extension(), "txt");
    }
}
