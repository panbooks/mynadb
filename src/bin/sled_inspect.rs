//! Simple sled database inspection tool
//!
//! Usage: cargo run --bin sled_inspect <db_path> [prefix]

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <db_path> [prefix]", args[0]);
        eprintln!(
            "Example: {} ./dev/data/coldstandby-simple/meta1/sled /nodes/",
            args[0]
        );
        std::process::exit(1);
    }

    let db_path = &args[1];
    let prefix = args.get(2).map(|s| s.as_str()).unwrap_or("");

    println!("Opening sled database: {}", db_path);

    let db = match sled::open(db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to open database: {}", e);
            std::process::exit(1);
        }
    };

    println!("Database opened successfully");
    println!("Size on disk: {:?} bytes", db.size_on_disk());
    println!("Tree names: {:?}", db.tree_names());
    println!();

    if prefix.is_empty() {
        println!("Scanning all keys...");
    } else {
        println!("Scanning keys with prefix: {:?}", prefix);
    }
    println!("----------------------------------------");

    let mut count = 0;
    let iter = if prefix.is_empty() {
        db.iter()
    } else {
        db.scan_prefix(prefix)
    };

    for item in iter {
        match item {
            Ok((key, value)) => {
                count += 1;
                let key_str = String::from_utf8_lossy(&key);

                // Try to determine value type and decode
                println!("Key: {}", key_str);
                println!("  Value length: {} bytes", value.len());

                // Try to decode as NodeInfo if it looks like a node key
                if key_str.starts_with("/nodes/") {
                    if let Ok(node) = bincode::deserialize::<mynadb::meta::types::NodeInfo>(&value)
                    {
                        println!("  Decoded NodeInfo:");
                        println!("    node_id: {}", node.node_id);
                        println!("    role: {:?}", node.role);
                        println!("    state: {:?}", node.state);
                        println!("    mode: {:?}", node.mode);
                        println!("    advertise_addr: {}", node.advertise_addr);
                    } else {
                        println!("  (Could not decode as NodeInfo)");
                        // Show raw hex
                        if value.len() <= 100 {
                            println!("  Raw hex: {:02x?}", value.as_ref());
                        }
                    }
                }
                // Try to decode as PartitionReplica
                else if key_str.starts_with("/replicas/") {
                    if let Ok(replica) =
                        bincode::deserialize::<mynadb::meta::types::PartitionReplica>(&value)
                    {
                        println!("  Decoded PartitionReplica:");
                        println!("    partition_key: {}", replica.partition_key);
                        println!("    leader: {}", replica.leader_node_id);
                    } else {
                        println!("  (Could not decode as PartitionReplica)");
                    }
                }
                // Show raw for other keys
                else {
                    if value.len() <= 50 {
                        // Try as string
                        if let Ok(s) = std::str::from_utf8(&value) {
                            println!("  Value (string): {}", s);
                        } else {
                            println!("  Value (hex): {:02x?}", value.as_ref());
                        }
                    } else {
                        // Try to decode as u64
                        if value.len() == 8 {
                            if let Ok(n) = bincode::deserialize::<u64>(&value) {
                                println!("  Value (u64): {}", n);
                            }
                        }
                    }
                }
                println!();
            }
            Err(e) => {
                eprintln!("Error reading entry: {}", e);
            }
        }
    }

    println!("----------------------------------------");
    println!("Total entries scanned: {}", count);

    // Also check specific node keys directly
    println!();
    println!("Direct key lookups:");
    for node_id in ["data-1", "data-2", "data-3", "data-4", "router-1"] {
        let key = format!("/nodes/{}", node_id);
        match db.get(key.as_bytes()) {
            Ok(Some(value)) => {
                println!("  {} -> {} bytes", key, value.len());
                if let Ok(node) = bincode::deserialize::<mynadb::meta::types::NodeInfo>(&value) {
                    println!("    Decoded: role={:?}, state={:?}", node.role, node.state);
                }
            }
            Ok(None) => {
                println!("  {} -> NOT FOUND", key);
            }
            Err(e) => {
                println!("  {} -> ERROR: {}", key, e);
            }
        }
    }
}
