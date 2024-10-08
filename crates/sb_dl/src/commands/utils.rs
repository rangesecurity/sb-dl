use std::collections::HashSet;

use serde_json::Value;

// sanitizes utf8 encoding issues which prevent converting serde_json::Value to a string
// this is done before failed blocks are persisted to disk
pub fn sanitize_value(value: &mut Value) {
    match value {
        Value::String(s) => {
            // Check if the string contains valid UTF-8
            if let Err(_) = std::str::from_utf8(s.as_bytes()) {
                // Replace invalid UTF-8 with a placeholder
                *s = String::from_utf8_lossy(s.as_bytes()).into_owned();
            }
        }
        Value::Array(arr) => {
            for v in arr {
                sanitize_value(v);
            }
        }
        Value::Object(map) => {
            for (_, v) in map.iter_mut() {
                sanitize_value(v);
            }
        }
        _ => {}
    }
}

// reads all files from the failed_blocks directory, and retrieves the block numbers
pub async fn get_failed_blocks(dir: &str) -> anyhow::Result<HashSet<u64>> {
    use regex::Regex;
    use std::collections::HashSet;
    use std::path::Path;
    let dir_path = Path::new(dir);
    let re = Regex::new(r"block_(\d+)\.json").unwrap();
    let mut hash_set = HashSet::new();

    let entries = tokio::fs::read_dir(dir_path).await?;
    tokio::pin!(entries);

    while let Some(entry) = entries.next_entry().await? {
        if let Some(file_name) = entry.file_name().to_str() {
            if let Some(captures) = re.captures(file_name) {
                if let Some(matched) = captures.get(1) {
                    if let Ok(number) = matched.as_str().parse::<u64>() {
                        hash_set.insert(number);
                    }
                }
            }
        }
    }
    Ok(hash_set)
}

pub async fn load_failed_blocks(
    dir: &str,
    blocks_tx: tokio::sync::mpsc::Sender<(u64, serde_json::Value)>,
) -> anyhow::Result<()> {
    use regex::Regex;

    let re = Regex::new(r"block_(\d+)\.json").unwrap();
    let entries = tokio::fs::read_dir(dir).await?;
    tokio::pin!(entries);

    while let Some(entry) = entries.next_entry().await? {
        if let Some(file_name) = entry.file_name().to_str() {
            if let Some(captures) = re.captures(file_name) {
                if let Some(matched) = captures.get(1) {
                    if let Ok(slot) = matched.as_str().parse::<u64>() {
                        let block = tokio::fs::read_to_string(entry.path()).await?;
                        let block: serde_json::Value = serde_json::from_str(&block)?;
                        if let Err(err) = blocks_tx.send((slot, block)).await {
                            log::error!("failed to notify block({slot}) {err:#?}");
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn sanitize_for_postgres(value: &mut Value) {
    match value {
        Value::String(ref mut s) => {
            *s = s.replace("\u{0000}", "");
        }
        Value::Array(ref mut arr) => {
            for item in arr {
                sanitize_for_postgres(item);
            }
        }
        Value::Object(ref mut obj) => {
            for (_key, val) in obj.iter_mut() {
                sanitize_for_postgres(val);
            }
        }
        _ => {}
    }
}
