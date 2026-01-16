// CDC subscription example
//
// This example demonstrates subscribing to change events from TiKV.
//
// Usage:
//   1. Start a TiKV cluster: `tiup playground --mode tikv-slim`
//   2. Run this example: `cargo run --example cdc_subscribe`
//   3. In another terminal, write data: `cargo run --example raw_put`

use futures::StreamExt;
use tikv_client::{CdcClient, CdcEvent, CdcOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let pd_addrs = std::env::var("PD_ADDRS").unwrap_or_else(|_| "127.0.0.1:2379".to_string());
    let pd_endpoints: Vec<&str> = pd_addrs.split(',').collect();

    println!("=== TiKV CDC Subscription Example ===\n");
    println!("Connecting to PD at {:?}", pd_endpoints);

    // Create the CDC client
    let cdc = CdcClient::new(pd_endpoints.clone()).await?;
    println!("CDC client connected successfully!");
    println!("Cluster ID: {}", cdc.cluster_id());

    // Subscribe to all changes
    println!("\nSubscribing to all key changes...");
    println!("Tip: In another terminal, run `cargo run --example raw_put` to generate events\n");
    println!("Waiting for events (Ctrl+C to stop)...\n");
    println!("{:-<60}", "");

    let mut stream = cdc.subscribe_all(CdcOptions::default()).await?;

    while let Some(event) = stream.next().await {
        match event {
            Ok(CdcEvent::Rows { region_id, rows }) => {
                for row in rows {
                    let key_bytes: &[u8] = (&row.key).into();
                    let key_str = String::from_utf8_lossy(key_bytes);
                    let value_str = row
                        .value
                        .as_ref()
                        .map(|v| String::from_utf8_lossy(v).to_string())
                        .unwrap_or_else(|| "<none>".to_string());

                    println!(
                        "[Region {:>3}] {:?} key={:?} value={:?} ts={}",
                        region_id, row.op, key_str, value_str, row.commit_ts
                    );
                }
            }
            Ok(CdcEvent::ResolvedTs { regions, ts }) => {
                // ResolvedTs indicates all events before this timestamp have been sent
                // Uncomment to see these (they're frequent):
                // println!("[ResolvedTs] ts={} regions={:?}", ts, regions);
                let _ = (regions, ts); // suppress warning
            }
            Ok(CdcEvent::Error { region_id, error }) => {
                eprintln!("[Region {}] Error: {:?}", region_id, error);
            }
            Ok(CdcEvent::Admin { region_id }) => {
                println!("[Region {}] Admin event (split/merge)", region_id);
            }
            Err(e) => {
                eprintln!("Stream error: {:?}", e);
            }
        }
    }

    println!("Stream ended");
    Ok(())
}
