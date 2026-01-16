// Transactional KV put example - writes test data using TransactionClient
// This matches what SurrealDB does internally and works with TiDb CDC API
//
// Usage:
//   1. Start a TiKV cluster: `tiup playground --mode tikv-slim`
//   2. In one terminal, run CDC subscriber: `cargo run --example cdc_subscribe`
//   3. In another terminal, run this: `cargo run --example txn_put`

use tikv_client::TransactionClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pd_addrs = std::env::var("PD_ADDRS").unwrap_or_else(|_| "127.0.0.1:2379".to_string());
    let pd_endpoints: Vec<&str> = pd_addrs.split(',').collect();

    println!("Connecting to PD at {:?}", pd_endpoints);
    let client = TransactionClient::new(pd_endpoints).await?;
    println!("Connected!\n");

    // Write some test data using transactions (like SurrealDB does)
    println!("Writing test data with transactions...\n");

    // Transaction 1: Create users
    {
        let mut txn = client.begin_optimistic().await?;
        txn.put("user:1".to_owned(), "Alice".to_owned()).await?;
        txn.put("user:2".to_owned(), "Bob".to_owned()).await?;
        txn.put("user:3".to_owned(), "Charlie".to_owned()).await?;
        txn.commit().await?;
        println!("  Transaction 1: Created users 1, 2, 3");
    }

    // Transaction 2: Create config
    {
        let mut txn = client.begin_optimistic().await?;
        txn.put("config:theme".to_owned(), "dark".to_owned())
            .await?;
        txn.put("config:lang".to_owned(), "en".to_owned()).await?;
        txn.commit().await?;
        println!("  Transaction 2: Created config entries");
    }

    // Transaction 3: Update users
    {
        let mut txn = client.begin_optimistic().await?;
        txn.put("user:1".to_owned(), "Alice Updated".to_owned())
            .await?;
        txn.put("user:2".to_owned(), "Bob Modified".to_owned())
            .await?;
        txn.commit().await?;
        println!("  Transaction 3: Updated users 1 and 2");
    }

    // Transaction 4: Delete user
    {
        let mut txn = client.begin_optimistic().await?;
        txn.delete("user:3".to_owned()).await?;
        txn.commit().await?;
        println!("  Transaction 4: Deleted user 3");
    }

    // Read back to verify
    println!("\nReading back data...\n");
    {
        let mut snapshot = client.snapshot(client.current_timestamp().await?, Default::default());
        for key in ["user:1", "user:2", "user:3", "config:theme", "config:lang"] {
            let value = snapshot.get(key.to_owned()).await?;
            match value {
                Some(v) => println!("  {} = {}", key, String::from_utf8_lossy(&v)),
                None => println!("  {} = <deleted>", key),
            }
        }
    }

    println!("\nDone! Check the CDC subscriber output for events.");
    Ok(())
}
