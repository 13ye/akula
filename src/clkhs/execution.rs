use crate::clkhs::model::*;

use clickhouse::{Client, Row, error::Result, inserter::Inserter};
use std::time::Duration;

// TODO: switch to read with confy
static TABLE_EVENTS:&str = "tx_event_test";
static TABLE_TXMSGS:&str = "tx_message_test";
static ADDRESS:&str = "http://127.0.0.1:8123";
static USER:&str = "default";
static PASSWD:&str = "******";
static DBNAME:&str = "******";

pub async fn clkhs_init_client() -> Result<Client> {
    let client = Client::default()
          .with_url(ADDRESS)
          .with_user(USER)
          .with_password(PASSWD)
          .with_database(DBNAME);

    Ok(client)
}

pub async fn clkhs_select_id() -> Result<u64> {
    let client = clkhs_init_client().await?;
    let id = client
        .query(format!("SELECT MAX(id) FROM tx_log.{}", TABLE_EVENTS).as_str())
        .fetch_one::<u64>()
        .await?;

    println!("id() = {id}");
    Ok(id)
}

pub async fn clkhs_insert_receipts(rows: &Vec<TxReceiptLog>) -> Result<()> {
    let client = clkhs_init_client().await?;

    let mut inserter:Inserter<TxReceiptLog> = client.inserter(TABLE_EVENTS)?
      .with_max_entries(100_000_000)
      .with_period(Some(Duration::from_secs(10)))
      .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

    for i in 0..rows.len(){
        inserter.write(&rows[i]).await?;
        if i % 10_000 == 1{
            println!("commit_logs:{i} total:{}", rows.len());
            inserter.commit().await?;
        }
    }
    inserter.end().await?;

    Ok(())
}

pub async fn clkhs_insert_txmsgs(rows: &Vec<TxMessage>) -> Result<()> {
    let client = clkhs_init_client().await?;

    let mut inserter:Inserter<TxMessage> = client.inserter(TABLE_TXMSGS)?
      .with_max_entries(100_000_000)
      .with_period(Some(Duration::from_secs(10)))
      .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

    for i in 0..rows.len(){
        inserter.write(&rows[i]).await?;
        if i % 10_000 == 1{
            println!("commit_txmsgs:{i} total:{}", rows.len());
            inserter.commit().await?;
        }
    }
    inserter.end().await?;

    Ok(())
}
