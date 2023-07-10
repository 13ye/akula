use crate::clkhs::model::*;

use clickhouse::{Client, Row, error::Result, inserter::Inserter};
use ethereum_jsonrpc::types::Tx;
use std::time::Duration;

// TODO: switch to read with confy
static TABLE_EVENTS:&str = "tx_event";
static TABLE_TXMSGS:&str = "tx_message";
static TABLE_EVENTS3:&str = "tx_event_test3";
static TABLE_TXMSGS3:&str = "tx_message_test3";
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

    let mut inserter:Inserter<TxReceiptLog> = client.inserter(TABLE_EVENTS3)?
      .with_max_entries(100_000_000)
      .with_period(Some(Duration::from_secs(10)))
      .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

    for i in 0..rows.len(){
        inserter.write(&rows[i]).await?;
        if i % 100_000 == 1{
            println!("commit_logs:{i} total:{}", rows.len());
            inserter.commit().await?;
        }
    }
    inserter.end().await?;

    Ok(())
}

pub async fn clkhs_insert_txmsgs(rows: &Vec<TxMessage>) -> Result<()> {
    let client = clkhs_init_client().await?;

    let mut inserter:Inserter<TxMessage> = client.inserter(TABLE_TXMSGS3)?
      .with_max_entries(100_000_000)
      .with_period(Some(Duration::from_secs(10)))
      .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

    for i in 0..rows.len(){
        inserter.write(&rows[i]).await?;
        if i % 100_000 == 1{
            println!("commit_txmsgs:{i} total:{}", rows.len());
            inserter.commit().await?;
        }
    }
    inserter.end().await?;

    Ok(())
}

// fix timestamp
pub async fn clkhs_fetch_all_logs(start_id:u64, finish_id:u64) -> Result<Vec<TxReceiptLog>> {
    let client = clkhs_init_client().await?;
    let block_stat_time = std::time::Instant::now();
    println!("beiginnn fetch id is::: {:?}_{:?}", start_id, finish_id);
    let rows: Vec<TxReceiptLog> = client
        .query(format!("SELECT * FROM tx_log.{} where id >=? and id <? order by id", TABLE_EVENTS).as_str())
        .bind(start_id)
        .bind(finish_id)
        .fetch_all::<TxReceiptLog>()
        .await?;
    println!("lennnn is::: {:?} , time:{:?}", rows.len(), block_stat_time.elapsed());
    Ok(rows)
}

// fix timestamp
pub async fn clkhs_fetch_all_msgs(start_id:u64, finish_id:u64) -> Result<Vec<TxMessage>> {
    let client = clkhs_init_client().await?;
    let block_stat_time = std::time::Instant::now();
    println!("beiginnn fetch idx is::: {:?}_{:?}", start_id, finish_id);
    let rows: Vec<TxMessage> = client
        .query(format!("SELECT * FROM tx_log.{} where idx >=? and idx <? order by idx", TABLE_TXMSGS).as_str())
        .bind(start_id)
        .bind(finish_id)
        .fetch_all::<TxMessage>()
        .await?;
    println!("lennnn is::: {:?} , time:{:?}", rows.len(), block_stat_time.elapsed());
    Ok(rows)
}

// fix height 1070w-1080w
pub async fn clkhs_fetch_all_tx_log_fix() -> Result<()> {
    let client = clkhs_init_client().await?;
    let rows: Vec<TxReceiptLog> = client
        .query(format!("SELECT * FROM tx_log.{} WHERE id< ? and tx_idx > ? ", TABLE_EVENTS).as_str())
        .bind(27121384)
        .bind(807790570) // >825589567 (1080w)
        .fetch_all::<TxReceiptLog>()
        .await?;

    // 670472729, max id < 1070w
    // 807790567, max tx_idx < 1070w

    println!("lennnn is::: {:?}", rows.len());
    //let mut row:TxReceiptLog = rows[0].clone();
    //row.id += 670472729;
    //println!("first is::: {:?} \ncopy id is {:?}", rows[0], row);

    let mut inserter:Inserter<TxReceiptLog> = client.inserter("tx_event_test2")?
      .with_max_entries(100_000_000)
      .with_period(Some(Duration::from_secs(10)))
      .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

    for i in 0..rows.len(){
        let mut row : TxReceiptLog = rows[i].clone();
        row.id += 670472729;
        inserter.write(&row).await?;
        if i % 10_000 == 1{
            println!("commit_logs:{i} total:{}", rows.len());
            inserter.commit().await?;
        }
    }
    inserter.end().await?;
    println!("insert finished!!! total:{}", rows.len());

    Ok(())
}

// fix height 1080w-1100w
pub async fn clkhs_fetch_all_tx_log_fix2() -> Result<()> {
    let client = clkhs_init_client().await?;
    let rows: Vec<TxReceiptLog> = client
        .query(format!("SELECT * FROM tx_log.{} WHERE id> ? and tx_idx > ? ", TABLE_EVENTS).as_str())
        .bind(670472729)
        .bind(825589567) // >825589567 (1080w)
        .fetch_all::<TxReceiptLog>()
        .await?;

    // 1070w-1080w counts: 27121383
    // 1080w tx_idx > 825589567

    //697594112 original max id
    //670472730 oririnal start id

    println!("lennnn is::: {:?}", rows.len());

    let mut inserter:Inserter<TxReceiptLog> = client.inserter("tx_event_test2")?
      .with_max_entries(100_000_000)
      .with_period(Some(Duration::from_secs(10)))
      .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

    for i in 0..rows.len(){
        let mut row : TxReceiptLog = rows[i].clone();
        row.id += 27121383;
        inserter.write(&row).await?;
        if i % 10_000 == 1{
            println!("commit_logs:{i} total:{}", rows.len());
            inserter.commit().await?;
        }
    }
    inserter.end().await?;
    println!("insert finished!!! total:{}", rows.len());

    Ok(())
}

pub async fn clkhs_fetch_all_tx_log_fix_move_all() -> Result<()> {
    let client = clkhs_init_client().await?;
    let rows: Vec<TxReceiptLog> = client
        .query(format!("SELECT * FROM tx_log.{} WHERE tx_idx<=", "tx_event_test2").as_str())
        .bind(825589567) // <(1080w)
        .fetch_all::<TxReceiptLog>()
        .await?;

    println!("lennnn is::: {:?}", rows.len());

    let mut inserter:Inserter<TxReceiptLog> = client.inserter("tx_event")?
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
    println!("insert finished !!!  total:{}", rows.len());

    Ok(())
}


