#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate db_key;
extern crate leveldb;
extern crate rand;
extern crate sha2;
#[macro_use]
extern crate log;
extern crate leveldb_sys;

use db_key::Key;
use leveldb::batch::Batch;
use leveldb::batch::Writebatch as WriteBatch;
use leveldb::database::Database as DB;
use leveldb::kv::KV;
use leveldb::options::{Options, ReadOptions, WriteOptions};
use leveldb_sys::Compression;

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::iter;
use std::path::Path;
use std::{mem, time};

struct DBKey(Vec<u8>);
impl Key for DBKey {
    fn from_u8(key: &[u8]) -> Self {
        DBKey(key.into())
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(&self.0)
    }
}

impl DBKey {
    fn from_u8(key: &[u8]) -> Self {
        DBKey(key.into())
    }
    fn from_str(key: &str) -> Self {
        DBKey(key.as_bytes().into())
    }
}

impl<'a> Into<DBKey> for &'a [u8] {
    fn into(self) -> DBKey {
        DBKey(self.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transaction {
    from: String,
    to: String,
    value: u64,
}

impl Transaction {
    fn new(from: String, to: String, value: u64) -> Self {
        let tx = Transaction { from, to, value };
        tx
    }

    fn coinbase() -> Self {
        Transaction {
            from: "".to_owned(),
            to: "a".to_owned(),
            value: 100000000000,
        }
    }

    fn encode(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    fn hash(&self) -> String {
        let mut hash = Sha256::default();
        hash.input(&self.encode());
        format!("{:x}", hash.result())
    }
}

type Txs = Vec<Transaction>;

#[derive(Debug, Serialize, Deserialize)]
struct Block {
    timestamp: u64,
    transactions: Txs,
    prev_hash: String,
    hash: String,
}

impl Block {
    fn new(txs: Txs, prev_hash: &str) -> Self {
        let ts = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap();
        let ts = ts.as_secs() * 1000 + ts.subsec_nanos() as u64 / 1_000_000;
        let ts_bytes: [u8; 8] = unsafe { mem::transmute(ts.to_be()) };
        let mut hash = Sha256::default();
        hash.input(&ts_bytes);
        for tx in &txs {
            hash.input(&tx.encode());
        }
        hash.input(prev_hash.as_bytes());
        Block {
            timestamp: ts,
            transactions: txs,
            prev_hash: prev_hash.to_owned(),
            hash: format!("{:x}", hash.result()),
        }
    }
}

struct Blockchain {
    db: DB<DBKey>,
    curr_hash: String,
}

type Result<T> = std::result::Result<T, leveldb::database::error::Error>;

impl Blockchain {
    fn new() -> Self {
        let mut options = Options::new();
        options.create_if_missing = true;
        options.compression = Compression::Snappy;
        options.cache = Some(leveldb::database::cache::Cache::new(8 * 1024 * 1024));
        options.block_restart_interval = Some(16);
        options.block_size = Some(4 * 1024);
        options.write_buffer_size = Some(4 * 1024);

        let mut db: DB<DBKey> = DB::open(Path::new("./data"), options).unwrap();

        let mut chain = Blockchain {
            db,
            curr_hash: "".to_string(),
        };
        let hash = match chain
            .db
            .get(ReadOptions::new(), DBKey::from_str("curr_hash"))
        {
            Ok(Some(val)) => String::from_utf8(val.to_vec()).unwrap(),
            Ok(None) => {
                let block = Block::new(vec![Transaction::coinbase()], "");
                chain.save(&block).unwrap();
                block.hash
            }
            Err(err) => panic!(err),
        };

        chain
    }

    fn curr_hash(&self) -> &str {
        &self.curr_hash
    }

    fn save(&mut self, block: &Block) -> Result<()> {
        if self.curr_hash == block.prev_hash {
            println!("begin process block");
            let mut batch = WriteBatch::new();
            /*let hash = &block.hash.as_bytes();
            let encoded: Vec<u8> = bincode::serialize(&block).unwrap();
            batch.put(DBKey::from_u8(hash), &encoded);
            batch.put(DBKey::from_str("curr_hash"), hash);
            */
            if block.prev_hash == "" {
                let balance = bincode::serialize(&100000000000u64).unwrap();
                batch.put(DBKey::from_str("a"), &balance);
            }
            for tx in &block.transactions {
                // acturally it should first check the batch to get the balance
                let mut from = self.balance(&tx.from)?;
                if from >= tx.value {
                    let mut to = self.balance(&tx.to)?;
                    from -= tx.value;
                    to += tx.value;
                    trace!("batch put {}: {}", tx.from, from);
                    trace!("batch put {}: {}", tx.to, to);
                    batch.put(
                        tx.from.as_bytes().into(),
                        &bincode::serialize(&from).unwrap(),
                    );
                    batch.put(tx.to.as_bytes().into(), &bincode::serialize(&to).unwrap());
                }
            }

            let mut write_options = WriteOptions::new();
            write_options.sync = true;
            self.db.write(write_options, &batch)?;
            self.curr_hash = block.hash.clone();
            return Ok(());
        }
        Ok(())
    }

    fn balance(&self, account: &str) -> Result<u64> {
        let read_option = ReadOptions::new();
        let bf = self.db.get(read_option, DBKey::from_str(account))?;
        bf.map(|v| Ok(bincode::deserialize(&v).unwrap()))
            .unwrap_or(Ok(0u64))
    }
}

impl Drop for Blockchain {
    fn drop(&mut self) {}
}

fn main() {
    let mut chain = Blockchain::new();
    const N: usize = 100000;
    const len_prefix: usize = 60;
    let issuer = "a".to_string();
    let prefix: String = iter::repeat('x').take(len_prefix).collect();

    let accounts: Vec<String> = (0..N)
        .map(|i| prefix.to_string() + &i.to_string())
        .collect();
    let txes: Vec<Transaction> = accounts
        .iter()
        .map(|addr| Transaction::new(issuer.clone(), addr.to_string(), 1000))
        .collect();

    let mut balances: HashMap<String, u64> = accounts
        .iter()
        .map(|addr| (addr.to_string(), 100))
        .collect();

    let block = Block::new(txes, chain.curr_hash());
    println!("begin save");
    chain.save(&block).unwrap();
    println!("end save");

    for _i in 0..200 {
        let txes: Vec<_> = (0..N)
            .map(|i| {
                let from = &accounts[rand::random::<usize>() % N];
                let to = &accounts[rand::random::<usize>() % N];
                Transaction::new(from.to_string(), to.to_string(), 1)
            })
            .collect();
        for tx in &txes {
            *balances.entry(tx.from.to_string()).or_insert(0) -= tx.value;
            *balances.entry(tx.to.to_string()).or_insert(0) += tx.value;
        }

        let block = Block::new(txes, chain.curr_hash());
        let now = time::Instant::now();
        chain.save(&block).unwrap();
        let new_now = time::Instant::now();
        println!("execution time:{:?}", new_now.duration_since(now));
    }

    let expected = balances
        .iter()
        .map(|(k, v)| {
            println!("expected {}, got {}", *v, chain.balance(k).unwrap());
            chain.balance(k).unwrap() == *v
        })
        .all(|v| v);

    println!("expected: {}", expected);
}
