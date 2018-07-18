#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate rand;
extern crate rocksdb;
extern crate sha2;

use rocksdb::WriteBatch;
use rocksdb::DB;
use sha2::{Digest, Sha256};
use std::{mem, time};

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
    db: DB,
    curr_hash: String,
}

type Result<T> = std::result::Result<T, rocksdb::Error>;

impl Blockchain {
    fn new() -> Self {
        let db = DB::open_default("./data").unwrap();
        let mut chain = Blockchain {
            db,
            curr_hash: "".to_string(),
        };
        let hash = match chain.db.get(b"curr_hash") {
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
            let hash = &block.hash.as_bytes();
            let encoded: Vec<u8> = bincode::serialize(&block).unwrap();
            let mut batch = WriteBatch::default();
            batch.put(hash, &encoded)?;
            batch.put(b"curr_hash", hash)?;
            for tx in &block.transactions {
                let mut from = self.balance(&tx.from)?;
                if from >= tx.value {
                    let mut to = self.balance(&tx.to)?;
                    from -= tx.value;
                    to += tx.value;
                    batch.put(tx.from.as_bytes(), &bincode::serialize(&from).unwrap())?;
                    batch.put(tx.to.as_bytes(), &bincode::serialize(&to).unwrap())?;
                }
            }

            let mut write_options = rocksdb::WriteOptions::default();
            write_options.set_sync(true);
            self.db.write_opt(batch, &write_options)?;
            self.curr_hash = block.hash.clone();
            return Ok(());
        }
        Ok(())
    }

    fn balance(&self, account: &str) -> Result<u64> {
        let bf = self.db.get(account.as_bytes())?;
        bf.map(|v| Ok(bincode::deserialize(&v).unwrap()))
            .unwrap_or(Ok(0u64))
    }
}

impl Drop for Blockchain {
    fn drop(&mut self) {}
}

fn main() {
    let mut chain = Blockchain::new();
    const N: usize = 500000;
    let issuer = "a".to_string();
    let accounts: Vec<String> = (0..N).map(|i| i.to_string()).collect();
    let txes: Vec<Transaction> = accounts
        .iter()
        .map(|addr| Transaction::new(issuer.clone(), addr.to_string(), 100))
        .collect();

    let block = Block::new(txes, chain.curr_hash());
    println!("begin save");
    chain.save(&block).unwrap();
    println!("end save");

    for _i in 0..10 {
        let txes: Vec<_> = (0..N)
            .map(|i| {
                let from = &accounts[rand::random::<usize>() % N];
                let to = &accounts[rand::random::<usize>() % N];
                Transaction::new(from.to_string(), to.to_string(), 1)
            })
            .collect();
        let block = Block::new(txes, chain.curr_hash());
        let now = time::Instant::now();
        chain.save(&block).unwrap();
        let new_now = time::Instant::now();
        println!("execution time:{:?}", new_now.duration_since(now));
    }
}
