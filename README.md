# crabstash

A high-performance embedded database written in Rust, featuring LSM-tree storage and MVCC transactions.

## Features

- **LSM-tree storage engine** - Write-optimized with background compaction
- **MVCC transactions** - Snapshot and serializable isolation levels
- **Write-ahead logging** - Durability guarantees with crash recovery
- **Bloom filters** - Fast negative lookups
- **Embedded library** - Link directly into your application

## Quick Start

```rust
use crabstash::Db;

fn main() -> crabstash::Result<()> {
    let db = Db::open("my_data")?;

    db.put("hello", "world")?;

    if let Some(value) = db.get("hello")? {
        println!("{}", String::from_utf8_lossy(&value));
    }

    db.delete("hello")?;

    Ok(())
}
```

## Transactions

```rust
use crabstash::{Db, Isolation};

fn main() -> crabstash::Result<()> {
    let db = Db::open("my_data")?;

    let txn = db.begin_with_isolation(Isolation::Serializable);
    txn.put("key1", "value1");
    txn.put("key2", "value2");
    txn.commit()?;

    Ok(())
}
```

## Status

Work in progress.

## License

MIT
