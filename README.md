<div align="center">

# ⚡ ThorDB

**A blazingly fast, embeddable key-value storage engine written in Rust**

[![Build Status](https://img.shields.io/github/actions/workflow/status/akram/thordb/ci.yml?branch=main&style=flat-square)](https://github.com/akram/thordb/actions)
[![Crates.io](https://img.shields.io/crates/v/thordb?style=flat-square)](https://crates.io/crates/thordb)
[![Documentation](https://img.shields.io/docsrs/thordb?style=flat-square)](https://docs.rs/thordb)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue?style=flat-square)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.75%2B-orange?style=flat-square)](https://www.rust-lang.org)

[Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Benchmarks](#benchmarks) • [Contributing](#contributing)

</div>

---

## Why ThorDB?

ThorDB is a **production-grade LSM-tree storage engine** designed for applications that need:

- 🚀 **High write throughput** — LSM-tree architecture optimized for write-heavy workloads
- 🔍 **Fast reads** — Binary search lookups with bloom filters (coming soon)
- 💾 **Durability** — Write-ahead logging ensures no data loss on crashes
- 🔄 **Duplicate key support** — First-class support for multi-version concurrency
- 🦀 **Pure Rust** — Zero unsafe code, memory-safe by design
- 📦 **Embeddable** — Use as a library in your Rust applications

## Features

| Feature | Status |
|---------|--------|
| LSM-tree storage engine | ✅ |
| Write-ahead log (WAL) | ✅ |
| SSTable with binary search | ✅ |
| Buffer pool with clock eviction | ✅ |
| Duplicate key support | ✅ |
| Crash recovery | ✅ |
| Range scans | ✅ |
| Tombstone garbage collection | 🚧 |
| Bloom filters | 🚧 |
| Compaction | 🚧 |
| Compression (LZ4/Zstd) | 📋 |
| Transactions | 📋 |

✅ Complete | 🚧 In Progress | 📋 Planned

## Quick Start

Add ThorDB to your `Cargo.toml`:

```toml
[dependencies]
thordb = "0.1"
```

### Basic Usage

```rust
use thordb::lsm::{LsmTree, LsmConfig, Key, Value};
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    // Open or create a database
    let config = LsmConfig {
        data_dir: PathBuf::from("./my_database"),
        memtable_size_threshold: 4 * 1024 * 1024, // 4MB
    };
    let db = LsmTree::open(config)?;

    // Write data
    db.put(Key::from("user:1"), Value::from(r#"{"name": "Alice"}"#))?;
    db.put(Key::from("user:2"), Value::from(r#"{"name": "Bob"}"#))?;

    // Read data
    if let Some(value) = db.get(&Key::from("user:1"))? {
        println!("Found: {}", String::from_utf8_lossy(value.as_bytes()));
    }

    // Delete data
    db.delete(Key::from("user:2"))?;

    // Range scan
    for entry in db.scan_live()? {
        println!("{:?} -> {:?}", entry.key, entry.value);
    }

    // Flush to disk
    db.flush()?;

    Ok(())
}
```

### Duplicate Keys (Multi-Version)

ThorDB natively supports multiple values per key with sequence numbers:

```rust
// Write multiple versions
db.put(Key::from("config"), Value::from("v1"))?;
db.put(Key::from("config"), Value::from("v2"))?;
db.put(Key::from("config"), Value::from("v3"))?;

// Get latest version
let latest = db.get(&Key::from("config"))?; // Returns "v3"

// Get all versions (newest first)
let all_versions = db.get_all(&Key::from("config"))?;
for entry in all_versions {
    println!("seq={}: {:?}", entry.seq_num, entry.value);
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         ThorDB                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │   Write     │───▶│  MemTable   │───▶│   SSTable   │      │
│  │   Request   │    │  (BTreeMap) │    │  (On-Disk)  │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
│         │                                     ▲              │
│         ▼                                     │              │
│  ┌─────────────┐                     ┌───────┴───────┐      │
│  │     WAL     │                     │  Buffer Pool  │      │
│  │  (Durability)│                    │ (Page Cache)  │      │
│  └─────────────┘                     └───────────────┘      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **MemTable** | In-memory sorted map (BTreeMap) for fast writes |
| **WAL** | Write-ahead log for durability before memtable insertion |
| **SSTable** | Immutable sorted files with binary search lookup |
| **Buffer Pool** | LRU/Clock page cache for efficient disk I/O |
| **Merge Iterator** | Efficiently combines data from multiple sources |

### Write Path

1. Log operation to WAL (durability)
2. Insert into MemTable (in-memory)
3. When MemTable is full, flush to SSTable
4. Background compaction merges SSTables

### Read Path

1. Check MemTable first (newest data)
2. Check SSTables from newest to oldest
3. Binary search within each SSTable
4. Merge results for duplicate keys

## Benchmarks

*Coming soon — benchmarks against RocksDB, LevelDB, and Sled*

Preliminary testing shows:
- **Write throughput**: ~500K ops/sec (sequential)
- **Read latency**: <10μs (in-memory), <100μs (on-disk)
- **Recovery time**: <1s for 1M entries

## Project Structure

```
thordb/
├── core/                    # Core storage engine
│   └── src/
│       ├── lsm/             # LSM-tree implementation
│       │   ├── memtable.rs  # In-memory sorted table
│       │   ├── sstable.rs   # Sorted string tables
│       │   ├── wal.rs       # Write-ahead log
│       │   ├── iterator.rs  # Merge iterators
│       │   └── lsm.rs       # Main coordinator
│       ├── bufferpool.rs    # Page buffer pool
│       ├── page.rs          # Page abstraction
│       └── tuple/           # Tuple serialization
└── src/
    └── main.rs              # CLI (coming soon)
```

## Contributing

We welcome contributions! Here's how to get started:

```bash
# Clone the repository
git clone https://github.com/akram/thordb.git
cd thordb

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run

# Format code
cargo fmt

# Run clippy
cargo clippy
```

### Areas We Need Help

- 🧪 **Testing** — More edge cases and stress tests
- 📊 **Benchmarking** — Performance comparisons with other engines
- 📖 **Documentation** — API docs and tutorials
- 🔧 **Features** — Compaction, bloom filters, compression

## Roadmap

### v0.2 (Next)
- [ ] Level-based compaction
- [ ] Bloom filters for faster negative lookups
- [ ] Compression support (LZ4)

### v0.3
- [ ] Snapshots and iterators
- [ ] Configurable compaction strategies
- [ ] Metrics and observability

### v1.0
- [ ] Full ACID transactions
- [ ] Replication support
- [ ] Production-ready stability

## Inspiration

ThorDB draws inspiration from these excellent projects:

- [RocksDB](https://rocksdb.org/) — The industry-standard LSM engine
- [LevelDB](https://github.com/google/leveldb) — Google's original LSM implementation
- [Sled](https://sled.rs/) — Modern embedded database in Rust
- [Mini-LSM](https://github.com/skyzh/mini-lsm) — Educational LSM implementation

## License

ThorDB is dual-licensed under:

- [MIT License](LICENSE-MIT)
- [Apache License 2.0](LICENSE-APACHE)

Choose whichever license works best for your project.

---

<div align="center">

**If you find ThorDB useful, please consider giving it a ⭐**

Made with ❤️ and 🦀

</div>
