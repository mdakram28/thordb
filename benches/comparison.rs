//! Benchmark comparison: ThorDB vs RocksDB vs Sled vs LevelDB
//!
//! Run with: cargo bench
//! Results will be in target/criterion/

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use tempfile::TempDir;

// ============================================================================
// Database Wrappers
// ============================================================================

trait KVStore {
    fn put(&self, key: &[u8], value: &[u8]);
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    #[allow(dead_code)]
    fn delete(&self, key: &[u8]);
    fn flush(&self);
}

// --- ThorDB ---
struct ThorDBWrapper {
    db: core::lsm::LsmTree,
    #[allow(dead_code)]
    dir: TempDir,
}

impl ThorDBWrapper {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let config = core::lsm::LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size_threshold: 4 * 1024 * 1024,
        };
        let db = core::lsm::LsmTree::open(config).unwrap();
        Self { db, dir }
    }
}

impl KVStore for ThorDBWrapper {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.db
            .put(
                core::lsm::Key::from_slice(key),
                core::lsm::Value::from_slice(value),
            )
            .unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get(&core::lsm::Key::from_slice(key))
            .unwrap()
            .map(|v| v.0)
    }

    fn delete(&self, key: &[u8]) {
        self.db.delete(core::lsm::Key::from_slice(key)).unwrap();
    }

    fn flush(&self) {
        self.db.flush().unwrap();
    }
}

// --- RocksDB ---
struct RocksDBWrapper {
    db: rocksdb::DB,
    #[allow(dead_code)]
    dir: TempDir,
}

impl RocksDBWrapper {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let db = rocksdb::DB::open_default(dir.path()).unwrap();
        Self { db, dir }
    }
}

impl KVStore for RocksDBWrapper {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).unwrap()
    }

    fn delete(&self, key: &[u8]) {
        self.db.delete(key).unwrap();
    }

    fn flush(&self) {
        self.db.flush().unwrap();
    }
}

// --- Sled ---
struct SledWrapper {
    db: sled::Db,
    #[allow(dead_code)]
    dir: TempDir,
}

impl SledWrapper {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let db = sled::open(dir.path()).unwrap();
        Self { db, dir }
    }
}

impl KVStore for SledWrapper {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.db.insert(key, value).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).unwrap().map(|v| v.to_vec())
    }

    fn delete(&self, key: &[u8]) {
        self.db.remove(key).unwrap();
    }

    fn flush(&self) {
        self.db.flush().unwrap();
    }
}

// --- LevelDB (rusty-leveldb) ---
struct LevelDBWrapper {
    db: std::sync::Mutex<rusty_leveldb::DB>,
    #[allow(dead_code)]
    dir: TempDir,
}

impl LevelDBWrapper {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let opts = rusty_leveldb::Options::default();
        let db = rusty_leveldb::DB::open(dir.path(), opts).unwrap();
        Self {
            db: std::sync::Mutex::new(db),
            dir,
        }
    }
}

impl KVStore for LevelDBWrapper {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.db.lock().unwrap().put(key, value).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.lock().unwrap().get(key)
    }

    fn delete(&self, key: &[u8]) {
        self.db.lock().unwrap().delete(key).unwrap();
    }

    fn flush(&self) {
        self.db.lock().unwrap().flush().unwrap();
    }
}

// ============================================================================
// Benchmark Helpers
// ============================================================================

fn generate_key(i: u64) -> Vec<u8> {
    format!("key_{:016}", i).into_bytes()
}

fn generate_value(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.r#gen::<u8>()).collect()
}

fn generate_random_key(max: u64) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    generate_key(rng.r#gen_range(0..max))
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");
    group.throughput(Throughput::Elements(1));

    for count in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("ThorDB", count), count, |b, &count| {
            b.iter_with_setup(
                || ThorDBWrapper::new(),
                |db| {
                    for i in 0..count {
                        db.put(&generate_key(i), &generate_value(100));
                    }
                },
            );
        });

        group.bench_with_input(BenchmarkId::new("RocksDB", count), count, |b, &count| {
            b.iter_with_setup(
                || RocksDBWrapper::new(),
                |db| {
                    for i in 0..count {
                        db.put(&generate_key(i), &generate_value(100));
                    }
                },
            );
        });

        group.bench_with_input(BenchmarkId::new("Sled", count), count, |b, &count| {
            b.iter_with_setup(
                || SledWrapper::new(),
                |db| {
                    for i in 0..count {
                        db.put(&generate_key(i), &generate_value(100));
                    }
                },
            );
        });

        group.bench_with_input(BenchmarkId::new("LevelDB", count), count, |b, &count| {
            b.iter_with_setup(
                || LevelDBWrapper::new(),
                |db| {
                    for i in 0..count {
                        db.put(&generate_key(i), &generate_value(100));
                    }
                },
            );
        });
    }

    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_read");
    group.throughput(Throughput::Elements(1));

    let count = 10000u64;

    // Pre-populate databases
    macro_rules! setup_db {
        ($wrapper:ident) => {{
            let db = $wrapper::new();
            for i in 0..count {
                db.put(&generate_key(i), &generate_value(100));
            }
            db.flush();
            db
        }};
    }

    group.bench_function("ThorDB", |b| {
        let db = setup_db!(ThorDBWrapper);
        b.iter(|| {
            let key = generate_random_key(count);
            black_box(db.get(&key));
        });
    });

    group.bench_function("RocksDB", |b| {
        let db = setup_db!(RocksDBWrapper);
        b.iter(|| {
            let key = generate_random_key(count);
            black_box(db.get(&key));
        });
    });

    group.bench_function("Sled", |b| {
        let db = setup_db!(SledWrapper);
        b.iter(|| {
            let key = generate_random_key(count);
            black_box(db.get(&key));
        });
    });

    group.bench_function("LevelDB", |b| {
        let db = setup_db!(LevelDBWrapper);
        b.iter(|| {
            let key = generate_random_key(count);
            black_box(db.get(&key));
        });
    });

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(1));

    // 80% reads, 20% writes
    let ops = 1000;
    let read_ratio = 80;

    macro_rules! run_mixed {
        ($db:expr) => {{
            let mut rng = rand::thread_rng();
            for i in 0..ops {
                if rng.r#gen_range(0..100) < read_ratio {
                    // Read
                    let key = generate_random_key(1000);
                    black_box($db.get(&key));
                } else {
                    // Write
                    $db.put(&generate_key(i), &generate_value(100));
                }
            }
        }};
    }

    group.bench_function("ThorDB", |b| {
        b.iter_with_setup(
            || {
                let db = ThorDBWrapper::new();
                for i in 0..1000u64 {
                    db.put(&generate_key(i), &generate_value(100));
                }
                db
            },
            |db| run_mixed!(db),
        );
    });

    group.bench_function("RocksDB", |b| {
        b.iter_with_setup(
            || {
                let db = RocksDBWrapper::new();
                for i in 0..1000u64 {
                    db.put(&generate_key(i), &generate_value(100));
                }
                db
            },
            |db| run_mixed!(db),
        );
    });

    group.bench_function("Sled", |b| {
        b.iter_with_setup(
            || {
                let db = SledWrapper::new();
                for i in 0..1000u64 {
                    db.put(&generate_key(i), &generate_value(100));
                }
                db
            },
            |db| run_mixed!(db),
        );
    });

    group.bench_function("LevelDB", |b| {
        b.iter_with_setup(
            || {
                let db = LevelDBWrapper::new();
                for i in 0..1000u64 {
                    db.put(&generate_key(i), &generate_value(100));
                }
                db
            },
            |db| run_mixed!(db),
        );
    });

    group.finish();
}

fn bench_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_sizes");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("ThorDB", size), size, |b, &size| {
            let db = ThorDBWrapper::new();
            let value = generate_value(size);
            let mut i = 0u64;
            b.iter(|| {
                db.put(&generate_key(i), &value);
                i += 1;
            });
        });

        group.bench_with_input(BenchmarkId::new("RocksDB", size), size, |b, &size| {
            let db = RocksDBWrapper::new();
            let value = generate_value(size);
            let mut i = 0u64;
            b.iter(|| {
                db.put(&generate_key(i), &value);
                i += 1;
            });
        });

        group.bench_with_input(BenchmarkId::new("Sled", size), size, |b, &size| {
            let db = SledWrapper::new();
            let value = generate_value(size);
            let mut i = 0u64;
            b.iter(|| {
                db.put(&generate_key(i), &value);
                i += 1;
            });
        });

        group.bench_with_input(BenchmarkId::new("LevelDB", size), size, |b, &size| {
            let db = LevelDBWrapper::new();
            let value = generate_value(size);
            let mut i = 0u64;
            b.iter(|| {
                db.put(&generate_key(i), &value);
                i += 1;
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_random_read,
    bench_mixed_workload,
    bench_value_sizes,
);

criterion_main!(benches);
