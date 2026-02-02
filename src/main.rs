use core::ThorDB;

fn main() {
    tracing_subscriber::fmt().init();
    let db = ThorDB::new("./data").unwrap();
    db.close();
}
