extern crate sqlite;

//use sqlite::*;

/*use std::rc::Rc;

type RowId = u32;
type NodeId = u32;
type MilliSeconds = u64;

struct RowEntry {
    id: RowId,
    parent_id: RowId,
    norm_name: String,
    files: Vec<Rc<FileEntry>>
}

struct FileEntry {
    node_id: NodeId,
    display_name: String,
    checksum: Checksum,
    size: MilliSeconds,
    mtime: MilliSeconds
}

struct Checksum {
    data: [u8; 20]
}*/

/*
fn create_tables(db: &mut sqlite::Database) -> sqlite::Result<()> {
    {
        let mut stmt = try!(db.prepare("CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            parent_id INTEGER NOT NULL,
            norm_name TEXT NOT NULL,
            node_id INTEGER NOT NULL,

            display_name TEXT NOT NULL,
            flags INTEGER NOT NULL,
            checksum BLOB NOT NULL,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,

            UNIQUE (parent_id, norm_name, node_id)
        )"));
        try!(stmt.step_done())
    }

    Ok(())
}


fn main() {
    println!("initialize...");
    sqlite::initialize().unwrap();
    /
    //let mut stmt: sqlite::Statement;
    {
        println!("open...");
        let mut db = sqlite::open(":memory:").unwrap();
        create_tables(&mut db).unwrap();

        {
            println!("prepare...");
            let mut stmt0 = db.prepare("SELECT 1").unwrap();
            let mut stmt = db.prepare("SELECT ?").unwrap();
            stmt.bind(&("jalla jalla",)).unwrap();
            stmt.reset().unwrap();
            if stmt.step().unwrap() {
                println!("got value: {}", stmt.get_value(0));
            }

            {
                let row = stmt.step_row().unwrap();
                let val: &str = row.get_value(0).unwrap();
                println!("got value: '{}'", val);

                stmt0.step_row().unwrap();
            }
            stmt.reset().unwrap();
            stmt0.reset().unwrap();

            stmt0.finalize().unwrap();
            stmt.finalize().unwrap();
        }

        //let mut stmt2 = db.prepare("CREATE TABLE test (foo INTEGER)").unwrap();
        db.close().unwrap();
    }

    println!("shutdown...");
    sqlite::shutdown().unwrap();

    println!("done.");
}*/

struct Test {
    value: String
}
impl Drop for Test {
    fn drop(&mut self) {
        println!("DROP {}", self);
    }
}
impl std::fmt::Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

fn main() {
    let mut db = sqlite::open(":memory:").unwrap();
    {
        let mut x = 0;
        {
            let test = Test { value: "jalla".to_string() };
            println!("create_collation");
            db.create_collation("test_collate", move |a, b| {
                println!("CMP {} {}", &test, x);
                a.cmp(b)
            }).unwrap();
            println!("after create_collation");
        };
        x += 1;
        x += 1;
    }
    {
        db.exec("CREATE TABLE test (foo INTEGER)", ()).unwrap();
        let mut stmt = db.prepare("INSERT INTO test VALUES(?)").unwrap();
        stmt.reset().unwrap(); stmt.bind_value(0, "ja").unwrap(); stmt.step_done().unwrap();
        stmt.reset().unwrap(); stmt.bind_value(0, "asdf").unwrap(); stmt.step_done().unwrap();
        stmt.reset().unwrap(); stmt.bind_value(0, "xyz").unwrap(); stmt.step_done().unwrap();
        let mut stmt = db.prepare("SELECT * FROM test ORDER BY foo COLLATE test_collate").unwrap();
        loop {
            match stmt.step().unwrap() {
                None => break,
                Some(row) => println!("{:?}", row.get_value::<&str>(0).unwrap())
            }
        }
    };
    println!("finish");
}
