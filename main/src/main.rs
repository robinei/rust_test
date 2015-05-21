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

    
    //let mut stmt: sqlite::Statement;
    {
        println!("open...");
        let mut db = sqlite::open(":memory:").unwrap();
        create_tables(&mut db).unwrap();

        println!("prepare...");
        let mut stmt0 = db.prepare("SELECT 1").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind(1, "jalla jalla").unwrap();
        /*stmt.reset().unwrap();
        if stmt.step().unwrap() {
            println!("got value: {}", stmt.get(0));
        }*/

        {
            let row = stmt.step_row().unwrap();
            let val = row.get_text(0).unwrap();
            println!("got value: '{}'", val);

            stmt0.step_row().unwrap();
        }
        stmt.reset().unwrap();
        stmt0.reset().unwrap();

        //let mut stmt2 = db.prepare("CREATE TABLE test (foo INTEGER)").unwrap();
    }

    println!("shutdown...");
    sqlite::shutdown().unwrap();

    println!("done.");
}

