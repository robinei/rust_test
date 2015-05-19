extern crate sqlite;

//use sqlite::*;

/*use std::rc::Rc;

type RowId = u32;
type MilliSeconds = u64;

struct RowEntry {
    parent_id: RowId,
    norm_name: String,
    files: Vec<Rc<FileEntry>>
}

struct FileEntry {
    display_name: String,
    checksum: Checksum,
    size: MilliSeconds,
    mtime: MilliSeconds
}

struct Checksum {
    data: [u8; 20]
}*/



fn main() {
    println!("initialize...");
    sqlite::initialize().unwrap();

    
    //let mut stmt: sqlite::Statement;
    {
    println!("open...");
    let db = sqlite::open(":memory:").unwrap();

        println!("prepare...");
        let mut stmt = db.prepare("SELECT 'asdf'").unwrap();
        /*stmt.reset().unwrap();
        if stmt.step().unwrap() {
            println!("got value: {}", stmt.get(0));
        }*/

        {
            let row = stmt.step_row().unwrap();
            let val = row.get_text(0).unwrap();
            println!("got value: '{}'", val);
        }
        stmt.reset().unwrap();

        //let mut stmt2 = db.prepare("CREATE TABLE test (foo INTEGER)").unwrap();
    }

    println!("shutdown...");
    sqlite::shutdown().unwrap();

    println!("done.");
}

