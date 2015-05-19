extern crate libc;

use libc::{c_char, c_int, c_double, uint8_t, int64_t, intptr_t};
use std::ffi::{CStr, CString};
use std::marker::PhantomData;


enum DbHandle {}
enum StmtHandle {}

extern {
    fn sqlite3_initialize() -> c_int;
    fn sqlite3_shutdown() -> c_int;

    fn sqlite3_open_v2(filename: *const c_char, db: *mut*const DbHandle, flags: c_int, vfs: *const c_char) -> c_int;
    fn sqlite3_close(db: *const DbHandle) -> c_int;
    fn sqlite3_extended_result_codes(db: *const DbHandle, onoff: c_int) -> c_int;
    fn sqlite3_errmsg(db: *const DbHandle) -> *const c_char;
    fn sqlite3_changes(db: *const DbHandle) -> c_int;

    fn sqlite3_prepare_v2(db: *const DbHandle, sql: *const c_char, nbytes: c_int, stmt: *mut*const StmtHandle, tail: *mut*const c_char) -> c_int;
    fn sqlite3_finalize(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_reset(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_step(stmt: *const StmtHandle) -> c_int;

    fn sqlite3_column_type(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_int(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_int64(stmt: *const StmtHandle, col: c_int) -> int64_t;
    fn sqlite3_column_double(stmt: *const StmtHandle, col: c_int) -> c_double;
    fn sqlite3_column_text(stmt: *const StmtHandle, col: c_int) -> *const uint8_t;
    fn sqlite3_column_blob(stmt: *const StmtHandle, col: c_int) -> *const uint8_t;
    fn sqlite3_column_bytes(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_count(stmt: *const StmtHandle) -> c_int;

    fn sqlite3_bind_int(stmt: *const StmtHandle, pos: c_int, val: c_int) -> c_int;
    fn sqlite3_bind_int64(stmt: *const StmtHandle, pos: c_int, val: int64_t) -> c_int;
    fn sqlite3_bind_double(stmt: *const StmtHandle, pos: c_int, val: c_double) -> c_int;
    fn sqlite3_bind_text(stmt: *const StmtHandle, pos: c_int, val: *const uint8_t, len: c_int, destroy: intptr_t) -> c_int;
    fn sqlite3_bind_blob(stmt: *const StmtHandle, pos: c_int, val: *const uint8_t, len: c_int, destroy: intptr_t) -> c_int;
    fn sqlite3_bind_zeroblob(stmt: *const StmtHandle, pos: c_int, len: c_int) -> c_int;
    fn sqlite3_bind_null(stmt: *const StmtHandle, pos: c_int) -> c_int;
    
    fn sqlite3_db_handle(stmt: *const StmtHandle) -> *const DbHandle;
    fn sqlite3_stmt_readonly(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_stmt_busy(stmt: *const StmtHandle) -> c_int;
}


//const STATIC: intptr_t = 0 as intptr_t;
const TRANSIENT: intptr_t = -1 as intptr_t;


#[derive(Debug, Eq, PartialEq)]
pub enum Type {
    Integer = 1,
    Float = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
    Unknown = 999
}

fn to_type(code: i32) -> Type {
    match code {
        1 => Type::Integer,
        2 => Type::Float,
        3 => Type::Text,
        4 => Type::Blob,
        5 => Type::Null,
        _ => Type::Unknown
    }
}


const ROW: i32 = 100;
const DONE: i32 = 101;

#[derive(Debug, Eq, PartialEq)]
pub enum ErrorCode {
    Error = 1,
    Internal = 2,
    Perm = 3,
    Abort = 4,
    Busy = 5,
    Locked = 6,
    NoMem = 7,
    ReadOnly = 8,
    Interrupt = 9,
    IOError = 10,
    Corrupt = 11,
    NotFound = 12,
    Full = 13,
    CantOpen = 14,
    Protocol = 15,
    Empty = 16,
    Schema = 17,
    TooBig = 18,
    Constraint = 19,
    Mismatch = 20,
    Misuse = 21,
    NoLFS = 22,
    Auth = 23,
    Format = 24,
    Range = 25,
    NotADB = 26,
    Notice = 27,
    Warning = 28
}

fn db_error<T>(code: i32, msg: String) -> Result<T> {
    let err = match code {
        1 => ErrorCode::Error,
        2 => ErrorCode::Internal,
        3 => ErrorCode::Perm,
        4 => ErrorCode::Abort,
        5 => ErrorCode::Busy,
        6 => ErrorCode::Locked,
        7 => ErrorCode::NoMem,
        8 => ErrorCode::ReadOnly,
        9 => ErrorCode::Interrupt,
        10 => ErrorCode::IOError,
        11 => ErrorCode::Corrupt,
        12 => ErrorCode::NotFound,
        13 => ErrorCode::Full,
        14 => ErrorCode::CantOpen,
        15 => ErrorCode::Protocol,
        16 => ErrorCode::Empty,
        17 => ErrorCode::Schema,
        18 => ErrorCode::TooBig,
        19 => ErrorCode::Constraint,
        20 => ErrorCode::Mismatch,
        21 => ErrorCode::Misuse,
        22 => ErrorCode::NoLFS,
        23 => ErrorCode::Auth,
        24 => ErrorCode::Format,
        25 => ErrorCode::Range,
        26 => ErrorCode::NotADB,
        27 => ErrorCode::Notice,
        28 => ErrorCode::Warning,
        _ => return Err(Error::UnknownDbError(code, msg))
    };
    Err(Error::DbError(err, msg))
}

#[derive(Debug)]
pub enum Error {
    DbError(ErrorCode, String),
    UnknownDbError(i32, String),
    StrNulByte(std::ffi::NulError),
    BadUtf8(std::str::Utf8Error),
    TypeMismatch(Type, Type),
    NotDone,
    NoRow
}

impl From<std::ffi::NulError> for Error {
    fn from(err: std::ffi::NulError) -> Error {
        Error::StrNulByte(err)
    }
}
impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::BadUtf8(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;




pub struct Database {
    ptr: *const DbHandle
}

pub struct Statement<'db> {
    ptr: *const StmtHandle,
    phantom: PhantomData<&'db i32>
}

pub struct Row<'db, 'stmt> where 'db: 'stmt {
    stmt: &'stmt mut Statement<'db>
}



fn db_errmsg(db: *const DbHandle) -> String {
    unsafe {
        let c_str = CStr::from_ptr(sqlite3_errmsg(db));
        let bytes = c_str.to_bytes();
        std::str::from_utf8_unchecked(bytes).to_string()
    }
}

fn stmt_errmsg(stmt: *const StmtHandle) -> String {
    db_errmsg(unsafe { sqlite3_db_handle(stmt) })
}

macro_rules! db_result {
    ($value:expr, $code:expr, $msg:expr) => {{
        let code = $code;
        match code {
            0 => Ok($value),
            _ => db_error(code, $msg)
        }
    }};
}


pub fn initialize() -> Result<()> {
    db_result!((), unsafe { sqlite3_initialize() }, "sqlite3_initialize() failed".to_string())
}

pub fn shutdown() -> Result<()> {
    db_result!((), unsafe { sqlite3_shutdown() }, "sqlite3_shutdown() failed".to_string())
}

pub fn open(filename: &str) -> Result<Database> {
    let filename_c = try!(CString::new(filename));
    let mut db = Database { ptr: std::ptr::null() };
    let code = unsafe { sqlite3_open_v2(filename_c.as_ptr(), &mut db.ptr, 6, std::ptr::null()) };
    if code == 0 {
        unsafe { sqlite3_extended_result_codes(db.ptr, 0) };
    };
    db_result!(db, code, "sqlite3_open_v2() failed".to_string())
}



impl Database {
    fn close(&mut self) -> Result<()> {
        db_result!((), unsafe { sqlite3_close(self.ptr) }, db_errmsg(self.ptr))
    }

    pub fn prepare<'a>(&'a mut self, sql: &str) -> Result<Statement<'a>> {
        let sql_c = try!(CString::new(sql));
        let mut stmt = Statement { ptr: std::ptr::null(), phantom: PhantomData };
        let code = unsafe {
            sqlite3_prepare_v2(self.ptr, sql_c.as_ptr(), -1, &mut stmt.ptr, std::ptr::null_mut())
        };
        db_result!(stmt, code, db_errmsg(self.ptr))
    }

    pub fn changes(&self) -> i32 {
        unsafe { sqlite3_changes(self.ptr) }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        match self.close() {
            Err(e) => println!("close() error in Database::drop(): {:?}", e),
            _ => ()
        }
    }
}


impl<'db> Statement<'db> {
    fn finalize(&mut self) -> Result<()> {
        db_result!((), unsafe { sqlite3_finalize(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn reset(&mut self) -> Result<()> {
        db_result!((), unsafe { sqlite3_reset(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn bind_int(&mut self, pos: i32, val: i32) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_int(self.ptr, pos, val) }, stmt_errmsg(self.ptr))
    }
    pub fn bind_int64(&mut self, pos: i32, val: i64) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_int64(self.ptr, pos, val) }, stmt_errmsg(self.ptr))
    }
    pub fn bind_double(&mut self, pos: i32, val: f64) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_double(self.ptr, pos, val) }, stmt_errmsg(self.ptr))
    }
    pub fn bind_text(&mut self, pos: i32, val: &str) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_text(self.ptr, pos, val.as_ptr(), val.len() as c_int, TRANSIENT) }, stmt_errmsg(self.ptr))
    }
    pub fn bind_blob(&mut self, pos: i32, val: &[u8]) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_blob(self.ptr, pos, val.as_ptr(), val.len() as c_int, TRANSIENT) }, stmt_errmsg(self.ptr))
    }
    pub fn bind_zeroblob(&mut self, pos: i32, len: i32) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_zeroblob(self.ptr, pos, len) }, stmt_errmsg(self.ptr))
    }
    pub fn bind_null(&mut self, pos: i32) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_null(self.ptr, pos) }, stmt_errmsg(self.ptr))
    }

    pub fn step<'a>(&'a mut self) -> Result<Option<Row<'db, 'a>>> {
        let code = unsafe { sqlite3_step(self.ptr) };
        match code {
            ROW => Ok(Some(Row { stmt: self })),
            DONE => Ok(None),
            _ => db_error(code, stmt_errmsg(self.ptr))
        }
    }
    pub fn step_done(&mut self) -> Result<()> {
        match self.step() {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(Error::NotDone),
            Err(e) => Err(e)
        }
    }
    pub fn step_row<'a>(&'a mut self) -> Result<Row<'db, 'a>> {
        match self.step() {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::NoRow),
            Err(e) => Err(e)
        }
    }

    pub fn readonly(&self) -> bool {
        unsafe { sqlite3_stmt_readonly(self.ptr) != 0 }
    }
    pub fn busy(&self) -> bool {
        unsafe { sqlite3_stmt_busy(self.ptr) != 0 }
    }
}

impl<'db> Drop for Statement<'db> {
    fn drop(&mut self) {
        match self.finalize() {
            Err(e) => println!("finalize() error in Statement::drop(): {:?}", e),
            _ => ()
        }
    }
}


macro_rules! assert_type {
    ($expected_type:expr, $found_type:expr) => {{
        if $expected_type != $found_type {
            return Err(Error::TypeMismatch($expected_type, $found_type))
        }
    }};
}

impl<'db, 'stmt> Row<'db, 'stmt> {
    pub fn column_count(&self) -> i32 {
        unsafe { sqlite3_column_count(self.stmt.ptr) }
    }

    pub fn get_type(&self, col: i32) -> Type {
        unsafe { to_type(sqlite3_column_type(self.stmt.ptr, col)) }
    }

    pub fn get_int(&self, col: i32) -> Result<i32> {
        assert_type!(Type::Integer, self.get_type(col));
        Ok(unsafe { sqlite3_column_int(self.stmt.ptr, col) })
    }
    pub fn get_int64(&self, col: i32) -> Result<i64> {
        assert_type!(Type::Integer, self.get_type(col));
        Ok(unsafe { sqlite3_column_int64(self.stmt.ptr, col) })
    }
    pub fn get_double(&self, col: i32) -> Result<f64> {
        assert_type!(Type::Float, self.get_type(col));
        Ok(unsafe { sqlite3_column_double(self.stmt.ptr, col) })
    }
    pub fn get_text(&self, col: i32) -> Result<&str> {
        assert_type!(Type::Text, self.get_type(col));
        Ok(unsafe {
            let data = sqlite3_column_text(self.stmt.ptr, col);
            let len = sqlite3_column_bytes(self.stmt.ptr, col);
            let bytes = std::mem::transmute(std::slice::from_raw_parts(data, len as usize));
            try!(std::str::from_utf8(bytes))
        })
    }
    pub fn get_blob(&self, col: i32) -> Result<&[u8]> {
        assert_type!(Type::Blob, self.get_type(col));
        Ok(unsafe {
            let data = sqlite3_column_blob(self.stmt.ptr, col);
            let len = sqlite3_column_bytes(self.stmt.ptr, col);
            std::mem::transmute(std::slice::from_raw_parts(data, len as usize))
        })
    }
}

