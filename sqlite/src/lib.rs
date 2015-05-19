extern crate libc;

use libc::{c_void, c_int, c_char, c_double, int64_t};
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

    fn sqlite3_db_handle(stmt: *const StmtHandle) -> *const DbHandle;
    fn sqlite3_prepare_v2(db: *const DbHandle, sql: *const c_char, nbytes: c_int, stmt: *mut*const StmtHandle, tail: *mut*const c_char) -> c_int;
    fn sqlite3_finalize(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_reset(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_step(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_column_int(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_int64(stmt: *const StmtHandle, col: c_int) -> int64_t;
    fn sqlite3_column_double(stmt: *const StmtHandle, col: c_int) -> c_double;
    fn sqlite3_column_text(stmt: *const StmtHandle, col: c_int) -> *const c_char;
    fn sqlite3_column_blob(stmt: *const StmtHandle, col: c_int) -> *const c_void;
    fn sqlite3_column_bytes(stmt: *const StmtHandle, col: c_int) -> c_int;
}

const ROW: i32 = 100;
const DONE: i32 = 101;

#[derive(Debug)]
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

fn to_error_code(code: c_int) -> ErrorCode {
    match code {
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
        _ => ErrorCode::Error
    }
}

#[derive(Debug)]
pub enum SqliteError {
    DatabaseError(ErrorCode, String),
    StrNulByteError(std::ffi::NulError),
    StrUtf8Error(std::str::Utf8Error),
    NotDoneError,
    NoRowError
}

impl From<std::ffi::NulError> for SqliteError {
    fn from(err: std::ffi::NulError) -> SqliteError {
        SqliteError::StrNulByteError(err)
    }
}
impl From<std::str::Utf8Error> for SqliteError {
    fn from(err: std::str::Utf8Error) -> SqliteError {
        SqliteError::StrUtf8Error(err)
    }
}

pub type SqliteResult<T> = Result<T, SqliteError>;

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

fn db_error<T>(code: i32, msg: String) -> SqliteResult<T> {
    Err(SqliteError::DatabaseError(to_error_code(code), msg))
}

macro_rules! db_result {
    ($value:expr, $code:expr, $msg:expr) => {{
        let code = $code;
        if code == 0 {
            Ok($value)
        } else {
            db_error(code, $msg)
        }
    }};
}



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




pub fn initialize() -> SqliteResult<()> {
    db_result!((), unsafe { sqlite3_initialize() }, "sqlite3_initialize() failed".to_string())
}

pub fn shutdown() -> SqliteResult<()> {
    db_result!((), unsafe { sqlite3_shutdown() }, "sqlite3_shutdown() failed".to_string())
}

pub fn open(filename: &str) -> SqliteResult<Database> {
    let mut db = Database { ptr: std::ptr::null() };
    let filename_c = CString::new(filename).unwrap();
    let code = unsafe { sqlite3_open_v2(filename_c.as_ptr(), &mut db.ptr, 6, std::ptr::null()) };
    if code == 0 {
        unsafe { sqlite3_extended_result_codes(db.ptr, 0) };
    };
    db_result!(db, code, "sqlite3_open_v2() failed".to_string())
}



impl Database {
    fn close(&mut self) -> SqliteResult<()> {
        db_result!((), unsafe { sqlite3_close(self.ptr) }, db_errmsg(self.ptr))
    }

    pub fn prepare<'a>(&'a self, sql: &str) -> SqliteResult<Statement<'a>> {
        let mut s = Statement { ptr: std::ptr::null(), phantom: PhantomData };
        let sql_c = CString::new(sql).unwrap();
        let code = unsafe {
            sqlite3_prepare_v2(self.ptr, sql_c.as_ptr(), -1, &mut s.ptr, std::ptr::null_mut())
        };
        db_result!(s, code, db_errmsg(self.ptr))
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
    fn finalize(&mut self) -> SqliteResult<()> {
        db_result!((), unsafe { sqlite3_finalize(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn reset(&mut self) -> SqliteResult<()> {
        db_result!((), unsafe { sqlite3_reset(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn step<'a>(&'a mut self) -> SqliteResult<Option<Row<'db, 'a>>> {
        let code = unsafe { sqlite3_step(self.ptr) };
        if code == ROW {
            Ok(Some(Row { stmt: self }))
        } else if code == DONE {
            Ok(None)
        } else {
            db_error(code, stmt_errmsg(self.ptr))
        }
    }

    pub fn step_done(&mut self) -> SqliteResult<()> {
        match self.step() {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(SqliteError::NotDoneError),
            Err(e) => Err(e)
        }
    }

    pub fn step_row<'a>(&'a mut self) -> SqliteResult<Row<'db, 'a>> {
        match self.step() {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(SqliteError::NoRowError),
            Err(e) => Err(e)
        }
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



impl<'db, 'stmt> Row<'db, 'stmt> {
    pub fn get_int(&self, col: i32) -> i32 {
        unsafe { sqlite3_column_int(self.stmt.ptr, col) }
    }
    pub fn get_int64(&self, col: i32) -> i64 {
        unsafe { sqlite3_column_int64(self.stmt.ptr, col) }
    }
    pub fn get_double(&self, col: i32) -> f64 {
        unsafe { sqlite3_column_double(self.stmt.ptr, col) }
    }
    pub fn get_text(&self, col: i32) -> Result<&str, std::str::Utf8Error> {
        unsafe {
            let data = sqlite3_column_text(self.stmt.ptr, col);
            let len = sqlite3_column_bytes(self.stmt.ptr, col);
            let bytes = std::mem::transmute(std::slice::from_raw_parts(data, len as usize));
            std::str::from_utf8(bytes)
        }
    }
    pub fn get_blob(&self, col: i32) -> &[u8] {
        unsafe {
            let data = sqlite3_column_blob(self.stmt.ptr, col);
            let len = sqlite3_column_bytes(self.stmt.ptr, col);
            std::mem::transmute(std::slice::from_raw_parts(data, len as usize))
        }
    }
}

