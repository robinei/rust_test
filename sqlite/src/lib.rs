extern crate libc;

#[macro_use] extern crate enum_primitive;
extern crate num;
use num::FromPrimitive;

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


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
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

enum_from_primitive! {
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
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
}

fn db_error<T>(code: i32, msg: String) -> Result<T> {
    match ErrorCode::from_i32(code) {
        Some(err) => Err(Error::DbError(err, msg)),
        None => Err(Error::UnknownDbError(code, msg))
    }
}

#[derive(Debug)]
pub enum Error {
    NotDone,
    NoRow,
    WrongParamCount(i32, i32),
    DbError(ErrorCode, String),
    UnknownDbError(i32, String),
    TypeMismatch(Type, Type),
    StrNulByte(std::ffi::NulError),
    BadUtf8(std::str::Utf8Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::NotDone => write!(f, "Got row while expecting statement to be done."),
            Error::NoRow => write!(f, "Statement was done while expecting row."),
            Error::WrongParamCount(expected, found) => write!(f, "Expected {} parameters but got {}.", expected, found),
            Error::DbError(err, ref msg) => write!(f, "The database operation failed with error {:?} ({}): {}", err, err as i32, msg),
            Error::UnknownDbError(code, ref msg) => write!(f, "The database operation failed with unknown error code {}: {}", code, msg),
            Error::TypeMismatch(expected, found) => write!(f, "Expected type {:?}, but found type {:?} while getting column value", expected, found),
            Error::StrNulByte(ref err) => err.fmt(f),
            Error::BadUtf8(ref err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotDone => "Got row while expecting statement to be done.",
            Error::NoRow => "Statement was done while expecting row.",
            Error::WrongParamCount(..) => "Wrong parameter count.",
            Error::DbError(..) => "The database operation failed",
            Error::UnknownDbError(..) => "The database operation failed",
            Error::TypeMismatch(..) => "Unexpected type while getting column value",
            Error::StrNulByte(ref err) => err.description(),
            Error::BadUtf8(ref err) => err.description(),
        }
    }
    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::StrNulByte(ref err) => Some(err),
            Error::BadUtf8(ref err) => Some(err),
            _ => None
        }
    }
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

    pub fn prepare<'a>(&'a self, sql: &str) -> Result<Statement<'a>> {
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

    pub fn bind_all(&mut self, vals: &[&BindValue]) -> Result<()> {
        let expected = vals.len() as i32;
        let found = self.column_count();
        if expected != found {
            return Err(Error::WrongParamCount(expected, found));
        }

        let mut pos = 0;
        for val in vals {
            pos += 1;
            try!(val.bind(self, pos));
        };
        Ok(())
    }

    pub fn bind<T: BindValue>(&mut self, pos: i32, val: T) -> Result<()> {
        val.bind(self, pos)
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
    pub fn column_count(&self) -> i32 {
        unsafe { sqlite3_column_count(self.ptr) }
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


pub trait BindValue {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()>;
}

impl BindValue for i32 {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
        stmt.bind_int(pos, *self)
    }
}
impl BindValue for i64 {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
        stmt.bind_int64(pos, *self)
    }
}
impl BindValue for f64 {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
        stmt.bind_double(pos, *self)
    }
}
impl<'a> BindValue for &'a str {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
        stmt.bind_text(pos, *self)
    }
}
impl<'a> BindValue for &'a [u8] {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
        stmt.bind_blob(pos, *self)
    }
}
impl BindValue for () {
    fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
        stmt.bind_null(pos)
    }
}

// implement BindValue for fixed size arrays up to a limitied size
macro_rules! array_impls {
    ($($N:expr)+) => {
        $(
            impl<'a> BindValue for &'a [u8; $N] {
                fn bind(&self, stmt: &mut Statement, pos: i32) -> Result<()> {
                    stmt.bind_blob(pos, *self)
                }
            }
        )+
    }
}
array_impls! {
     0  1  2  3  4  5  6  7  8  9
    10 11 12 13 14 15 16 17 18 19
    20 21 22 23 24 25 26 27 28 29
    30 31 32
}


macro_rules! assert_type {
    ($expected_type:expr, $found_type:expr) => {{
        if $expected_type != $found_type {
            return Err(Error::TypeMismatch($expected_type, $found_type))
        }
    }};
}

impl<'db, 'stmt> Row<'db, 'stmt> {
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


#[cfg(test)]
mod tests {
    use {open, BindValue};
    use std;

    struct Foo<'a> {
        a: i32,
        b: i64,
        c: f64,
        d: &'a str,
        e: &'a [u8]
    }

    #[test]
    fn test_readonly() {
        let db = open(":memory:").unwrap();
        let stmt = db.prepare("SELECT ?, ?, ?").unwrap();
        assert_eq!(true, stmt.readonly());
    }

    #[test]
    fn test_not_readonly() {
        let db = open(":memory:").unwrap();
        let stmt = db.prepare("CREATE TABLE foo (bar INTEGER)").unwrap();
        assert_eq!(false, stmt.readonly());
    }

    #[test]
    fn test_column_count() {
        let db = open(":memory:").unwrap();
        let stmt = db.prepare("SELECT ?, ?, ?").unwrap();
        assert_eq!(3, stmt.column_count());
    }

    #[test]
    #[should_panic(expected = "WrongParamCount(3, 5)")]
    fn test_bind_all_wrong_count() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        stmt.bind_all(&[&(99 as i32),
                        &(66 as i64),
                        &(3.14 as f64)]).unwrap();
    }

    #[test]
    fn test_bind_all_echo() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        let foo = Foo { a:99, b:66, c:3.14, d:"foo", e:&[123]};
        stmt.bind_all(&[&foo.a, &foo.b, &foo.c, &foo.d, &foo.e]).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(99, row.get_int(0).unwrap());
        assert_eq!(66, row.get_int64(1).unwrap());
        assert_eq!(3.14, row.get_double(2).unwrap());
        assert_eq!("foo", row.get_text(3).unwrap());
        assert_eq!(&[123], row.get_blob(4).unwrap());
    }

    #[test]
    fn test_simple_echo_int() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind(1, 888 as i32).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(888, row.get_int(0).unwrap());
    }

    #[test]
    fn test_simple_echo_int64() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind(1, 888 as i64).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(888, row.get_int64(0).unwrap());
    }

    #[test]
    fn test_simple_echo_double() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind(1, 3.14).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(3.14, row.get_double(0).unwrap());
    }

    #[test]
    fn test_simple_echo_text() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind(1, "foobar").unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!("foobar", row.get_text(0).unwrap());
    }

    #[test]
    fn test_simple_echo_blob() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind(1, &[0,1,2]).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(&[0,1,2], row.get_blob(0).unwrap());
    }

    #[test]
    #[should_panic(expected = " DbError")]
    fn test_prepare_syntax_error() {
        let db = open(":memory:").unwrap();
        db.prepare("SEKECT 1").unwrap();
    }

    #[test]
    #[should_panic(expected = "TypeMismatch(Text, Blob)")]
    fn test_type_mismatch() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_zeroblob(1, 10).unwrap();
        let row = stmt.step_row().unwrap();
        row.get_text(0).unwrap();
    }

    #[test]
    #[should_panic(expected = "BadUtf8")]
    fn test_utf8_error() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        let v = [0,1,2,3,4,255];
        let s = unsafe { std::str::from_utf8_unchecked(&v) };
        stmt.bind(1, s).unwrap();
        let row = stmt.step_row().unwrap();
        row.get_text(0).unwrap();
    }

    #[test]
    #[should_panic(expected = "StrNulByte")]
    fn test_str_nul_error_open() {
        let v = [0,1,2,3,4,255];
        let s = unsafe { std::str::from_utf8_unchecked(&v) };
        open(s).unwrap();
    }

    #[test]
    #[should_panic(expected = "StrNulByte")]
    fn test_str_nul_error_prepare() {
        let v = [0,1,2,3,4,255];
        let s = unsafe { std::str::from_utf8_unchecked(&v) };
        let db = open(":memory:").unwrap();
        db.prepare(s).unwrap();
    }

    #[test]
    #[should_panic(expected = "NotDone")]
    fn test_not_done_error() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT 123").unwrap();
        stmt.step_done().unwrap();
    }

    #[test]
    #[should_panic(expected = "NoRow")]
    fn test_no_row_error() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("BEGIN").unwrap();
        stmt.step_row().unwrap();
    }
}

