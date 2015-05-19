extern crate libc;

use libc::{c_int};
use std::ffi::CString;
use std::ptr;
use std::marker::PhantomData;

mod ffi;


const ROW: i32 = 100;
const DONE: i32 = 101;


#[derive(Debug)]
pub enum SqliteError {
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

fn to_error(code: c_int) -> SqliteError {
    match code {
        1 => SqliteError::Error,
        2 => SqliteError::Internal,
        3 => SqliteError::Perm,
        4 => SqliteError::Abort,
        5 => SqliteError::Busy,
        6 => SqliteError::Locked,
        7 => SqliteError::NoMem,
        8 => SqliteError::ReadOnly,
        9 => SqliteError::Interrupt,
        10 => SqliteError::IOError,
        11 => SqliteError::Corrupt,
        12 => SqliteError::NotFound,
        13 => SqliteError::Full,
        14 => SqliteError::CantOpen,
        15 => SqliteError::Protocol,
        16 => SqliteError::Empty,
        17 => SqliteError::Schema,
        18 => SqliteError::TooBig,
        19 => SqliteError::Constraint,
        20 => SqliteError::Mismatch,
        21 => SqliteError::Misuse,
        22 => SqliteError::NoLFS,
        23 => SqliteError::Auth,
        24 => SqliteError::Format,
        25 => SqliteError::Range,
        26 => SqliteError::NotADB,
        27 => SqliteError::Notice,
        28 => SqliteError::Warning,
        _ => SqliteError::Error
    }
}


pub type SqliteResult<T> = Result<T, SqliteError>;

pub struct Database {
    ptr: *const ffi::DbHandle
}

pub struct Statement<'db> {
    ptr: *const ffi::StmtHandle,
    phantom: PhantomData<&'db Database>
}


macro_rules! to_result {
    ($value:expr, $code:expr) => {{
        let code = $code;
        if code == 0 {
            Ok($value)
        } else {
            Err(to_error(code))
        }
    }};
}


pub fn initialize() -> SqliteResult<()> {
    to_result!((), unsafe { ffi::sqlite3_initialize() })
}

pub fn shutdown() -> SqliteResult<()> {
    to_result!((), unsafe { ffi::sqlite3_shutdown() })
}

pub fn open(filename: &str) -> SqliteResult<Database> {
    let mut h = Database { ptr: ptr::null() };
    let filename_c = CString::new(filename).unwrap();
    let code = unsafe {
        ffi::sqlite3_open_v2(filename_c.as_ptr(), &mut h.ptr, 6, ptr::null())
    };
    to_result!(h, code)
}

impl Database {
    fn close(&mut self) -> SqliteResult<()> {
        to_result!((), unsafe { ffi::sqlite3_close(self.ptr) })
    }

    pub fn prepare<'db>(&'db self, sql: &str) -> SqliteResult<Statement<'db>> {
        let mut s = Statement { ptr: ptr::null(), phantom: PhantomData };
        let sql_c = CString::new(sql).unwrap();
        let code = unsafe {
            ffi::sqlite3_prepare_v2(self.ptr, sql_c.as_ptr(), -1, &mut s.ptr, ptr::null_mut())
        };
        to_result!(s, code)
    }
}

impl<'db> Statement<'db> {
    fn finalize(&mut self) -> SqliteResult<()> {
        to_result!((), unsafe { ffi::sqlite3_finalize(self.ptr) })
    }

    pub fn reset(&mut self) -> SqliteResult<()> {
        to_result!((), unsafe { ffi::sqlite3_reset(self.ptr) })
    }

    pub fn step(&mut self) -> SqliteResult<bool> {
        let code = unsafe { ffi::sqlite3_step(self.ptr) };
        if code == ROW {
            Ok(true)
        } else if code == DONE {
            Ok(false)
        } else {
            Err(to_error(code))
        }
    }

    pub fn get(&self, col: i32) -> i32 {
        unsafe { ffi::sqlite3_column_int(self.ptr, col) }
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

impl<'db> Drop for Statement<'db> {
    fn drop(&mut self) {
        match self.finalize() {
            Err(e) => println!("finalize() error in Statement::drop(): {:?}", e),
            _ => ()
        }
    }
}
