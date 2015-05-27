extern crate libc;

#[macro_use] extern crate enum_primitive;
extern crate num;
use num::FromPrimitive;

use libc::{c_void, c_char, c_int, c_double, uint8_t, int64_t, intptr_t};
use std::ffi::{CStr, CString};
use std::marker::PhantomData;

pub use std::cmp::{Ordering};


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
    fn sqlite3_create_collation_v2(db: *const DbHandle, name: *const c_char, textrep: c_int, arg: *mut c_void, compare: *const c_void, destroy: *const c_void) -> c_int;

    fn sqlite3_prepare_v2(db: *const DbHandle, sql: *const c_char, nbytes: c_int, stmt: *mut*const StmtHandle, tail: *mut*const c_char) -> c_int;
    fn sqlite3_finalize(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_reset(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_clear_bindings(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_step(stmt: *const StmtHandle) -> c_int;

    fn sqlite3_column_type(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_int(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_int64(stmt: *const StmtHandle, col: c_int) -> int64_t;
    fn sqlite3_column_double(stmt: *const StmtHandle, col: c_int) -> c_double;
    fn sqlite3_column_text(stmt: *const StmtHandle, col: c_int) -> *const uint8_t;
    fn sqlite3_column_blob(stmt: *const StmtHandle, col: c_int) -> *const uint8_t;
    fn sqlite3_column_bytes(stmt: *const StmtHandle, col: c_int) -> c_int;
    fn sqlite3_column_count(stmt: *const StmtHandle) -> c_int;

    fn sqlite3_bind_int(stmt: *const StmtHandle, index: c_int, val: c_int) -> c_int;
    fn sqlite3_bind_int64(stmt: *const StmtHandle, index: c_int, val: int64_t) -> c_int;
    fn sqlite3_bind_double(stmt: *const StmtHandle, index: c_int, val: c_double) -> c_int;
    fn sqlite3_bind_text(stmt: *const StmtHandle, index: c_int, val: *const uint8_t, len: c_int, destroy: intptr_t) -> c_int;
    fn sqlite3_bind_blob(stmt: *const StmtHandle, index: c_int, val: *const uint8_t, len: c_int, destroy: intptr_t) -> c_int;
    fn sqlite3_bind_zeroblob(stmt: *const StmtHandle, index: c_int, len: c_int) -> c_int;
    fn sqlite3_bind_null(stmt: *const StmtHandle, index: c_int) -> c_int;
    fn sqlite3_bind_parameter_count(stmt: *const StmtHandle) -> c_int;
    fn sqlite3_bind_parameter_index(stmt: *const StmtHandle, name: *const c_char) -> c_int;
    fn sqlite3_bind_parameter_name(stmt: *const StmtHandle, index: c_int) -> *const c_char;
    
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
    ValueNull(i32, Type),
    TypeMismatch(Type, Type),
    UnknownParameter(String),
    DbError(ErrorCode, String),
    UnknownDbError(i32, String),
    StrNulByte(std::ffi::NulError),
    BadUtf8(std::str::Utf8Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::NotDone => write!(f, "Got row while expecting statement to be done."),
            Error::NoRow => write!(f, "Statement was done while expecting row."),
            Error::ValueNull(col, expected) => write!(f, "The value at column {} was null, but expected {:?}.", col, expected),
            Error::TypeMismatch(expected, found) => write!(f, "Expected type {:?}, but found type {:?} while getting column value", expected, found),
            Error::UnknownParameter(ref name) => write!(f, "Unknown parameter name: {}", name),
            Error::DbError(err, ref msg) => write!(f, "The database operation failed with error {:?} ({}): {}", err, err as i32, msg),
            Error::UnknownDbError(code, ref msg) => write!(f, "The database operation failed with unknown error code {}: {}", code, msg),
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
            Error::ValueNull(..) => "The column value was null.",
            Error::TypeMismatch(..) => "Unexpected type while getting column value.",
            Error::DbError(..) => "The database operation failed.",
            Error::UnknownParameter(..) => "Unknown parameter name.",
            Error::UnknownDbError(..) => "The database operation failed.",
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
        // sqlite3_errmsg is specified to return UTF8, so we do unchecked conversion
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


impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.do_close();
    }
}

impl Database {
    // probably preferable to relying on drop, because it can't return errors
    pub fn close(mut self) -> Result<()> {
        let result = self.do_close();
        std::mem::forget(self); // don't want drop to run
        result
    }

    fn do_close(&mut self) -> Result<()> {
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

    pub fn exec<T: Bind>(&self, sql: &str, params: T) -> Result<()> {
        let mut stmt = try!(self.prepare(sql));
        try!(params.bind(&mut stmt));
        stmt.step_done()
    }

    pub fn changes(&self) -> i32 {
        unsafe { sqlite3_changes(self.ptr) }
    }

    pub fn create_collation<F>(&mut self, name: &str, comparer: F) -> Result<()>
        where F: 'static + Fn(&str, &str) -> Ordering
    {
        let name_c = try!(CString::new(name));
        db_result!((), unsafe {
            sqlite3_create_collation_v2(self.ptr, name_c.as_ptr(), 1 /* SQLITE_UTF8 */,
                                        std::mem::transmute(Box::new(comparer)),
                                        std::mem::transmute(invoke_comparer::<F>),
                                        std::mem::transmute(destroy_comparer::<F>))
        }, db_errmsg(self.ptr))
    }
}

extern "C" fn invoke_comparer<F>(arg: *mut c_void, len1: c_int, str1: *const c_char, len2: c_int, str2: *const c_char) -> c_int
    where F: Fn(&str, &str) -> Ordering
{
    let comparer: Box<F> = unsafe { std::mem::transmute(arg) };
    
    // we specified in the call to sqlite3_create_collation_v2 that we wanted UTF8 strings
    // so we perform unchecked conversions here
    let s1 = unsafe {
        let bytes = std::mem::transmute(std::slice::from_raw_parts(str1, len1 as usize));
        std::str::from_utf8_unchecked(bytes)
    };
    let s2 = unsafe {
        let bytes = std::mem::transmute(std::slice::from_raw_parts(str2, len2 as usize));
        std::str::from_utf8_unchecked(bytes)
    };

    match comparer(s1, s2) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1
    }
}

extern "C" fn destroy_comparer<F>(arg: *mut c_void) {
    println!("before free");
    {
        let _: Box<F> = unsafe { std::mem::transmute(arg) };
        // now it will be dropped
    }
    println!("after free");
}


impl<'db> Drop for Statement<'db> {
    fn drop(&mut self) {
        let _ = self.do_finalize();
    }
}

impl<'db> Statement<'db> {
    // probably preferable to relying on drop, because it can't return errors
    pub fn finalize(mut self) -> Result<()> {
        let result = self.do_finalize();
        std::mem::forget(self); // don't want drop to run
        result
    }

    fn do_finalize(&mut self) -> Result<()> {
        db_result!((), unsafe { sqlite3_finalize(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn reset(&mut self) -> Result<()> {
        db_result!((), unsafe { sqlite3_reset(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn clear_bindings(&mut self) -> Result<()> {
        db_result!((), unsafe { sqlite3_clear_bindings(self.ptr) }, stmt_errmsg(self.ptr))
    }

    pub fn bind<T: Bind>(&mut self, tup: &T) -> Result<()> {
        tup.bind(self)
    }

    pub fn bind_value<T: BindValue>(&mut self, index: i32, val: T) -> Result<()> {
        BindValue::bind_value(self, index, val)
    }

    pub fn bind_value_named<T: BindValue>(&mut self, name: &str, val: T) -> Result<()> {
        let index = try!(self.parameter_index(name));
        BindValue::bind_value(self, index, val)
    }

    fn bind_i32(&mut self, index: i32, val: i32) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_int(self.ptr, index+1, val) }, stmt_errmsg(self.ptr))
    }
    fn bind_i64(&mut self, index: i32, val: i64) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_int64(self.ptr, index+1, val) }, stmt_errmsg(self.ptr))
    }
    fn bind_f64(&mut self, index: i32, val: f64) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_double(self.ptr, index+1, val) }, stmt_errmsg(self.ptr))
    }
    fn bind_text(&mut self, index: i32, val: &str) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_text(self.ptr, index+1, val.as_ptr(), val.len() as c_int, TRANSIENT) }, stmt_errmsg(self.ptr))
    }
    fn bind_blob(&mut self, index: i32, val: &[u8]) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_blob(self.ptr, index+1, val.as_ptr(), val.len() as c_int, TRANSIENT) }, stmt_errmsg(self.ptr))
    }
    fn bind_zeroblob(&mut self, index: i32, len: i32) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_zeroblob(self.ptr, index+1, len) }, stmt_errmsg(self.ptr))
    }
    fn bind_null(&mut self, index: i32) -> Result<()> {
        db_result!((), unsafe { sqlite3_bind_null(self.ptr, index+1) }, stmt_errmsg(self.ptr))
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

    pub fn parameter_count(&self) -> i32 {
        unsafe { sqlite3_bind_parameter_count(self.ptr) }
    }

    pub fn parameter_index(&self, name: &str) -> Result<i32> {
        let name_c = try!(CString::new(name));
        let index = unsafe { sqlite3_bind_parameter_index(self.ptr, name_c.as_ptr()) };
        match index {
            0 => Err(Error::UnknownParameter(name.to_string())),
            i => Ok(i - 1)
        }
    }

    pub fn parameter_name(&self, index: i32) -> Option<&str> {
        unsafe {
            let data = sqlite3_bind_parameter_name(self.ptr, index+1);
            if data == std::ptr::null() {
                return None
            };
            let c_str = CStr::from_ptr(data);
            let bytes = c_str.to_bytes();
            // parameter names are guaranteed to be UTF8, so we do unchecked conversion
            Some(std::str::from_utf8_unchecked(bytes))
        }
    }
}


pub trait BindValue {
    fn bind_value(stmt: &mut Statement, index: i32, val: Self) -> Result<()>;
}

macro_rules! bind_value_impls {
    ($($binder:ident: $typ:ty as $to_typ:ty),*) => {
        $(
            impl<'a> BindValue for $typ {
                fn bind_value(stmt: &mut Statement, index: i32, val: Self) -> Result<()> {
                    stmt.$binder(index, val as $to_typ)
                }
            }
        )*
    }
}
bind_value_impls!(
    bind_i32: i8 as i32,
    bind_i32: u8 as i32,
    bind_i32: i16 as i32,
    bind_i32: u16 as i32,
    bind_i32: i32 as i32,
    bind_i64: u32 as i64,
    bind_i64: i64 as i64,
    bind_i64: isize as i64,
    bind_f64: f32 as f64,
    bind_f64: f64 as f64,
    bind_text: &'a str as &'a str,
    bind_blob: &'a [u8] as &'a [u8]
);

pub struct ZeroBlob(pub i32);

impl<'a> BindValue for &'a Vec<u8> {
    fn bind_value(stmt: &mut Statement, index: i32, val: Self) -> Result<()> {
        stmt.bind_blob(index, &val[..])
    }
}
impl BindValue for ZeroBlob {
    fn bind_value(stmt: &mut Statement, index: i32, val: Self) -> Result<()> {
        stmt.bind_zeroblob(index, val.0)
    }
}
impl BindValue for () {
    fn bind_value(stmt: &mut Statement, index: i32, _: Self) -> Result<()> {
        stmt.bind_null(index)
    }
}
impl<T: BindValue> BindValue for Option<T> {
    fn bind_value(stmt: &mut Statement, index: i32, val: Self) -> Result<()> {
        match val {
            Some(x) => BindValue::bind_value(stmt, index, x),
            None => stmt.bind_null(index)
        }
    }
}

// implement BindValue for fixed size arrays up to a limitied size
macro_rules! array_impls {
    ($($N:expr)+) => {
        $(
            impl<'a> BindValue for &'a [u8; $N] {
                fn bind_value(stmt: &mut Statement, index: i32, val: Self) -> Result<()> {
                    stmt.bind_blob(index, val)
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


pub trait Bind {
    fn bind(&self, stmt: &mut Statement) -> Result<()>;
}

impl Bind for () {
    fn bind(&self, _: &mut Statement) -> Result<()> {
        Ok(())
    }
}

macro_rules! bind_tuple_instance {
    ($($name:ident : $typ:ident),+) => {
        impl<$($typ: BindValue + Copy,)+> Bind for ($($typ,)+) {
            fn bind(&self, stmt: &mut Statement) -> Result<()> {
                match *self {
                    ($($name,)+) => {
                        let mut index = -1;
                        $( index += 1; try!(BindValue::bind_value(stmt, index, $name)); )+
                    }
                };
                Ok(())
            }
        }
    }
}
bind_tuple_instance!(a:A);
bind_tuple_instance!(a:A, b:B);
bind_tuple_instance!(a:A, b:B, c:C);
bind_tuple_instance!(a:A, b:B, c:C, d:D);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K, l:L);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K, l:L, m:M);
bind_tuple_instance!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K, l:L, m:M, n:N);



macro_rules! assert_type {
    ($expected_type:expr, $found_type:expr, $col:expr) => {{
        let found = $found_type;
        if found == Type::Null {
            return Err(Error::ValueNull($col, $expected_type))
        }
        if $expected_type != found {
            return Err(Error::TypeMismatch($expected_type, found))
        }
    }};
}

macro_rules! getter_converters {
    ($($from_name:ident as $name:ident: $typ:ty),*) => {
        $(
            fn $name(&self, col: i32) -> Result<$typ> {
                match self.$from_name(col) {
                    Ok(val) => Ok(val as $typ),
                    Err(e) => Err(e)
                }
            }
        )*
    }
}

impl<'db, 'stmt> Row<'db, 'stmt> {
    pub fn get<'row, T: Get<'row>>(&'row self) -> Result<T> {
        Get::<'row>::get(self)
    }

    pub fn get_type(&self, col: i32) -> Type {
        unsafe { to_type(sqlite3_column_type(self.stmt.ptr, col)) }
    }

    pub fn get_value<'row, T: GetValue<'row>>(&'row self, col: i32) -> Result<T> {
        GetValue::<'row>::get_value(self, col)
    }

    fn get_i32(&self, col: i32) -> Result<i32> {
        assert_type!(Type::Integer, self.get_type(col), col);
        Ok(unsafe { sqlite3_column_int(self.stmt.ptr, col) })
    }
    fn get_i64(&self, col: i32) -> Result<i64> {
        assert_type!(Type::Integer, self.get_type(col), col);
        Ok(unsafe { sqlite3_column_int64(self.stmt.ptr, col) })
    }
    fn get_f64(&self, col: i32) -> Result<f64> {
        assert_type!(Type::Float, self.get_type(col), col);
        Ok(unsafe { sqlite3_column_double(self.stmt.ptr, col) })
    }
    fn get_text(&self, col: i32) -> Result<&str> {
        assert_type!(Type::Text, self.get_type(col), col);
        Ok(unsafe {
            let data = sqlite3_column_text(self.stmt.ptr, col);
            let len = sqlite3_column_bytes(self.stmt.ptr, col);
            let bytes = std::mem::transmute(std::slice::from_raw_parts(data, len as usize));
            try!(std::str::from_utf8(bytes))
        })
    }
    fn get_blob(&self, col: i32) -> Result<&[u8]> {
        assert_type!(Type::Blob, self.get_type(col), col);
        Ok(unsafe {
            let data = sqlite3_column_blob(self.stmt.ptr, col);
            let len = sqlite3_column_bytes(self.stmt.ptr, col);
            std::mem::transmute(std::slice::from_raw_parts(data, len as usize))
        })
    }

    getter_converters!(
        get_i32 as get_i8: i8,
        get_i32 as get_u8: u8,
        get_i32 as get_i16: i16,
        get_i32 as get_u16: u16,
        get_i64 as get_u32: u32,
        get_i64 as get_isize: isize,
        get_f64 as get_f32: f32
    );
}

pub trait GetValue<'row> {
    fn get_value(row: &'row Row, col: i32) -> Result<Self>;
}

impl<'row, T: GetValue<'row>> GetValue<'row> for Option<T> {
    fn get_value(row: &'row Row, col: i32) -> Result<Self> {
        let result : Result<T> = GetValue::<'row>::get_value(row, col);
        match result {
            Ok(value) => Ok(Some(value)),
            Err(Error::ValueNull(..)) => Ok(None),
            Err(e) => Err(e)
        }
    }
}

macro_rules! get_value_impls {
    ($($getter:ident: $typ:ty),*) => {
        $(
            impl<'row> GetValue<'row> for $typ {
                fn get_value(row: &'row Row, col: i32) -> Result<Self> {
                    row.$getter::<'row>(col)
                }
            }
        )*
    }
}
get_value_impls!(
    get_i8: i8,
    get_u8: u8,
    get_i16: i16,
    get_u16: u16,
    get_i32: i32,
    get_u32: u32,
    get_isize: isize,
    get_i64: i64,
    get_f32: f32,
    get_f64: f64,
    get_text: &'row str,
    get_blob: &'row [u8]
);


pub trait Get<'row> {
    fn get(row: &'row Row) -> Result<Self>;
}

macro_rules! get_impl {
    ($($name:ident: $typ:ident),+) => {
        impl<'row, $($typ,)+> Get<'row> for ($($typ,)+) where $($typ : GetValue<'row>,)+ {
            fn get(row: &'row Row) -> Result<($($typ,)+)> {
                let mut index = -1;
                $( index += 1; let $name : $typ = try!(row.get_value(index)); )+
                Ok(($($name,)+))
            }
        }
    }
}
get_impl!(a:A);
get_impl!(a:A, b:B);
get_impl!(a:A, b:B, c:C);
get_impl!(a:A, b:B, c:C, d:D);
get_impl!(a:A, b:B, c:C, d:D, e:E);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K, l:L);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K, l:L, m:M);
get_impl!(a:A, b:B, c:C, d:D, e:E, f:F, g:G, h:H, i:I, j:J, k:K, l:L, m:M, n:N);




#[cfg(test)]
mod tests {
    use {open, Result, Statement, Bind};
    use std;

    struct Foo<'a> {
        a: i32,
        b: i64,
        c: f64,
        d: &'a str,
        e: &'a [u8]
    }

    impl<'a> Bind for Foo<'a> {
        fn bind(&self, stmt: &mut Statement) -> Result<()> {
            try!(stmt.bind_value(0, self.a));
            try!(stmt.bind_value(1, self.b));
            try!(stmt.bind_value(2, self.c));
            try!(stmt.bind_value(3, self.d));
            try!(stmt.bind_value(4, self.e));
            Ok(())
        }
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
    fn test_create_collation() {
        let mut db = open(":memory:").unwrap();
        db.create_collation("test_collate", |a, b| b.cmp(a)).unwrap();
        db.exec("CREATE TABLE test (foo TEXT)", ()).unwrap();
        let mut stmt = db.prepare("INSERT INTO test VALUES(?)").unwrap();
        stmt.reset().unwrap(); stmt.bind_value(0, "ja").unwrap(); stmt.step_done().unwrap();
        stmt.reset().unwrap(); stmt.bind_value(0, "asdf").unwrap(); stmt.step_done().unwrap();
        stmt.reset().unwrap(); stmt.bind_value(0, "xyz").unwrap(); stmt.step_done().unwrap();
        let mut stmt = db.prepare("SELECT * FROM test ORDER BY foo COLLATE test_collate ASC").unwrap();
        { let row = stmt.step_row().unwrap(); assert_eq!("xyz", row.get_value::<&str>(0).unwrap()); }
        { let row = stmt.step_row().unwrap(); assert_eq!("ja", row.get_value::<&str>(0).unwrap()); }
        { let row = stmt.step_row().unwrap(); assert_eq!("asdf", row.get_value::<&str>(0).unwrap()); }
        stmt.step_done().unwrap();
        let mut stmt = db.prepare("SELECT * FROM test ORDER BY foo COLLATE test_collate DESC").unwrap();
        { let row = stmt.step_row().unwrap(); assert_eq!("asdf", row.get_value::<&str>(0).unwrap()); }
        { let row = stmt.step_row().unwrap(); assert_eq!("ja", row.get_value::<&str>(0).unwrap()); }
        { let row = stmt.step_row().unwrap(); assert_eq!("xyz", row.get_value::<&str>(0).unwrap()); }
        stmt.step_done().unwrap();
    }

    #[test]
    fn test_bind_tuple_echo() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?,?,?,?,?,?,?,?,?,?,?,?").unwrap();

        type TupleType<'a> = (Option<i8>, u8, i16, u16, i32, u32, isize, i64, f32, f64, &'a str, &'a [u8]);

        let input: TupleType = (Some(1), 2, 3, 4, 5, 6, 7, 66, 99.4, 3.14, "foo", &[123]);
        stmt.bind(&input).unwrap();
        
        let row = stmt.step_row().unwrap();
        let output: TupleType = row.get().unwrap();
        
        assert_eq!(input, output);
    }

    #[test]
    fn test_bind_foo_echo() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        let foo = Foo { a:99, b:66, c:3.14, d:"foo", e:&[123]};
        stmt.bind(&foo).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(99, row.get_value::<i32>(0).unwrap());
        assert_eq!(66, row.get_value::<i64>(1).unwrap());
        assert_eq!(3.14, row.get_value::<f64>(2).unwrap());
        assert_eq!("foo", row.get_value::<&str>(3).unwrap());
        assert_eq!(&[123], row.get_value::<&[u8]>(4).unwrap());
    }

    #[test]
    #[should_panic(expected = "ValueNull(1, Integer)")]
    fn test_get_null_fails() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT 99, ?").unwrap();
        stmt.bind_value(0, ()).unwrap();
        let row = stmt.step_row().unwrap();
        row.get_value::<i32>(1).unwrap();
    }

    #[test]
    fn test_get_null_with_option_succeeds() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_value(0, ()).unwrap();
        let row = stmt.step_row().unwrap();
        let val : Option<i32> = row.get_value(0).unwrap();
        assert_eq!(None, val);
    }

    #[test]
    fn test_simple_echo_int() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_value(0, 888 as i32).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(888, row.get_value::<i32>(0).unwrap());
    }

    #[test]
    fn test_simple_echo_int64() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_value(0, 888 as i64).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(888, row.get_value::<i64>(0).unwrap());
    }

    #[test]
    fn test_simple_echo_double() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_value(0, 3.14).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(3.14, row.get_value::<f64>(0).unwrap());
    }

    #[test]
    fn test_simple_echo_text() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_value(0, "foobar").unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!("foobar", row.get_value::<&str>(0).unwrap());
    }

    #[test]
    fn test_simple_echo_blob() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_value(0, &[0,1,2]).unwrap();
        let row = stmt.step_row().unwrap();
        assert_eq!(&[0,1,2], row.get_value::<&[u8]>(0).unwrap());
    }

    #[test]
    fn test_get_tuple_2() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT 66, 3.14").unwrap();
        let row : (i32, f64) = stmt.step_row().unwrap().get().unwrap();
        assert_eq!(66, row.0);
        assert_eq!(3.14, row.1);
    }

    #[test]
    fn test_named_parameters() {
        let db = open(":memory:").unwrap();
        let stmt = db.prepare("SELECT ?, ?, $foo, 56").unwrap();
        assert_eq!(3, stmt.parameter_count());
        assert_eq!(2, stmt.parameter_index("$foo").unwrap());
        assert_eq!("$foo", stmt.parameter_name(2).unwrap());
    }

    #[test]
    #[should_panic(expected = "UnknownParameter(\"$bar\")")]
    fn test_parameter_index_fails() {
        let db = open(":memory:").unwrap();
        let stmt = db.prepare("SELECT ?, ?, $foo, 56").unwrap();
        assert_eq!(10, stmt.parameter_index("$bar").unwrap());
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
        stmt.bind_zeroblob(0, 10).unwrap();
        let row = stmt.step_row().unwrap();
        row.get_value::<&str>(0).unwrap();
    }

    #[test]
    #[should_panic(expected = "BadUtf8")]
    fn test_utf8_error() {
        let db = open(":memory:").unwrap();
        let mut stmt = db.prepare("SELECT ?").unwrap();
        let v = [0,1,2,3,4,255];
        let s = unsafe { std::str::from_utf8_unchecked(&v) };
        stmt.bind_value(0, s).unwrap();
        let row = stmt.step_row().unwrap();
        row.get_value::<&str>(0).unwrap();
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
