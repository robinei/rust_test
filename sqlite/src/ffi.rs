use libc::{c_int, c_char};

pub enum DbHandle {}
pub enum StmtHandle {}

extern {
    pub fn sqlite3_initialize() -> c_int;
    pub fn sqlite3_shutdown() -> c_int;

    pub fn sqlite3_open_v2(filename: *const c_char, db: *mut*const DbHandle, flags: c_int, vfs: *const c_char) -> c_int;
    pub fn sqlite3_close(db: *const DbHandle) -> c_int;

    pub fn sqlite3_prepare_v2(db: *const DbHandle, sql: *const c_char, nbytes: c_int, stmt: *mut*const StmtHandle, tail: *mut*const c_char) -> c_int;
    pub fn sqlite3_finalize(stmt: *const StmtHandle) -> c_int;
    pub fn sqlite3_reset(stmt: *const StmtHandle) -> c_int;
    pub fn sqlite3_step(stmt: *const StmtHandle) -> c_int;
    pub fn sqlite3_column_int(stmt: *const StmtHandle, col: c_int) -> c_int;
}

