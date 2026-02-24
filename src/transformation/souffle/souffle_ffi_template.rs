//! FFI to use souffle and each compiled souffle program directly. This file is willingly
//! incomplete and will be extended automatically by build.rs for each souffle program found.
pub use souffle_ffi::*;

#[cxx::bridge(namespace = "souffle")]
pub mod souffle_ffi {
    unsafe extern "C++" {
        include!("/usr/include/souffle/SouffleInterface.h");

        type SouffleProgram;
        type ProgramFactory;
        type Relation;
        type RecordTable;
        type SymbolTable;
        type tuple;

    }

    unsafe extern "C++" {
        include!("cpp_util/souffleUtil.hpp");

        fn newInstance(name: &CxxString) -> *mut SouffleProgram;
        unsafe fn getRelation(prog: *const SouffleProgram, name: &CxxString) -> *mut Relation;
        unsafe fn getRecordTable(prog: &*mut SouffleProgram) -> &RecordTable;
        unsafe fn getSymbolTable(prog: &*mut SouffleProgram) -> &SymbolTable;
        unsafe fn runProgram(prog: *mut SouffleProgram);
        unsafe fn createTuple(rel: *const Relation) -> UniquePtr<tuple>;
        fn insertNumber(tuple: &UniquePtr<tuple>, number: u32);
        fn insertText(tuple: &UniquePtr<tuple>, text: &CxxString);
        unsafe fn insertTuple(rel: *mut Relation, tuple: UniquePtr<tuple>);
        unsafe fn freeProgram(prog: *mut SouffleProgram);

        type TupleIterator;
        unsafe fn createTupleIterator(rel: *const Relation) -> UniquePtr<TupleIterator>;
        fn hasNext(iter: &UniquePtr<TupleIterator>) -> bool;
        fn getNext(iter: &mut UniquePtr<TupleIterator>) -> *const tuple;

        unsafe fn getNumber(t: *const tuple) -> u32;
        unsafe fn getSigned(t: *const tuple) -> i32;
        unsafe fn getText(t : *const tuple) -> UniquePtr<CxxString>;
        unsafe fn unpack_record(record: &RecordTable, index: i32, arity: u32) -> Vec<i32>;
        unsafe fn decode_symbol(table: &SymbolTable, index: i32) -> String;

        unsafe fn purgeProgram(prog: *mut SouffleProgram);
    }
}
