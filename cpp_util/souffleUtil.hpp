#pragma once
#include "/usr/include/souffle/SouffleInterface.h"
#include "rust/cxx.h"

namespace souffle
{
    souffle::SouffleProgram *newInstance(const std::string &name)
    {
        return souffle::ProgramFactory::newInstance(name);
    }

    souffle::Relation *getRelation(const souffle::SouffleProgram *prog, const std::string &name)
    {
        return prog->getRelation(name);
    }

    const souffle::RecordTable& getRecordTable(souffle::SouffleProgram * const &prog) {
        return prog->getRecordTable();
    }

    const souffle::SymbolTable& getSymbolTable(souffle::SouffleProgram * const &prog) {
        return prog->getSymbolTable();
    }

    void runProgram(souffle::SouffleProgram *prog)
    {
        prog->run();
    }

    std::unique_ptr<souffle::tuple> createTuple(const souffle::Relation *rel)
    {
        std::unique_ptr<souffle::tuple> p = std::make_unique<souffle::tuple>(rel);
        return p;
    }

    void insertNumber(const std::unique_ptr<souffle::tuple> &tuple, uint32_t number)
    {
        *(tuple.get()) << number;
    }

    void insertText(const std::unique_ptr<souffle::tuple> &tuple, const std::string &text)
    {
        *(tuple.get()) << text;
    }

    uint32_t getNumber(const souffle::tuple* t) {
        souffle::RamUnsigned res;
        ((souffle::tuple &)*t) >> res;
        return res;
    }

    int32_t getSigned(const souffle::tuple* t) {
        souffle::RamSigned res;
        ((souffle::tuple &)*t) >> res;
        return res;
    }

    std::unique_ptr<std::string> getText(const souffle::tuple* t) {
        std::unique_ptr<std::string> res = std::make_unique<std::string>();
        ((souffle::tuple &)*t) >> *res;
        return res;
    }

    rust::Vec<int32_t> unpack_record(const souffle::RecordTable& records, int32_t index, uint32_t arity) {
        const int32_t* data = records.unpack(index, arity);
        rust::Vec<int32_t> res;
        for (uint32_t i = 0; i < arity; i++) {
            res.push_back(data[i]);
        }
        return res;
    }

    rust::String decode_symbol(const souffle::SymbolTable& symTable, int32_t id) {
        return rust::String(symTable.decode(id));
    }

    void insertTuple(souffle::Relation *rel, std::unique_ptr<souffle::tuple> tuple)
    {
        rel->insert(*(tuple.release()));
    }

    void freeProgram(souffle::SouffleProgram *prog)
    {
        delete prog;
    }

    class TupleIterator
    {
    private:
        souffle::Relation::iterator begin;
        souffle::Relation::iterator end;

    public:
        TupleIterator(const souffle::Relation *rel);
        ~TupleIterator();

        bool hasNext() const {
            return this->begin != this->end;
        }

        const souffle::tuple *getNext() {
            souffle::tuple *t = &*(this->begin);
            this->begin++;
            return t;
        }
    };

    TupleIterator::TupleIterator(const souffle::Relation *rel)
    {
        this->begin = rel->begin();
        this->end = rel->end();
    }

    TupleIterator::~TupleIterator()
    {
    }

    std::unique_ptr<TupleIterator> createTupleIterator(const souffle::Relation *rel) {
        return std::make_unique<TupleIterator>(rel);
    }

    bool hasNext(const std::unique_ptr<TupleIterator> &iter) {
        return iter->hasNext();
    }

    const souffle::tuple *getNext(std::unique_ptr<TupleIterator> &iter) {
        return iter->getNext();
    }

    void purgeProgram(souffle::SouffleProgram * prog) {
        prog->purgeInputRelations();
        prog->purgeInternalRelations();
        prog->purgeOutputRelations();
    }

} // namespace souffle
