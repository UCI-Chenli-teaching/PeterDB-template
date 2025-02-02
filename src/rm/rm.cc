#include "src/include/rm.h"

#include "src/include/rm_utils.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        if (RecordBasedFileManager::instance().createFile("Tables") != 0) {
            return -1;
        }
        if (RecordBasedFileManager::instance().createFile("Columns") != 0) {
            RecordBasedFileManager::instance().destroyFile("Tables");
            return -1;
        }

        FileHandle tablesFile, columnsFile;
        if (RecordBasedFileManager::instance().openFile("Tables", tablesFile) != 0) {
            return -1;
        }
        if (RecordBasedFileManager::instance().openFile("Columns", columnsFile) != 0) {
            return -1;
        }

        std::vector<Attribute> tablesDesc;
        std::vector<Attribute> columnsDesc;
        getTablesRecordDescriptor(tablesDesc);
        getColumnsRecordDescriptor(columnsDesc);

        {
            std::vector<std::string> vals = {"1", "Tables", "Tables"};
            insertRowGeneric(tablesFile, tablesDesc, vals);
        }
        {
            std::vector<std::string> vals = {"2", "Columns", "Columns"};
            insertRowGeneric(tablesFile, tablesDesc, vals);
        }
        {
            std::vector<std::string> vals = {"1", "table-id", "0", "4", "1"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"1", "table-name", "2", "50", "2"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"1", "file-name", "2", "50", "3"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"2", "table-id", "0", "4", "1"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"2", "column-name", "2", "50", "2"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"2", "column-type", "0", "4", "3"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"2", "column-length", "0", "4", "4"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }
        {
            std::vector<std::string> vals = {"2", "column-position", "0", "4", "5"};
            insertRowGeneric(columnsFile, columnsDesc, vals);
        }

        RecordBasedFileManager::instance().closeFile(tablesFile);
        RecordBasedFileManager::instance().closeFile(columnsFile);

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        return -1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB