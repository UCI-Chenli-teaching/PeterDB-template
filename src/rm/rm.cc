#include "src/include/rm.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <src/include/rbfm_utils.h>

#include "src/include/rm_utils.h"
#include "src/include/rbfm.h"
#include "src/include/pfm.h"

namespace PeterDB {

    // A small helper to find the maximum tableId from the in-memory cache
    // so we can assign the next ID without scanning. If no tables, return 0.
    static int findMaxTableIdInCache() {
        int maxId = 0;
        for (auto &kv : g_tableCache) {
            if (kv.second.tableId > maxId) {
                maxId = kv.second.tableId;
            }
        }
        return maxId;
    }

    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager;
        return _relation_manager;
    }

    void RelationManager::SyncTablesCache()
    {
        FileHandle tablesFile;
        if (PagedFileManager::instance().openFile("Tables", tablesFile) != 0) {
            return;
        }

        FileHandle columnsFile;
        if (PagedFileManager::instance().openFile("Columns", columnsFile) != 0) {
            PagedFileManager::instance().closeFile(tablesFile);
            return;
        }

        std::vector<Attribute> tablesDesc;
        getTablesRecordDescriptor(tablesDesc);

        RBFM_ScanIterator scanIter;
        std::vector<std::string> projection = {"table-id", "table-name", "file-name"};

        RC rc = RecordBasedFileManager::instance().scan(
                tablesFile,
                tablesDesc,
                "",   // no condition
                NO_OP,
                nullptr,
                projection,
                scanIter);
        if (rc != 0) {
            scanIter.close();
            PagedFileManager::instance().closeFile(tablesFile);
            PagedFileManager::instance().closeFile(columnsFile);
            return;
        }

        std::map<int, std::string> idToName;
        std::map<int, std::string> idToFile;

        RID rid;
        char data[200];
        while (true) {
            rc = scanIter.getNextRecord(rid, data);
            if (rc == RBFM_EOF) {
                break;
            }
            if (rc != 0) {
                break;
            }

            int offset = 0;
            offset += 1;
            int tId;
            memcpy(&tId, data + offset, sizeof(int));
            offset += sizeof(int);

            int tnLen;
            memcpy(&tnLen, data + offset, sizeof(int));
            offset += sizeof(int);
            std::string tName(tnLen, ' ');
            memcpy(&tName[0], data + offset, tnLen);
            offset += tnLen;

            int fnLen;
            memcpy(&fnLen, data + offset, sizeof(int));
            offset += sizeof(int);
            std::string fName(fnLen, ' ');
            memcpy(&fName[0], data + offset, fnLen);
            offset += fnLen;

            idToName[tId] = tName;
            idToFile[tId] = fName;
        }
        scanIter.close();
        PagedFileManager::instance().closeFile(tablesFile);

        std::vector<Attribute> columnsDesc;
        getColumnsRecordDescriptor(columnsDesc);

        RBFM_ScanIterator cScanIter;

        std::cout << "SyncTablesCache: Scanning Columns" << std::endl;

        std::vector<std::string> cProjection = {"column-name", "column-type", "column-length", "column-position", "table-id"};

        rc = RecordBasedFileManager::instance().scan(
                columnsFile,
                columnsDesc,
                "",
                NO_OP,
                nullptr,
                cProjection,
                cScanIter);

        if (rc != 0) {
            cScanIter.close();
            PagedFileManager::instance().closeFile(columnsFile);
            return;
        }

        struct ColInfo {
            std::string name;
            AttrType type;
            int length;
            int position;
            int tableId;
        };
        std::vector<ColInfo> allCols;

        while (true) {
            rc = cScanIter.getNextRecord(rid, data);
            if (rc == RBFM_EOF) {
                break;
            }
            if (rc != 0) {
                break;
            }

            int offset = 0;
            unsigned char nullByte = *((unsigned char*) data + offset);
            offset += 1;
            if (nullByte != 0) {
                continue;
            }

            int cnLen;
            memcpy(&cnLen, data + offset, sizeof(int));
            offset += sizeof(int);
            std::string cname(cnLen, ' ');
            memcpy(&cname[0], data + offset, cnLen);
            offset += cnLen;

            int ctype;
            memcpy(&ctype, data + offset, sizeof(int));
            offset += 4;

            int clen;
            memcpy(&clen, data + offset, sizeof(int));
            offset += 4;

            int cpos;
            memcpy(&cpos, data + offset, sizeof(int));
            offset += 4;

            int tid;
            memcpy(&tid, data + offset, sizeof(int));
            offset += 4;

            ColInfo ci;
            ci.name = cname;
            ci.type = (AttrType) ctype;
            ci.length = clen;
            ci.position = cpos;
            ci.tableId = tid;
            allCols.push_back(ci);
        }
        cScanIter.close();
        PagedFileManager::instance().closeFile(columnsFile);

        std::map<int, std::vector<ColInfo>> grouped;
        for (auto &col : allCols) {
            if (idToName.find(col.tableId) == idToName.end()) {
                continue;
            }
            grouped[col.tableId].push_back(col);
        }

        for (auto &kv : grouped) {
            auto &colsVec = kv.second;
            std::sort(colsVec.begin(), colsVec.end(), [](const ColInfo &a, const ColInfo &b){
                return a.position < b.position;
            });
        }

        for (auto &pair : idToName) {
            int tId = pair.first;
            const std::string &tName = pair.second;
            const std::string &fName = idToFile[tId];

            TableMeta meta;
            meta.tableId = tId;
            meta.fileName = fName;

            auto itCols = grouped.find(tId);
            if (itCols != grouped.end()) {
                for (auto &col : itCols->second) {
                    Attribute A;
                    A.name = col.name;
                    A.type = col.type;
                    A.length = col.length;
                    meta.attrs.push_back(A);
                }
            }
            g_tableCache[tName] = meta;
        }

    }

    RelationManager::RelationManager() {
        SyncTablesCache();
    }

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
            RecordBasedFileManager::instance().closeFile(tablesFile);
            return -1;
        }

        std::vector<Attribute> tablesDesc;
        std::vector<Attribute> columnsDesc;
        getTablesRecordDescriptor(tablesDesc);
        getColumnsRecordDescriptor(columnsDesc);

        insertRowGeneric(tablesFile, tablesDesc, {"1", "Tables", "Tables"});
        insertRowGeneric(tablesFile, tablesDesc, {"2", "Columns", "Columns"});

        insertRowGeneric(columnsFile, columnsDesc, {"1", "table-id",    "0", "4",  "1"});
        insertRowGeneric(columnsFile, columnsDesc, {"1", "table-name",  "2", "50", "2"});
        insertRowGeneric(columnsFile, columnsDesc, {"1", "file-name",   "2", "50", "3"});

        insertRowGeneric(columnsFile, columnsDesc, {"2", "table-id",       "0", "4",  "1"});
        insertRowGeneric(columnsFile, columnsDesc, {"2", "column-name",     "2", "50", "2"});
        insertRowGeneric(columnsFile, columnsDesc, {"2", "column-type",     "0", "4",  "3"});
        insertRowGeneric(columnsFile, columnsDesc, {"2", "column-length",   "0", "4",  "4"});
        insertRowGeneric(columnsFile, columnsDesc, {"2", "column-position", "0", "4",  "5"});

        RecordBasedFileManager::instance().closeFile(tablesFile);
        RecordBasedFileManager::instance().closeFile(columnsFile);

        g_tableCache.clear();
        SyncTablesCache();

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        if (RecordBasedFileManager::instance().destroyFile("Tables") != 0) {
            return -1;
        }
        if (RecordBasedFileManager::instance().destroyFile("Columns") != 0) {
            return -1;
        }

        g_tableCache.clear();
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {

        if (tableName == "Tables" || tableName == "Columns") {
            return -1;
        }

        if (RecordBasedFileManager::instance().createFile(tableName) != 0) {
            return -1;
        }

        int newTableId = findMaxTableIdInCache() + 1;

        FileHandle tablesFile;
        if (RecordBasedFileManager::instance().openFile("Tables", tablesFile) != 0) {
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }
        std::vector<Attribute> tablesDesc;
        getTablesRecordDescriptor(tablesDesc);

        if (insertTableMetadata(tablesFile, tablesDesc, newTableId, tableName) != 0) {
            RecordBasedFileManager::instance().closeFile(tablesFile);
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }
        RecordBasedFileManager::instance().closeFile(tablesFile);

        FileHandle columnsFile;
        if (RecordBasedFileManager::instance().openFile("Columns", columnsFile) != 0) {
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }
        std::vector<Attribute> columnsDesc;
        getColumnsRecordDescriptor(columnsDesc);
        if (insertColumnsMetadata(columnsFile, columnsDesc, newTableId, attrs) != 0) {
            RecordBasedFileManager::instance().closeFile(columnsFile);
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }
        RecordBasedFileManager::instance().closeFile(columnsFile);

        TableMeta meta;
        meta.tableId = newTableId;
        meta.fileName = tableName;
        meta.attrs = attrs;
        g_tableCache[tableName] = meta;

        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if (tableName == "Tables" || tableName == "Columns") {
            return -1;
        }

        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) {
            return -1; // not found
        }

        if (RecordBasedFileManager::instance().destroyFile(it->second.fileName) != 0) {
            return -1;
        }

        RC rc = removeTableEntryFromCatalogs(tableName);
        if (rc != 0) {
            return -1;
        }

        g_tableCache.erase(it);

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) {
            return -1;
        }
        attrs = it->second.attrs;
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (tableName == "Tables" || tableName == "Columns") {
            return -1;
        }

        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) {
            return -1;
        }
        const std::string &fileName = it->second.fileName;
        const auto &recordDescriptor = it->second.attrs;

        FileHandle fileHandle;
        if (RecordBasedFileManager::instance().openFile(fileName, fileHandle) != 0) {
            return -1;
        }
        RC rc = RecordBasedFileManager::instance().insertRecord(fileHandle, recordDescriptor, data, rid);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        FileHandle fileHandle;
        if (RecordBasedFileManager::instance().openFile(it->second.fileName, fileHandle) != 0) {
            return -1;
        }
        RC rc = RecordBasedFileManager::instance().deleteRecord(fileHandle, it->second.attrs, rid);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        FileHandle fileHandle;
        if (RecordBasedFileManager::instance().openFile(it->second.fileName, fileHandle) != 0) {
            return -1;
        }
        RC rc = RecordBasedFileManager::instance().updateRecord(fileHandle, it->second.attrs, data, rid);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        FileHandle fileHandle;
        if (RecordBasedFileManager::instance().openFile(it->second.fileName, fileHandle) != 0) {
            return -1;
        }
        RC rc = RecordBasedFileManager::instance().readRecord(fileHandle, it->second.attrs, rid, data);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return RecordBasedFileManager::instance().printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid,
                                      const std::string &attributeName, void *data) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        FileHandle fileHandle;
        if (RecordBasedFileManager::instance().openFile(it->second.fileName, fileHandle) != 0) {
            return -1;
        }
        RC rc = RecordBasedFileManager::instance().readAttribute(fileHandle, it->second.attrs, rid, attributeName, data);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        if (RecordBasedFileManager::instance().openFile(it->second.fileName,
                                                        rm_ScanIterator.fileHandle) != 0) {
            return -1;
        }

        RC rc = RecordBasedFileManager::instance().scan(
                rm_ScanIterator.fileHandle,
                it->second.attrs,
                conditionAttribute,
                compOp,
                value,
                attributeNames,
                rm_ScanIterator.rbfmIter);
        if (rc != 0) {

            RecordBasedFileManager::instance().closeFile(rm_ScanIterator.fileHandle);
            return -1;
        }
        rm_ScanIterator.isOpen = true;
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;
    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        if (!isOpen) return RM_EOF;
        RC rc = rbfmIter.getNextRecord(rid, data);
        if (rc == RBFM_EOF) return RM_EOF;
        return rc;
    }

    RC RM_ScanIterator::close() {
        if (!isOpen) return 0;
        rbfmIter.close();
        RecordBasedFileManager::instance().closeFile(fileHandle);
        isOpen = false;
        return 0;
    }

    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        int tableId = it->second.tableId;
        RC rc = insertOneColumnMetadata(tableId, attr, it->second.attrs.size() + 1 /* position */);
        if (rc != 0) return rc;

        it->second.attrs.push_back(attr);

        return 0;
    }

    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        auto it = g_tableCache.find(tableName);
        if (it == g_tableCache.end()) return -1;

        auto &vec = it->second.attrs;
        vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const Attribute &A){
            return (A.name == attributeName);
        }), vec.end());

        return 0;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    RC RelationManager::indexScan(const std::string &tableName,
                                  const std::string &attributeName,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        return -1;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;
    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC RM_IndexScanIterator::close() {
        return -1;
    }

} // namespace PeterDB
