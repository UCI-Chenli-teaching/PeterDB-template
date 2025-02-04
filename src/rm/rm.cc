#include "src/include/rm.h"

#include "src/include/rm_utils.h"

namespace PeterDB
{
    RelationManager& RelationManager::instance()
    {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager&) = default;

    RelationManager& RelationManager::operator=(const RelationManager&) = default;

    RC RelationManager::createCatalog()
    {
        if (RecordBasedFileManager::instance().createFile("Tables") != 0)
        {
            return -1;
        }
        if (RecordBasedFileManager::instance().createFile("Columns") != 0)
        {
            RecordBasedFileManager::instance().destroyFile("Tables");
            return -1;
        }

        FileHandle tablesFile, columnsFile;
        if (RecordBasedFileManager::instance().openFile("Tables", tablesFile) != 0)
        {
            return -1;
        }
        if (RecordBasedFileManager::instance().openFile("Columns", columnsFile) != 0)
        {
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

    RC RelationManager::deleteCatalog()
    {
        RC rc = RecordBasedFileManager::instance().destroyFile("Tables");
        if (rc != 0)
        {
            return -1;
        }

        rc = RecordBasedFileManager::instance().destroyFile("Columns");
        if (rc != 0)
        {
            return -1;
        }

        return 0;
    }

    RC RelationManager::createTable(const std::string& tableName, const std::vector<Attribute>& attrs)
    {
        RC rc = RecordBasedFileManager::instance().createFile(tableName);
        if (rc != 0)
        {
            // Could not create the file (maybe it already exists)
            return -1;
        }

        // 2) Open "Tables"
        FileHandle tablesFile;
        rc = RecordBasedFileManager::instance().openFile("Tables", tablesFile);
        if (rc != 0)
        {
            // If we fail to open "Tables", remove the user file we just created
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }

        // 3) Get table-id & insert the new row in "Tables"
        std::vector<Attribute> tablesDesc;
        getTablesRecordDescriptor(tablesDesc);

        int tableId;
        rc = getNextTableId(tablesFile, tablesDesc, tableId);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(tablesFile);
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }

        rc = insertTableMetadata(tablesFile, tablesDesc, tableId, tableName);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(tablesFile);
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }

        // close "Tables"
        RecordBasedFileManager::instance().closeFile(tablesFile);

        // 4) Open "Columns"
        FileHandle columnsFile;
        rc = RecordBasedFileManager::instance().openFile("Columns", columnsFile);
        if (rc != 0)
        {
            // If we cannot open "Columns", revert the new user file
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }

        // 5) Insert each attribute into "Columns"
        std::vector<Attribute> columnsDesc;
        getColumnsRecordDescriptor(columnsDesc);

        rc = insertColumnsMetadata(columnsFile, columnsDesc, tableId, attrs);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(columnsFile);
            RecordBasedFileManager::instance().destroyFile(tableName);
            return -1;
        }

        // close "Columns"
        RecordBasedFileManager::instance().closeFile(columnsFile);

        // 6) Done
        return 0;
    }

    RC RelationManager::deleteTable(const std::string& tableName)
    {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string& tableName, std::vector<Attribute>& attrs)
    {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string& tableName, const void* data, RID& rid)
    {
        std::string fileName;
        int tableId;
        RC rc = getFileNameAndTableId(tableName, fileName, tableId);
        if (rc != 0)
        {
            return -1;
        }

        std::vector<Attribute> recordDescriptor;
        rc = getAttributesForTableId(tableId, recordDescriptor);
        if (rc != 0)
        {
            return -1;
        }

        FileHandle fileHandle;
        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if (rc != 0)
        {
            return -1;
        }

        rc = RecordBasedFileManager::instance().insertRecord(fileHandle, recordDescriptor, data, rid);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(fileHandle);
            return -1;
        }

        RecordBasedFileManager::instance().closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string& tableName, const RID& rid)
    {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string& tableName, const void* data, const RID& rid)
    {
        return -1;
    }

    RC RelationManager::readTuple(const std::string& tableName, const RID& rid, void* data)
    {
        std::string fileName;
        int tableId;
        RC rc = getFileNameAndTableId(tableName, fileName, tableId);
        if (rc != 0)
        {
            return -1;
        }

        std::vector<Attribute> recordDescriptor;
        rc = getAttributesForTableId(tableId, recordDescriptor);
        if (rc != 0)
        {
            return -1;
        }

        FileHandle fileHandle;
        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if (rc != 0)
        {
            return -1;
        }

        rc = RecordBasedFileManager::instance().readRecord(fileHandle, recordDescriptor, rid, data);

        RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::printTuple(const std::vector<Attribute>& attrs, const void* data, std::ostream& out)
    {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string& tableName, const RID& rid, const std::string& attributeName,
                                      void* data)
    {
        return -1;
    }

    RC RelationManager::scan(const std::string& tableName,
                             const std::string& conditionAttribute,
                             const CompOp compOp,
                             const void* value,
                             const std::vector<std::string>& attributeNames,
                             RM_ScanIterator& rm_ScanIterator)
    {
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID& rid, void* data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string& tableName, const std::string& attributeName)
    {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string& tableName, const Attribute& attr)
    {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string& tableName, const std::string& attributeName)
    {
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string& tableName, const std::string& attributeName)
    {
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string& tableName,
                                  const std::string& attributeName,
                                  const void* lowKey,
                                  const void* highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator& rm_IndexScanIterator)
    {
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID& rid, void* key)
    {
        return -1;
    }

    RC RM_IndexScanIterator::close()
    {
        return -1;
    }
} // namespace PeterDB
