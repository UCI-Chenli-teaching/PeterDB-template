#include "src/include/rm_utils.h"

#include <algorithm>

#include "src/include/rbfm.h"

#include <cmath>    // for std::ceil
#include <cstring>  // for memcpy
#include <string>   // for stoi/stof

namespace PeterDB
{
    RC buildDataFromStrings(const std::vector<Attribute>& recordDescriptor,
                            const std::vector<std::string>& values,
                            void* buffer,
                            unsigned& recordSize)
    {
        // Must match number of columns
        if (values.size() != recordDescriptor.size())
        {
            return -1;
        }

        const unsigned numFields = (unsigned)recordDescriptor.size();
        const int nullIndicatorSize = (int)std::ceil((double)numFields / 8.0);

        unsigned offset = 0;

        std::vector<unsigned char> nullIndicator(nullIndicatorSize, 0);
        memcpy((char*)buffer + offset, nullIndicator.data(), nullIndicatorSize);
        offset += nullIndicatorSize;

        // Now copy each field from 'values'
        for (unsigned i = 0; i < numFields; i++)
        {
            const Attribute& attr = recordDescriptor[i];
            const std::string& valStr = values[i];

            switch (attr.type)
            {
            case TypeInt:
                {
                    int ival = std::stoi(valStr); // convert string -> int
                    memcpy((char*)buffer + offset, &ival, sizeof(int));
                    offset += sizeof(int);
                    break;
                }
            case TypeReal:
                {
                    float fval = std::stof(valStr); // convert string -> float
                    memcpy((char*)buffer + offset, &fval, sizeof(float));
                    offset += sizeof(float);
                    break;
                }
            case TypeVarChar:
                {
                    // We store 4 bytes = length, then the actual chars
                    int length = (int)valStr.size();
                    memcpy((char*)buffer + offset, &length, sizeof(int));
                    offset += sizeof(int);

                    memcpy((char*)buffer + offset, valStr.c_str(), length);
                    offset += length;
                    break;
                }
            default:
                // unknown attr.type
                return -1;
            }
        }

        recordSize = offset;
        return 0; // success
    }


    RC insertRowGeneric(FileHandle& fileHandle,
                        const std::vector<Attribute>& recordDescriptor,
                        const std::vector<std::string>& fieldValues)
    {
        char recordBuffer[PAGE_SIZE];
        unsigned recordSize = 0;
        RC rc = buildDataFromStrings(recordDescriptor, fieldValues, recordBuffer, recordSize);
        if (rc != 0)
        {
            return rc;
        }

        // Insert into the file
        RID rid;
        rc = RecordBasedFileManager::instance().insertRecord(fileHandle,
                                                             recordDescriptor,
                                                             recordBuffer,
                                                             rid);
        return rc;
    }

    void getTablesRecordDescriptor(std::vector<Attribute>& descriptor)
    {
        descriptor.clear();

        Attribute attr;
        // table-id : INT
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // table-name : VARCHAR(50)
        attr.name = "table-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        descriptor.push_back(attr);

        // file-name : VARCHAR(50)
        attr.name = "file-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        descriptor.push_back(attr);
    }

    void getColumnsRecordDescriptor(std::vector<Attribute>& descriptor)
    {
        descriptor.clear();

        Attribute attr;
        // table-id : INT
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // column-name : VARCHAR(50)
        attr.name = "column-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        descriptor.push_back(attr);

        // column-type : INT
        attr.name = "column-type";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // column-length : INT
        attr.name = "column-length";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // column-position : INT
        attr.name = "column-position";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);
    }

    RC getNextTableId(FileHandle& tablesFile,
                      const std::vector<Attribute>& tablesDescriptor,
                      int& nextTableId)
    {
        // We'll do a scan over "Tables" with no condition,
        // reading "table-id" to find the maximum.
        RBFM_ScanIterator scanIter;
        std::vector<std::string> projection{"table-id"};

        RC rc = RecordBasedFileManager::instance().scan(tablesFile,
                                                        tablesDescriptor,
                                                        "", // no condition
                                                        NO_OP,
                                                        nullptr,
                                                        projection,
                                                        scanIter);
        if (rc != 0)
        {
            return -1;
        }

        int maxId = 0;
        char data[50];
        while (true)
        {
            RID rid;
            rc = scanIter.getNextRecord(rid, data);
            if (rc == RBFM_EOF)
            {
                break;
            }
            if (rc != 0)
            {
                // Some error while scanning
                scanIter.close();
                return -1;
            }
            // parse single attribute => [nullIndicator][4 bytes int]
            unsigned char nullByte = *(unsigned char*)data;
            bool isNull = ((nullByte & 0x80) != 0);
            if (!isNull)
            {
                int currentId;
                memcpy(&currentId, data + 1, sizeof(int));
                if (currentId > maxId)
                {
                    maxId = currentId;
                }
            }
        }
        scanIter.close();

        nextTableId = maxId + 1;
        return 0;
    }


    RC insertTableMetadata(FileHandle& tablesFile,
                           const std::vector<Attribute>& tablesDescriptor,
                           int tableId,
                           const std::string& tableName)
    {
        // build row => (table-id, table-name, file-name)
        // file-name usually same as tableName
        std::vector<std::string> rowValues = {
            std::to_string(tableId),
            tableName,
            tableName
        };

        return insertRowGeneric(tablesFile, tablesDescriptor, rowValues);
    }


    RC insertColumnsMetadata(FileHandle& columnsFile,
                             const std::vector<Attribute>& columnsDescriptor,
                             int tableId,
                             const std::vector<Attribute>& attrs)
    {
        int position = 1;
        for (auto& attr : attrs)
        {
            // (tableId, colName, colType, colLength, position)
            std::vector<std::string> rowValues = {
                std::to_string(tableId),
                attr.name,
                std::to_string((int)attr.type),
                std::to_string(attr.length),
                std::to_string(position)
            };

            RC rc = insertRowGeneric(columnsFile, columnsDescriptor, rowValues);
            if (rc != 0)
            {
                return -1; // insertion failed
            }
            position++;
        }
        return 0;
    }

    RC getFileNameAndTableId(const std::string& tableName,
                             std::string& fileName,
                             int& tableId)
    {
        FileHandle tablesFile;
        RC rc = RecordBasedFileManager::instance().openFile("Tables", tablesFile);
        if (rc != 0)
        {
            return -1;
        }

        std::vector<Attribute> tablesDesc;
        getTablesRecordDescriptor(tablesDesc);

        RBFM_ScanIterator scanIter;
        std::vector<std::string> projection{"table-id", "file-name"};
        std::string conditionAttr = "table-name";
        CompOp compOp = EQ_OP;

        int nameLen = (int)tableName.size();
        std::vector<char> valueBuf(sizeof(int) + nameLen);
        memcpy(valueBuf.data(), &nameLen, sizeof(int));
        memcpy(valueBuf.data() + sizeof(int), tableName.data(), nameLen);

        rc = RecordBasedFileManager::instance().scan(
            tablesFile,
            tablesDesc,
            conditionAttr,
            compOp,
            valueBuf.data(),
            projection,
            scanIter);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(tablesFile);
            return -1;
        }

        RID rid;
        char data[200];
        RC fetchRc = scanIter.getNextRecord(rid, data);
        if (fetchRc == RBFM_EOF)
        {
            // Not found
            scanIter.close();
            RecordBasedFileManager::instance().closeFile(tablesFile);
            return -1;
        }
        if (fetchRc != 0)
        {
            // Some error
            scanIter.close();
            RecordBasedFileManager::instance().closeFile(tablesFile);
            return -1;
        }

        // 'data' now has 2 fields: (table-id, file-name), all non-null presumably
        // parse them:
        //   [1-byte nullIndicator=0][(table-id)4 bytes][(fileName-len)4 bytes + fileName chars]
        unsigned offset = 0;
        unsigned char nullByte = *(unsigned char*)(data + offset);
        offset += 1; // move past null-indicator
        if (nullByte != 0x00)
        {
            // unexpected null fields
            scanIter.close();
            RecordBasedFileManager::instance().closeFile(tablesFile);
            return -1;
        }

        // table-id
        memcpy(&tableId, data + offset, sizeof(int));
        offset += sizeof(int);

        // file-name (varchar)
        int varLen;
        memcpy(&varLen, data + offset, sizeof(int));
        offset += sizeof(int);

        fileName.resize(varLen);
        memcpy(&fileName[0], data + offset, varLen);
        offset += varLen;

        scanIter.close();
        RecordBasedFileManager::instance().closeFile(tablesFile);

        return 0;
    }


    /**
     *  2) getAttributesForTableId:
     *     Scans "Columns" where "table-id = tableId"
     *     to reconstruct vector<Attribute> => (name, type, length).
     *     We also read "column-position" to store them in ascending order, if needed.
     */
    RC getAttributesForTableId(int tableId,
                               std::vector<Attribute>& attrs)
    {
        // Open "Columns"
        FileHandle columnsFile;
        RC rc = RecordBasedFileManager::instance().openFile("Columns", columnsFile);
        if (rc != 0)
        {
            return -1;
        }

        // Build descriptor for "Columns"
        std::vector<Attribute> columnsDesc;
        getColumnsRecordDescriptor(columnsDesc);

        // Condition => "table-id" = tableId (TypeInt)
        RBFM_ScanIterator scanIter;
        std::vector<std::string> projection{
            "column-name",
            "column-type",
            "column-length",
            "column-position"
        };
        std::string conditionAttr = "table-id";
        CompOp compOp = EQ_OP;

        // "value" for an int: just 4 bytes
        int condVal = tableId;

        rc = RecordBasedFileManager::instance().scan(
            columnsFile,
            columnsDesc,
            conditionAttr,
            compOp,
            &condVal,
            projection,
            scanIter);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(columnsFile);
            return -1;
        }

        // We'll gather the results in a temporary structure that includes columnPosition
        struct ColInfo
        {
            std::string name;
            AttrType type;
            int length;
            int position;
        };
        std::vector<ColInfo> temp;

        while (true)
        {
            RID rid;
            char data[300];
            RC fetchRc = scanIter.getNextRecord(rid, data);
            if (fetchRc == RBFM_EOF)
            {
                break;
            }
            if (fetchRc != 0)
            {
                scanIter.close();
                RecordBasedFileManager::instance().closeFile(columnsFile);
                return -1;
            }

            unsigned offset = 0;
            unsigned char nullByte = *(unsigned char*)(data + offset);
            offset += 1;
            if (nullByte != 0x00)
            {
                // unexpected null
                continue;
            }

            // column-name => varchar
            int varLen;
            memcpy(&varLen, data + offset, sizeof(int));
            offset += sizeof(int);

            std::string colName;
            colName.resize(varLen);
            memcpy(&colName[0], data + offset, varLen);
            offset += varLen;

            // column-type => int
            int colType;
            memcpy(&colType, data + offset, sizeof(int));
            offset += sizeof(int);

            // column-length => int
            int colLen;
            memcpy(&colLen, data + offset, sizeof(int));
            offset += sizeof(int);

            // column-position => int
            int colPos;
            memcpy(&colPos, data + offset, sizeof(int));
            offset += sizeof(int);

            ColInfo ci;
            ci.name = colName;
            ci.type = (AttrType)colType;
            ci.length = colLen;
            ci.position = colPos;
            temp.push_back(ci);
        }

        scanIter.close();
        RecordBasedFileManager::instance().closeFile(columnsFile);

        // Sort by column-position so the attributes appear in the correct order
        std::sort(temp.begin(), temp.end(), [](const ColInfo& a, const ColInfo& b)
        {
            return a.position < b.position;
        });

        attrs.clear();
        for (auto& ci : temp)
        {
            Attribute A;
            A.name = ci.name;
            A.type = ci.type;
            A.length = ci.length;
            attrs.push_back(A);
        }

        return 0;
    }

    RC removeAllRowsMatching(const std::string& catalogFileName,
                             const std::vector<Attribute>& catalogDescriptor,
                             const std::string& conditionAttribute,
                             const CompOp compOp,
                             const void* value)
    {
        FileHandle fileHandle;
        RC rc = RecordBasedFileManager::instance().openFile(catalogFileName, fileHandle);
        if (rc != 0)
        {
            return -1;
        }

        RBFM_ScanIterator scanIter;
        std::vector<std::string> projection;

        rc = RecordBasedFileManager::instance().scan(
            fileHandle,
            catalogDescriptor,
            conditionAttribute,
            compOp,
            value,
            projection,
            scanIter);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(fileHandle);
            return -1;
        }

        std::vector<RID> toDelete;
        RID rid;
        char dummy[1];
        while (true)
        {
            rc = scanIter.getNextRecord(rid, dummy);
            if (rc == RBFM_EOF)
            {
                break;
            }
            if (rc != 0)
            {
                scanIter.close();
                RecordBasedFileManager::instance().closeFile(fileHandle);
                return -1;
            }
            toDelete.push_back(rid);
        }
        scanIter.close();

        for (auto& delRid : toDelete)
        {
            rc = RecordBasedFileManager::instance().deleteRecord(fileHandle, catalogDescriptor, delRid);
            if (rc != 0)
            {
                RecordBasedFileManager::instance().closeFile(fileHandle);
                return -1;
            }
        }

        RecordBasedFileManager::instance().closeFile(fileHandle);
        return 0;
    }

    RC removeTableEntryFromCatalogs(const std::string& tableName)
    {
        std::string fileName;
        int tableId;
        RC rc = getFileNameAndTableId(tableName, fileName, tableId);
        if (rc != 0)
        {
            return -1;
        }

        std::vector<Attribute> tablesDesc;
        getTablesRecordDescriptor(tablesDesc);

        int nameLen = (int)tableName.size();
        std::vector<char> valueBuf(sizeof(int) + nameLen);
        memcpy(valueBuf.data(), &nameLen, sizeof(int));
        memcpy(valueBuf.data() + sizeof(int), tableName.data(), nameLen);

        rc = removeAllRowsMatching("Tables",
                                   tablesDesc,
                                   "table-name",
                                   EQ_OP,
                                   valueBuf.data());
        if (rc != 0)
        {
            return -1;
        }

        std::vector<Attribute> columnsDesc;
        getColumnsRecordDescriptor(columnsDesc);

        rc = removeAllRowsMatching("Columns",
                                   columnsDesc,
                                   "table-id",
                                   EQ_OP,
                                   &tableId);
        if (rc != 0)
        {
            return -1;
        }

        return 0;
    }

    RC insertOneColumnMetadata(int tableId,
                               const Attribute& attr,
                               int position)
    {
        // 1) Open the "Columns" catalog
        FileHandle columnsFile;
        RC rc = RecordBasedFileManager::instance().openFile("Columns", columnsFile);
        if (rc != 0)
        {
            return -1;
        }

        // 2) Build the record descriptor for the "Columns" table
        //    (this is the same descriptor used in createCatalog())
        std::vector<Attribute> columnsDesc;
        getColumnsRecordDescriptor(columnsDesc);

        // 3) Construct a record in the format:
        //    (table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
        //    - Exactly 5 attributes
        //    - We have 1-byte of null-indicator if none of them is NULL
        const int numFields = 5;
        unsigned char nullIndicator = 0; // 0 means "no attribute is null"
        // We only need 1 byte for null indicators if numFields <= 8
        char recordData[200]; // Enough for a short record
        int offset = 0;

        // (a) Write the null-indicator byte
        memcpy(recordData + offset, &nullIndicator, 1);
        offset += 1;

        // (b) table-id (int)
        memcpy(recordData + offset, &tableId, sizeof(int));
        offset += sizeof(int);

        // (c) column-name (varchar)
        //  First write 4 bytes = length
        int nameLen = (int)attr.name.size();
        memcpy(recordData + offset, &nameLen, sizeof(int));
        offset += sizeof(int);
        //  Then the actual characters
        memcpy(recordData + offset, attr.name.c_str(), nameLen);
        offset += nameLen;

        // (d) column-type (int)
        int colType = (int)attr.type;
        memcpy(recordData + offset, &colType, sizeof(int));
        offset += sizeof(int);

        // (e) column-length (int)
        int colLen = (int)attr.length;
        memcpy(recordData + offset, &colLen, sizeof(int));
        offset += sizeof(int);

        // (f) column-position (int)
        memcpy(recordData + offset, &position, sizeof(int));
        offset += sizeof(int);

        // 4) Insert the record
        RID rid;
        rc = RecordBasedFileManager::instance().insertRecord(columnsFile, columnsDesc, recordData, rid);
        if (rc != 0)
        {
            RecordBasedFileManager::instance().closeFile(columnsFile);
            return -1;
        }

        // 5) Close the "Columns" file
        RecordBasedFileManager::instance().closeFile(columnsFile);

        return 0;
    }
} // namespace PeterDB
