#ifndef RM_UTILS_H
#define RM_UTILS_H

#include <vector>
#include "src/include/rbfm.h"

namespace PeterDB
{
    // Build the "Tables" schema
    void getTablesRecordDescriptor(std::vector<Attribute>& descriptor);

    // Build the "Columns" schema
    void getColumnsRecordDescriptor(std::vector<Attribute>& descriptor);

    // Convert a vector of string values into the correct “insertRecord” format:
    //   null-indicators + field data (INT, REAL, or VARCHAR).
    // Returns 0 on success, -1 on error (e.g., mismatch in size).
    RC buildDataFromStrings(const std::vector<Attribute>& recordDescriptor,
                            const std::vector<std::string>& values,
                            void* buffer,
                            unsigned& recordSize);

    // A helper that calls buildDataFromStrings() and then rbfm.insertRecord().
    // This is a convenient one-liner for inserting a row given a vector of string values.
    RC insertRowGeneric(FileHandle& fileHandle,
                        const std::vector<Attribute>& recordDescriptor,
                        const std::vector<std::string>& fieldValues);

    // Scans the "Tables" file to find the maximum "table-id", returns max+1 via "nextTableId".
    RC getNextTableId(FileHandle& tablesFile,
                      const std::vector<Attribute>& tablesDescriptor,
                      int& nextTableId);

    // Inserts a single row into "Tables" => (tableId, tableName, fileName)
    RC insertTableMetadata(FileHandle& tablesFile,
                           const std::vector<Attribute>& tablesDescriptor,
                           int tableId,
                           const std::string& tableName);

    // Inserts rows into "Columns" for each attribute =>
    //   (tableId, attr.name, attr.type, attr.length, position)
    RC insertColumnsMetadata(FileHandle& columnsFile,
                             const std::vector<Attribute>& columnsDescriptor,
                             int tableId,
                             const std::vector<Attribute>& attrs);

    // Look up file-name + table-id for the given tableName by scanning "Tables"
    //    Returns 0 on success, -1 if not found or error.
    RC getFileNameAndTableId(const std::string& tableName,
                             std::string& fileName,
                             int& tableId);

    // Build the vector<Attribute> by scanning "Columns" for all rows with that tableId
    //    (Ordered by column-position)
    RC getAttributesForTableId(int tableId,
        std::vector<Attribute>& attrs);

    // Remove from "Tables" and "Columns" all rows describing `tableName`.
    // This will:
    //   1) Look up (tableId, fileName) for tableName
    //   2) Delete the row from "Tables" with table-name = tableName
    //   3) Delete all rows from "Columns" with table-id = tableId
    RC removeTableEntryFromCatalogs(const std::string &tableName);

    // A generic function that opens a file, scans with condition (attr=compValue),
    // collects all RIDs, then calls rbfm.deleteRecord(...) for each.
    // projection can be empty because we only need RIDs to delete.
    RC removeAllRowsMatching(const std::string &catalogFileName,
                             const std::vector<Attribute> &catalogDescriptor,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value);
} // namespace PeterDB

#endif
