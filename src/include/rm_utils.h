#ifndef RM_UTILS_H
#define RM_UTILS_H

#include <vector>
#include "src/include/rbfm.h"

namespace PeterDB {

    // Build the "Tables" schema
    void getTablesRecordDescriptor(std::vector<Attribute> &descriptor);

    // Build the "Columns" schema
    void getColumnsRecordDescriptor(std::vector<Attribute> &descriptor);

    // Convert a vector of string values into the correct “insertRecord” format:
    //   null-indicators + field data (INT, REAL, or VARCHAR).
    // Returns 0 on success, -1 on error (e.g., mismatch in size).
    RC buildDataFromStrings(const std::vector<Attribute> &recordDescriptor,
                            const std::vector<std::string> &values,
                            void *buffer,
                            unsigned &recordSize);

    // A helper that calls buildDataFromStrings() and then rbfm.insertRecord().
    // This is a convenient one-liner for inserting a row given a vector of string values.
    RC insertRowGeneric(FileHandle &fileHandle,
                        const std::vector<Attribute> &recordDescriptor,
                        const std::vector<std::string> &fieldValues);

} // namespace PeterDB

#endif