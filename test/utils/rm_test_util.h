#ifndef _test_util_h_
#define _test_util_h_

#include "src/include/rm.h"
#include "gtest/gtest.h"
#include "test/utils/general_test_utils.h"
#include "test/utils/rbfm_test_utils.h"

namespace PeterDBTesting {

    class RM_Catalog_Test : public ::testing::Test {

    public:
        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();

    };

    class RM_Tuple_Test : public ::testing::Test {
    protected:

        std::string tableName = "rm_test_table";
        PeterDB::FileHandle fileHandle;
        size_t bufSize;
        void *inBuffer = nullptr, *outBuffer = nullptr;
        unsigned char *nullsIndicator = nullptr;
        unsigned char *nullsIndicatorWithNull = nullptr;
        bool destroyFile = true;
        PeterDB::RID rid;
        std::vector<PeterDB::Attribute> attrs;

    public:

        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();

        void SetUp() override {

            if (!fileExists(tableName)) {

                // Try to delete the System Catalog.
                // If this is the first time, it will generate an error. It's OK and we will ignore that.
                rm.deleteCatalog();

                // Create Catalog
                ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

                // Create a table
                std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                        "CREATE TABLE " + tableName + " (emp_name VARCHAR(50), age INT, height REAL, salary REAL)");
                ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                            << "Create table " << tableName << " should succeed.";
                ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";
            }

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should not fail.";
            }

            // Delete Catalog
            ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";
        }

    };

    class RM_Scan_Test : public RM_Tuple_Test {
    protected:
        PeterDB::RM_ScanIterator rmsi;

    public:

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            // Close the iterator
            ASSERT_EQ(rmsi.close(), success) << "RM_ScanIterator should be able to close.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Deleting the table should succeed.";
            }

            // Delete Catalog
            ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";
        };
    };

    class RM_Large_Table_Test : public RM_Scan_Test {
    protected:
        size_t bufSize = 4000;
        std::string tableName = "rm_test_large_table";
        bool destroyFile = false;
        std::vector<PeterDB::RID> rids;
        std::vector<size_t> sizes;
    public:

        void SetUp() override {

            if (!fileExists(tableName)) {
                // Try to delete the System Catalog.
                // If this is the first time, it will generate an error. It's OK and we will ignore that.
                rm.deleteCatalog();

                // Create Catalog
                ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";
                createLargeTable(tableName);
            }
        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            // Close the iterator
            ASSERT_EQ(rmsi.close(), success) << "RM_ScanIterator should be able to close.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Deleting the table should succeed.";

                // Delete Catalog
                ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";

                remove("rids_file");
                remove("sizes_file");
            }

        };

        // Create a large table for pressure test
        void createLargeTable(const std::string &largeTableName) {

            // 1. Create Table
            std::vector<PeterDB::Attribute> attrs;

            int index = 0;

            for (unsigned i = 0; i < 10; i++) {
                PeterDB::Attribute attr;
                std::string suffix = std::to_string(index);
                attr.name = "attr" + suffix;
                attr.type = PeterDB::TypeVarChar;
                attr.length = (PeterDB::AttrLength) 50;
                attrs.push_back(attr);
                index++;

                suffix = std::to_string(index);
                attr.name = "attr" + suffix;
                attr.type = PeterDB::TypeInt;
                attr.length = (PeterDB::AttrLength) 4;
                attrs.push_back(attr);
                index++;

                suffix = std::to_string(index);
                attr.name = "attr" + suffix;
                attr.type = PeterDB::TypeReal;
                attr.length = (PeterDB::AttrLength) 4;
                attrs.push_back(attr);
                index++;
            }

            ASSERT_EQ(rm.createTable(largeTableName, attrs), success)
                                        << "Create table " << largeTableName << " should succeed.";

        }

        static void prepareLargeTuple(int attributeCount, unsigned char *nullAttributesIndicator, const unsigned index,
                                      void *buffer, size_t &size) {
            size_t offset = 0;

            // Null-indicators
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

            // Null-indicator for the fields
            memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // compute the count
            unsigned count = index % 50 + 1;

            // compute the letter
            char text = (char) (index % 26 + 97);

            for (unsigned i = 0; i < 10; i++) {
                // length
                memcpy((char *) buffer + offset, &count, sizeof(int));
                offset += sizeof(int);

                // varchar
                for (int j = 0; j < count; j++) {
                    memcpy((char *) buffer + offset, &text, 1);
                    offset += 1;
                }

                // integer
                memcpy((char *) buffer + offset, &index, sizeof(int));
                offset += sizeof(int);

                // real
                auto real = (float) (index + 1);
                memcpy((char *) buffer + offset, &real, sizeof(float));
                offset += sizeof(float);
            }
            size = offset;
        }

    };

    class RM_Catalog_Scan_Test : public RM_Scan_Test {

    public:
        void checkCatalog(const std::string &expectedString) {
            memset(outBuffer, 0, bufSize);
            ASSERT_NE(rmsi.getNextTuple(rid, outBuffer), RM_EOF) << "Scan should continue.";
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager::printTuple() should succeed.";
            // Here we check only the needed fields, some values are ignored (marked "x")
            checkPrintRecord(expectedString, stream.str(), true, {"table-id"});
        }
    };

    class RM_Version_Test : public RM_Tuple_Test {
    protected:
        std::string tableName = "rm_extra_test_table";

    public:
        void SetUp() override {

            if (!fileExists(tableName)) {

                // Try to delete the System Catalog.
                // If this is the first time, it will generate an error. It's OK and we will ignore that.
                rm.deleteCatalog();

                // Create Catalog
                ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

                // Create a table
                std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                        "CREATE TABLE " + tableName + " (emp_name VARCHAR(40), age INT, height REAL, salary REAL)");
                ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                            << "Create table " << tableName << " should succeed.";
                ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";
            }

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should not fail.";
            }
        }
    };

    // Function to prepare the data in the correct form to be inserted/read/updated
    void prepareTuple(const size_t &attributeCount, unsigned char *nullAttributesIndicator, const size_t nameLength,
                      const std::string &name, const unsigned age, const float height, const float salary, void *buffer,
                      size_t &tupleSize) {
        unsigned offset = 0;

        // Null-indicators
        bool nullBit;
        unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
        offset += nullAttributesIndicatorActualSize;

        // Beginning of the actual data
        // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
        // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

        // Is the name field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &nameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, name.c_str(), nameLength);
            offset += nameLength;
        }

        // Is the age field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &age, sizeof(int));
            offset += sizeof(int);
        }


        // Is the height field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &height, sizeof(float));
            offset += sizeof(float);
        }


        // Is the salary field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 4);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &salary, sizeof(float));
            offset += sizeof(int);
        }

        tupleSize = offset;
    }

    // Function to get the data in the correct form to be inserted/read after adding the attribute ssn
    void prepareTupleAfterAdd(int attributeCount, unsigned char *nullAttributesIndicator, const int nameLength,
                              const std::string &name, const unsigned age, const float height, const float salary,
                              const int ssn, void *buffer, size_t &tupleSize) {
        unsigned offset = 0;

        // Null-indicators
        bool nullBit;
        unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
        offset += nullAttributesIndicatorActualSize;

        // Beginning of the actual data
        // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
        // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

        // Is the name field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &nameLength, sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char *) buffer + offset, name.c_str(), nameLength);
            offset += nameLength;
        }

        // Is the age field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &age, sizeof(unsigned));
            offset += sizeof(unsigned);
        }

        // Is the height field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &height, sizeof(float));
            offset += sizeof(float);
        }

        // Is the salary field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 4);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &salary, sizeof(float));
            offset += sizeof(float);
        }

        // Is the ssn field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 3);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &ssn, sizeof(unsigned));
            offset += sizeof(unsigned);
        }

        tupleSize = offset;
    }

} // namespace PeterDBTesting
#endif // _test_util_h_