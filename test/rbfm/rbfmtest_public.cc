#include "src/include/rbfm.h"
#include "rbfm_test_utils.h"

namespace PeterDBTesting {

    TEST_F(RBFM_Test, insert_and_read_a_record) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Record
        // 4. Read Record
        // 5. Close Record-Based File
        // 6. Destroy Record-Based File

        PeterDB::RID rid;
        int recordSize = 0;
        inBuffer = malloc(100);
        outBuffer = malloc(100);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a inBuffer into a file and print the inBuffer
        prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, inBuffer, &recordSize);

        std::ostringstream stream;
        rbfm.printRecord(recordDescriptor, inBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: 25, Height: 177.8, Salary: 6200", stream.str()));

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a inBuffer should succeed.";

        // Given the rid, read the inBuffer from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a inBuffer should succeed.";

        stream.clear();
        rbfm.printRecord(recordDescriptor, outBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: 25, Height: 177.8, Salary: 6200", stream.str()));

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "the read data should match the inserted data";

    }

    TEST_F(RBFM_Test, insert_and_read_a_record_with_null) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Record - NULL
        // 4. Read Record
        // 5. Close Record-Based File
        // 6. Destroy Record-Based File

        PeterDB::RID rid;
        int recordSize = 0;
        inBuffer = malloc(100);
        outBuffer = malloc(100);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);

        // Initialize a NULL field indicator
        int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
        unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
        memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

        // Setting the age & salary fields value as null
        nullsIndicator[0] = 80; // 01010000

        // Insert a record into a file and print the record
        prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, inBuffer, &recordSize);

        std::ostringstream stream;
        rbfm.printRecord(recordDescriptor, inBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: NULL, Height: 177.8, Salary: NULL", stream.str()));

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        stream.clear();
        rbfm.printRecord(recordDescriptor, outBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: NULL, Height: 177.8, Salary: NULL", stream.str()));

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "the read data should match the inserted data";

    }

    TEST_F(RBFM_Test, insert_and_read_multiple_records) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Multiple Records
        // 4. Reopen Record-Based File
        // 5. Read Multiple Records
        // 6. Close Record-Based File
        // 7. Destroy Record-Based File

        PeterDB::RID rid;
        inBuffer = malloc(1000);
        int numRecords = 2000;

        // clean caches
        rids.clear();
        sizes.clear();

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor(recordDescriptor);

        for (PeterDB::Attribute &i : recordDescriptor) {
            GTEST_LOG_(INFO) << "Attr Name: " << i.name << " Attr Type: " << (PeterDB::AttrType) i.type
                             << " Attr Len: " << i.length;
        }

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert 2000 records into file
        for (int i = 0; i < numRecords; i++) {

            // Test insert Record
            int size = 0;
            memset(inBuffer, 0, 1000);
            prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a inBuffer should succeed.";

            // Leave rid and sizes for next test to examine
            rids.push_back(rid);
            sizes.push_back(size);
        }

        ASSERT_EQ(rids.size(), numRecords) << "Reading records should succeed.";
        ASSERT_EQ(sizes.size(), (unsigned) numRecords) << "Reading records should succeed.";

        outBuffer = malloc(1000);

        for (int i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 1000);
            memset(outBuffer, 0, 1000);
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";

            if (i % 1000 == 0) {
                std::ostringstream stream;
                rbfm.printRecord(recordDescriptor, outBuffer, stream);
                GTEST_LOG_(INFO) << "Returned Data: " << stream.str();
            }

            int size = 0;
            prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);
            ASSERT_EQ(memcmp(outBuffer, inBuffer, sizes[i]), 0) << "the read data should match the inserted data";
        }

    }

    TEST_F(RBFM_Test, insert_and_read_massive_records) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Massive Records
        // 4. Reopen Record-Based File
        // 5. Read Massive Records
        // 6. Close Record-Based File
        // 7. Destroy Record-Based File
        PeterDB::RID rid;
        inBuffer = malloc(1000);
        int numRecords = 10000;

        // clean caches
        rids.clear();
        sizes.clear();

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor(recordDescriptor);

        for (PeterDB::Attribute &i : recordDescriptor) {
            GTEST_LOG_(INFO) << "Attr Name: " << i.name << " Attr Type: " << (PeterDB::AttrType) i.type
                             << " Attr Len: " << i.length;
        }

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert 2000 records into file
        for (int i = 0; i < numRecords; i++) {

            // Test insert Record
            int size = 0;
            memset(inBuffer, 0, 1000);
            prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a inBuffer should succeed.";

            // Leave rid and sizes for next test to examine
            rids.push_back(rid);
            sizes.push_back(size);
        }

        ASSERT_EQ(rids.size(), numRecords) << "Reading records should succeed.";
        ASSERT_EQ(sizes.size(), (unsigned) numRecords) << "Reading records should succeed.";

        outBuffer = malloc(1000);

        for (int i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 1000);
            memset(outBuffer, 0, 1000);
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";

            if (i % 1000 == 0) {
                std::ostringstream stream;
                rbfm.printRecord(recordDescriptor, outBuffer, stream);
                GTEST_LOG_(INFO) << "Returned Data: " << stream.str();

            }

            int size = 0;
            prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);
            ASSERT_EQ(memcmp(outBuffer, inBuffer, sizes[i]),
                      0) << "the read data should match the inserted data";
        }
    }
}// namespace PeterDBTesting