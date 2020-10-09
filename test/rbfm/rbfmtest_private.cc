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

        auto evaluateFunc = [&](){
            rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid);
        };

        auto metricFunc = [&](){
            unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
            fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
            return readPageCount;
        };
        auto expectedN = [&](){
            return fileHandle.getNumberOfPages();
        };

        // Check if insertRecord executes in O(n) page read operations
        ASSERT_EQ(checkTimeComplexity(evaluateFunc,expectedN,metricFunc),true);

    }
}

