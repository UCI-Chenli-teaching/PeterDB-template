#include "src/include/pfm.h"
#include "pfm_test_utils.h"

namespace PeterDBTesting {

    TEST_F (PFM_File_Test, test_all_file_operations) {
        // Functions Tested:
        // 1. Create File
        // 2. Open File
        // 3. Close File
        // 4. Destroy File

        // Create a file
        ASSERT_EQ(pfm.createFile(fileName), success) << "Creating the file should succeed: " << fileName;
        ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";

        // Open the file
        PeterDB::FileHandle fileHandle;
        ASSERT_EQ(pfm.openFile(fileName, fileHandle), success)
                                    << "Opening the file should succeed: " << fileName;

        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";

        // Close the file
        ASSERT_EQ(pfm.closeFile(fileHandle), success) << "Closing the file should succeed.";

        ASSERT_EQ(pfm.destroyFile(fileName), success) << "Destroying the file should success: " << fileName;
        ASSERT_FALSE(fileExists(fileName)) << "The file should not exist now: " << fileName;

        // Attempt to destroy the file again, should not succeed
        ASSERT_NE(pfm.destroyFile(fileName), success) << "Destroying the same file should not succeed: " << fileName;
        ASSERT_FALSE(fileExists(fileName)) << "The file should not exist now: " << fileName;

    }


    TEST_F (PFM_Page_Test, check_page_num_after_appending) {
        // Test case procedure:
        // 1. Append 39 Pages
        // 2. Check Page Number after each append
        // 3. Keep the file for the next test case

        size_t fileSizeBeforeAppend = getFileSize(fileName);
        inBuffer = malloc(PAGE_SIZE);
        outBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        for (int i = 0; i < numPages; ++i) {
            generateData(inBuffer, PAGE_SIZE, 53 + i, 47 + i);
            ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
            ASSERT_GT(getFileSize(fileName), fileSizeBeforeAppend) << "File size should have been increased";
            ASSERT_EQ(fileHandle.getNumberOfPages(), i + 1)
                                        << "The page count should be " << i + 1 << " at this moment";
        }
        destroyFile = false;

    }

    TEST_F (PFM_Page_Test, check_page_num_after_writing) {
        // Test case procedure:
        // 1. Overwrite the 39 Pages from the previous test case
        // 2. Check Page Number after each write
        // 3. Keep the file for the next test case

        inBuffer = malloc(PAGE_SIZE);
        outBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        size_t fileSizeAfterAppend = getFileSize(fileName);
        for (int i = 0; i < numPages; ++i) {
            generateData(inBuffer, PAGE_SIZE, 47 + i, 53 + i);
            ASSERT_EQ(fileHandle.writePage(i, inBuffer), success) << "Writing a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
            ASSERT_EQ(getFileSize(fileName), fileSizeAfterAppend) << "File size should not have been increased";
            ASSERT_EQ(fileHandle.getNumberOfPages(), numPages) << "The page count should be 10 at this moment";
        }
        destroyFile = false;
    }

    TEST_F (PFM_Page_Test, check_page_num_after_reading) {
        // Test case procedure:
        // 1. Read the 39 Pages from the previous test case
        // 2. Check Page Number after each write

        inBuffer = malloc(PAGE_SIZE);
        outBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        size_t fileSizeAfterAppend = getFileSize(fileName);
        for (int i = 0; i < numPages; ++i) {
            generateData(inBuffer, PAGE_SIZE, 47 + i, 53 + i);
            ASSERT_EQ(fileHandle.readPage(i, outBuffer), success) << "Reading a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
            ASSERT_EQ(fileHandle.getNumberOfPages(), numPages) << "The page count should be 10 at this moment";
            ASSERT_EQ(getFileSize(fileName), fileSizeAfterAppend) << "File size should not have been increased";
            ASSERT_EQ(memcmp(inBuffer, outBuffer, PAGE_SIZE), 0)
                                        << "Checking the integrity of the page should succeed.";
        }
    }


}
