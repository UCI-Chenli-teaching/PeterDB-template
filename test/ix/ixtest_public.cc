#include <random>

#include "src/include/ix.h"
#include "test/utils/ix_test_utils.h"

namespace PeterDBTesting {
    TEST_F(IX_File_Test, create_open_close_destory_index) {
        // Functions tested
        // 1. Create Index File
        // 2. Open Index File
        // 3. Create Index File -- when index file is already created
        // 4. Open Index File -- when a file handle is already opened
        // 5. Close Index File

        // create index file
        ASSERT_EQ(ix.createFile(indexFileName), success) << "indexManager::createFile() should succeed.";

        // open index file
        ASSERT_EQ(ix.openFile(indexFileName, ixFileHandle), success) << "indexManager::openFile() should succeed.";
        ASSERT_TRUE(fileExists(indexFileName)) << "The index file " << indexFileName << " should exist now.";

        // create duplicate index file
        ASSERT_NE(ix.createFile(indexFileName), success)
                                    << "indexManager::createFile() on an existing index file should not success.";
        EXPECT_TRUE(fileExists(indexFileName)) << "The index file " << indexFileName << " should exist now.";

        // open index file again using the file handle that is already opened.
        ASSERT_NE(ix.openFile(indexFileName, ixFileHandle), success)
                                    << "indexManager::openFile() using an already opened file handle should not succeed.";

        // close index file
        ASSERT_EQ(ix.closeFile(ixFileHandle), success) << "indexManager::closeFile() should succeed.";
        EXPECT_TRUE(fileExists(indexFileName)) << "The index file " << indexFileName << " should exist now.";

        // destroy index file
        ASSERT_EQ(ix.destroyFile(indexFileName), success) << "indexManager::destroyFile() should succeed.";
        EXPECT_FALSE(fileExists(indexFileName)) << "The index file " << indexFileName << " should not exist now.";

        // destroy index file again
        ASSERT_NE(ix.destroyFile(indexFileName), success)
                                    << "indexManager::destroyFile() on a non-existence index file should not succeed.";
        ASSERT_FALSE(fileExists(indexFileName)) << "The index file " << indexFileName << " should not exist now.";

    }

    TEST_F(IX_Test, insert_one_entry_and_print) {
        // Functions tested
        // 1. Insert one entry
        // 2. Disk I/O check of Insertion - CollectCounterValues
        // 3. print B+ Tree

        int key = 200;
        rid.pageNum = 500;
        rid.slotNum = 20;

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rc, wc, ac), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // insert entry
        ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                    << "indexManager::insertEntry() should succeed.";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rcAfter, wcAfter, acAfter), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // check counters
        EXPECT_IN_RANGE(rcAfter - rc, 0, 1); // could read the tree root pointer
        EXPECT_IN_RANGE(wcAfter - wc, 0,
                        2); // could write to both tree root pointer and first tree node, depends on the implementation
        EXPECT_EQ(acAfter - ac, 2); // one for tree root pointer, one for the first tree node

        EXPECT_GE(getFileSize(indexFileName) / PAGE_SIZE, 2) << "File size should get increased.";
        EXPECT_EQ(getFileSize(indexFileName) % PAGE_SIZE, 0) << "File should be based on PAGE_SIZE.";

        // print BTree, by this time the BTree should have only one node
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, ageAttr, stream), success)
                                    << "indexManager::printBTree() should succeed.";

        validateTree(stream, 1, 1, 0, PAGE_SIZE / 10 / 2, true);

    }

    TEST_F(IX_Test, insert_one_entry_and_scan) {
        // Functions tested
        // 1. Insert one entry
        // 2. Disk I/O check of Scan - NO_OP and getNextEntry
        // 3. CollectCounterValues

        int key = 123;
        rid.pageNum = 900;
        rid.slotNum = 75;

        // Insert one entry
        ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                    << "indexManager::insertEntry() should succeed.";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rc, wc, ac), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // Initialize a scan - Full scan, no condition.
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rcAfter, wcAfter, acAfter), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        EXPECT_GE(getFileSize(indexFileName) / PAGE_SIZE, 2) << "File size should get increased.";
        EXPECT_EQ(getFileSize(indexFileName) % PAGE_SIZE, 0) << "File should be based on PAGE_SIZE.";

        // There should be one entry
        // reset RID
        rid = PeterDB::RID{};
        int count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            ASSERT_EQ(rid.pageNum, 900) << "rid.pageNum is not correct.";
            ASSERT_EQ(rid.slotNum, 75) << "rid.slotNum is not correct.";
            count++;
        }
        ASSERT_EQ(count, 1) << "scan count is not correct.";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rcAfter, wcAfter, acAfter), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // check counters
        EXPECT_EQ(rcAfter - rc, 2); // one for tree root pointer, one for the first tree node
        EXPECT_IN_RANGE(wcAfter - wc, 0, 1); // persist counters
        EXPECT_EQ(acAfter - ac, 0); // no page appended during iteration.

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
    }

    TEST_F(IX_Test, insert_and_delete_one_entry) {
        // Functions tested
        // 1. Insert one entry
        // 2. Disk I/O check of deleteEntry - CollectCounterValues

        int key = 222;
        rid.pageNum = 440;
        rid.slotNum = 23;

        // Insert one entry
        ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                    << "indexManager::insertEntry() should succeed.";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rc, wc, ac), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // delete entry
        ASSERT_EQ(ix.deleteEntry(ixFileHandle, ageAttr, &key, rid), success)
                                    << "indexManager::deleteEntry() should succeed.";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rcAfter, wcAfter, acAfter), success)
                                    << "indexManager::collectCounterValues() should succeed.";


        // check counters
        EXPECT_EQ(rcAfter - rc, 2); // one for tree root pointer, and one for tree node
        EXPECT_IN_RANGE(wcAfter - wc, 1, 2); // write to update the first tree node, persist counters
        EXPECT_EQ(acAfter - ac, 0); // no pages appended when deleting an entry

        EXPECT_GE(getFileSize(indexFileName) / PAGE_SIZE, 2) << "File size should get increased.";
        EXPECT_EQ(getFileSize(indexFileName) % PAGE_SIZE, 0) << "File should be based on PAGE_SIZE.";

        // delete entry again
        ASSERT_NE(ix.deleteEntry(ixFileHandle, ageAttr, &key, rid), success)
                                    << "indexManager::deleteEntry() on a non-existent entry should not succeed.";

        // print BTree, by this time the BTree should have no node
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, ageAttr, stream), success)
                                    << "indexManager::printBTree() should succeed.";

        validateTree(stream, 0, 0, 0, PAGE_SIZE / 10 / 2, true);

        EXPECT_GE(getFileSize(indexFileName) / PAGE_SIZE, 2) << "File size should get increased.";
        EXPECT_EQ(getFileSize(indexFileName) % PAGE_SIZE, 0) << "File should be based on PAGE_SIZE.";

    }

    TEST_F(IX_Test, scan_on_destroyed_index) {
        // Functions tested
        // 1. Destroy Index File
        // 2. Open Index File -- should not succeed
        // 3. Scan  -- should not succeed

        destroyFile = false; // prevent double destroy in TearDown()

        // destroy index file
        ASSERT_EQ(ix.destroyFile(indexFileName), success) << "indexManager::destroyFile() should succeed.";
        EXPECT_FALSE(fileExists(indexFileName)) << "the index file " << indexFileName << " should not exist now.";

        // Try to open the destroyed index
        ASSERT_NE(ix.openFile(indexFileName, ixFileHandle), success)
                                    << "indexManager::openFile() on a destroyed file should not succeed.";

        // Try to initialize a scan on the destroyed index
        ASSERT_NE(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() on a destroyed file should not succeed.";

    }

    TEST_F(IX_Test, scan_by_NO_OP) {
        // Functions tested
        // 1. Insert multiple entries
        // 2. Scan entries NO_OP -- open
        // 3. Reopen index file
        // 4. Insert more entries
        // 5. Scan entries NO_OP -- open

        unsigned key;
        unsigned numOfEntries = 12345;
        unsigned numOfMoreEntries = 12345;
        unsigned seed = 12545;
        unsigned salt = 90;

        // insert entries
        generateAndInsertEntries(numOfEntries, ageAttr, seed, salt);

        // Scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // Fetch and check all entries
        std::vector<PeterDB::RID> ridsCopy(rids);
        int count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            validateUnorderedRID(key, count + seed, ridsCopy);
            count++;
            if (count % 5000 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }

        }

        EXPECT_EQ(count, numOfEntries) << "full scanned count should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

        ASSERT_EQ(getFileSize(indexFileName) % PAGE_SIZE, 0) << "File should be based on PAGE_SIZE.";

        // insert more entries
        seed = 200;
        salt = 567;
        generateAndInsertEntries(numOfMoreEntries, ageAttr, seed, salt);

        // Reopen the file
        reopenIndexFile();

        // Scan again
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() on a destroyed file should succeed.";


        // Fetch and check all entries
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            validateUnorderedRID(key, count + seed, this->rids);
            count++;
            if (count % 5000 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }

        }

        EXPECT_EQ(rids.size(), 0) << "all RIDs are scanned";
        EXPECT_EQ(count, numOfEntries + numOfMoreEntries) << "full scanned count should match inserted.";

        ASSERT_EQ(getFileSize(indexFileName) % PAGE_SIZE, 0) << "File should be based on PAGE_SIZE.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

        EXPECT_GE (getFileSize(indexFileName) / PAGE_SIZE, (numOfEntries + numOfMoreEntries) / PAGE_SIZE / 10)
                            << "page size should be increased.";

    }

    TEST_F(IX_Test, scan_by_GE_OP) {
        // Functions tested
        // 1. Insert entry
        // 2. Scan entries GE_OP

        unsigned numOfEntries = 800;
        unsigned numOfMoreEntries = 1500;
        unsigned key, seed = 10, salt = 14;
        unsigned value = 7001;

        // Insert entries
        generateAndInsertEntries(numOfEntries, ageAttr, seed, salt);

        // Insert more entries
        seed = value;
        generateAndInsertEntries(numOfMoreEntries, ageAttr, seed, salt);

        // Scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, &value, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // IndexScan iterator
        unsigned count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;
            validateRID(key, seed, salt);
            if (count % 500 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }

        }
        EXPECT_EQ(count, numOfMoreEntries) << "scanned count should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

        EXPECT_GE (getFileSize(indexFileName) / PAGE_SIZE, (numOfEntries + numOfMoreEntries) / PAGE_SIZE / 10)
                            << "page size should be increased.";

    }

    TEST_F(IX_Test, scan_by_LT_OP) {
        // Functions tested
        // 1. Insert entry
        // 2. Scan entries LT_OP


        unsigned numOfEntries = 2500;
        unsigned numOfMoreEntries = 8000;
        float key;
        float compVal = 6500.23;
        float seed = 49.51, salt = 124.1;

        // insert entries
        generateAndInsertEntries(numOfEntries, heightAttr, seed, salt);

        // insert more entries
        generateAndInsertEntries(numOfMoreEntries, heightAttr, compVal, salt);

        // Scan
        ASSERT_EQ(ix.scan(ixFileHandle, heightAttr, NULL, &compVal, true, false, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // Iterate
        unsigned count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;
            validateRID(key, seed, salt);
            if (count % 500 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }

        }

        EXPECT_EQ(count, numOfEntries) << "scanned count should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

        EXPECT_GE (getFileSize(indexFileName) / PAGE_SIZE, (numOfEntries + numOfMoreEntries) / PAGE_SIZE / 10)
                            << "page size should be increased.";

    }

    TEST_F(IX_Test, scan_by_EQ_OP) {
        // Functions tested
        // 1. Insert entries with two different keys
        // 2. Scan entries that match one of the keys

        unsigned key1 = 400;
        unsigned key2 = 100;
        unsigned numOfEntries = 5000;
        unsigned numOfMoreEntries = 10000;
        unsigned key, seed = 10090, salt = 5617;

        // Insert entries
        generateAndInsertEntries(numOfEntries, ageAttr, seed, salt, key1);

        // Insert more entries
        rids.clear();
        seed += 10;
        salt += 50;
        generateAndInsertEntries(numOfMoreEntries, ageAttr, seed, salt, key2);

        // Scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, &key2, &key2, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";


        // iterate
        unsigned count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            validateUnorderedRID(key, key2, this->rids);
            count++;
            if (count % 1000 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }

        }
        EXPECT_EQ(rids.size(), 0) << "all RIDs are scanned";
        EXPECT_EQ(count, numOfMoreEntries) << "scanned count should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Test, scan_on_reinserted_entries) {
        // Functions tested
        // 1. Insert large number of records
        // 2. Scan large number of records to validate insert correctly
        // 3. Delete some tuples
        // 4. Insert large number of records again
        // 5. Scan large number of records to validate insert correctly
        // 6. Delete all

        unsigned key;
        unsigned numOfEntries = 1000 * 1000;
        unsigned seed = 581078, salt = 21414;

        // Insert entries
        generateAndInsertEntries(numOfEntries, ageAttr, seed, salt);

        // Scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // Iterate
        unsigned count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;
            validateRID(key, seed, salt);
            if (count % 200000 == 0) {
                GTEST_LOG_(INFO) << count << " scanned. ";
            }
        }
        EXPECT_EQ(count, numOfEntries) << "scanned count should match inserted.";


        // Delete some tuples
        unsigned deletedRecordNum = 0;

        for (unsigned i = 5; i <= numOfEntries; i += 10) {
            key = i + seed;
            rid.pageNum = (unsigned) (key * salt + seed) % INT_MAX;
            rid.slotNum = (unsigned) (key * salt * seed + seed) % SHRT_MAX;

            ASSERT_EQ(ix.deleteEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::deleteEntry() should succeed.";

            deletedRecordNum += 1;
            if (deletedRecordNum % 20000 == 0) {
                GTEST_LOG_(INFO) << deletedRecordNum << " deleted. ";
            }
        }

        // Close Scan and reinitialize the scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // Iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;
            validateRID(key, seed, salt);
            if (count % 200000 == 0) {
                GTEST_LOG_(INFO) << count << " scanned. ";
            }

        }
        EXPECT_EQ(count, numOfEntries - deletedRecordNum) << "scanned count should match inserted.";


        // Insert the deleted entries again
        int reInsertedRecordNum = 0;
        for (unsigned i = 5; i <= numOfEntries; i += 10) {
            key = i + seed;
            rid.pageNum = (unsigned) (key * salt + seed) % INT_MAX;
            rid.slotNum = (unsigned) (key * salt * seed + seed) % SHRT_MAX;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            reInsertedRecordNum += 1;
            if (reInsertedRecordNum % 20000 == 0) {
                GTEST_LOG_(INFO) << reInsertedRecordNum << " inserted - rid: " << rid.pageNum << " " << rid.slotNum;
            }
        }

        // Close Scan and reinitialize the scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // Iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;
            validateRID(key, seed, salt);

            if (count % 200000 == 0) {
                GTEST_LOG_(INFO) << count << " scanned. ";
            }

        }

        EXPECT_EQ(count, numOfEntries) << "scanned count should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

        EXPECT_GE (getFileSize(indexFileName) / PAGE_SIZE, numOfEntries / PAGE_SIZE / 10)
                            << "page size should be increased.";

    }

    TEST_F(IX_Test, scan_to_delete_entries) {
        // Checks whether deleting an entry after getNextEntry() in a scan is handled properly or not.
        //    An example:
        //    IX_ScanIterator ix_ScanIterator;
        //    indexManager.scan(ixFileHandle, ..., ix_ScanIterator);
        //    while ((rc = ix_ScanIterator.getNextEntry(rid, &key)) != IX_EOF)
        //    {
        //       indexManager.deleteEntry(ixFileHandle, attribute, &key, rid);
        //    }

        // Functions tested
        // 1. Insert entry
        // 2. Scan entries - NO_OP, and delete entries


        unsigned numOfEntries = 160000;
        float key;
        float seed = 495.1, salt = 21.89;

        // insert entries
        generateAndInsertEntries(numOfEntries / 4, heightAttr, seed, salt);
        generateAndInsertEntries(numOfEntries / 4, heightAttr, seed + 1267, salt - 414);
        generateAndInsertEntries(numOfEntries / 4, heightAttr, seed - 5, salt + 523);
        generateAndInsertEntries(numOfEntries / 4, heightAttr, seed + 14, salt - 413);

        EXPECT_GE (getFileSize(indexFileName) / PAGE_SIZE, numOfEntries / PAGE_SIZE / 10)
                            << "page size should be increased.";

        // Scan - NO_OP
        ASSERT_EQ(ix.scan(ixFileHandle, heightAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // Delete entries in IndexScan Iterator
        unsigned count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;

            if (count % 5000 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }
            ASSERT_EQ(ix.deleteEntry(ixFileHandle, heightAttr, &key, rid), success)
                                        << "indexManager::deleteEntry() should succeed.";
        }

        EXPECT_EQ(count, numOfEntries) << "scanned count should match inserted.";


        // Close Scan and reinitialize the scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle, heightAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // iterate - should hit EOF since there are no entries
        ASSERT_EQ(ix_ScanIterator.getNextEntry(rid, &key), IX_EOF) << "there should be no returned entries.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Test, scan_varchar_with_compact_size) {
        // Checks whether VARCHAR type is handled properly or not.
        //
        // Functions Tested:
        // 1. Insert Entry
        // 2. Get Insert IO count
        // 3. Scan
        // 4. Get Scan IO count
        // 5. Close Scan

        unsigned numOfEntries = 500;
        unsigned numOfMoreEntries = 5;
        char key[1004];
        char testedAscii = 107 - 96;

        // insert entries
        for (unsigned i = 0; i < numOfEntries; i++) {
            memset(key, 0, 1004);
            prepareKeyAndRid(i, key, rid);

            ASSERT_EQ(ix.insertEntry(ixFileHandle, empNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            if (i % 100 == testedAscii) {
                rids.emplace_back(rid);
            }
        }
        // insert more entries

        for (unsigned i = 0; i < numOfMoreEntries; i++) {
            memset(key, 0, 1004);
            prepareKeyAndRid(testedAscii, key, rid);
            rid.slotNum = rid.pageNum + 1;
            ASSERT_EQ(ix.insertEntry(ixFileHandle, empNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            rids.emplace_back(rid);
        }

        // print BTree, by this time the BTree should have only one node
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                    << "indexManager::printBTree() should succeed.";

        // we give D a very large
        // (1+n)n/2 <= PAGE_SIZE, thus n >= 2^6.5 =90.5, we would put very loose D as around 45.
        validateTree(stream, numOfEntries, numOfEntries + numOfMoreEntries, 2,
                     45, true);

        // collect counter
        ASSERT_EQ(ixFileHandle.collectCounterValues(rc, wc, ac), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // Scan
        memset(key, 0, 100);
        prepareKeyAndRid(testedAscii, key, rid);
        ASSERT_EQ(ix.scan(ixFileHandle, empNameAttr, &key, &key, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        //iterate
        int count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {

            auto target = std::find_if(rids.begin(), rids.end(), [&](const PeterDB::RID &r) {
                return r.slotNum == rid.slotNum && r.pageNum == rid.pageNum;
            });
            EXPECT_NE(target, rids.end()) << "RID is not from inserted.";
            rids.erase(target);
            count++;
            if (count % 20 == 0) {
                GTEST_LOG_(INFO) << count << " scanned - returned rid: " << rid.pageNum << " " << rid.slotNum;
            }
        }

        EXPECT_EQ(rids.size(), 0) << "all RIDs are scanned";

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rcAfter, wcAfter, acAfter), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        // check counters
        EXPECT_GE(rcAfter - rc, 4);
        // for scan and iteration, at least 4 pages needed (1 tree root pointer, 3 tree nodes)
        EXPECT_IN_RANGE(wcAfter - wc, 0, 1); // persist counters
        EXPECT_EQ(acAfter - ac, 0); // no page appended during iteration.

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Test, split_rotate_and_promote_on_insertion) {
        // Checks whether the insertion is implemented correctly (split should happen)
        // Functions tested
        // 1. Insert entries to make root full
        // 2. Print BTree
        // 3. Insert one more entries to watch the shape of the BTree


        unsigned numOfEntries = 21;
        char key[PAGE_SIZE];
        empNameAttr.length = PAGE_SIZE / 5; // each node can only occupy 4 keys

        std::vector<unsigned> keys(numOfEntries);
        std::iota(keys.begin(), keys.end(), 1);
        std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));

        // insert entry
        unsigned i = 0;
        for (unsigned &k:keys) {
            i++;
            // Prepare a key
            prepareKeyAndRid(k, key, rid, empNameAttr.length);
            ASSERT_EQ(ix.insertEntry(ixFileHandle, empNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
            if (i == 5) {
                // print BTree, by this time the BTree should have height of 1 - one root (c*) with two leaf nodes
                // (2 + 3) or (3 + 2)
                std::stringstream stream;
                ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                            << "indexManager::printBTree() should succeed";

                validateTree(stream, 5, 5, 1, 2);
            }

        }

        // print BTree, by this time the BTree should have height of 2
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                    << "indexManager::printBTree() should succeed.";
        validateTree(stream, numOfEntries, numOfEntries, 2, 2);

    }

    TEST_F(IX_Test, duplicate_keys_in_one_page) {
        // Checks whether duplicated entries in a page are handled properly.
        //
        // Functions tested
        // 1. Insert entries with the same key
        // 2. Print BTree

        int key = 300;
        unsigned numOfEntries = 200;


        // insert entries
        for (unsigned i = 0; i < numOfEntries; i++) {
            rid.pageNum = numOfEntries + i + 1;
            rid.slotNum = i + 2;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        // Actually, this should print out only one page.
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, ageAttr, stream), success)
                                    << "indexManager::printBTree() should succeed";

        // no matter which implementation, print should give a key:[RID1,RID2...] structure on leaf nodes to be checked.
        validateTree(stream, 1, numOfEntries, 0, PAGE_SIZE / 10 / 2, true);

    }

    TEST_F(IX_Test, extra_duplicate_keys_span_multiple_pages) {
        // Checks whether duplicated entries spanning multiple page are handled properly or not.
        //
        // Functions tested
        // 1. Insert entry
        // 2. Scan entries - EQ_OP.
        // 3. Scan close

        unsigned numOfEntries = 60000;
        unsigned key;
        unsigned compVal1 = 1234, compVal2 = 4321;
        unsigned count;

        std::vector<unsigned short> entries(numOfEntries);
        std::iota(entries.begin(), entries.end(), 1);
        std::shuffle(entries.begin(), entries.end(), std::mt19937(std::random_device()()));

        // insert entry
        for (unsigned short i: entries) {
            rid.pageNum = i;
            rid.slotNum = i;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &compVal1, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            rid.pageNum = i + 10;
            rid.slotNum = i;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &compVal2, rid), success);
        }

        // Scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, &compVal1, &compVal1, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;

            EXPECT_EQ(rid.pageNum, rid.slotNum) << "scanned count should match inserted.";
            EXPECT_EQ(key, compVal1) << "scanned key should match inserted.";

            if (count % 2000 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }
        }

        EXPECT_EQ(count, numOfEntries) << "scanned count should match inserted.";

        // Close and reinitialize Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, &compVal2, &compVal2, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;

            EXPECT_EQ(rid.pageNum, rid.slotNum + 10) << "scanned count should match inserted.";
            EXPECT_EQ(key, compVal2) << "scanned key should match inserted.";

            if (count % 2000 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }
        }

        EXPECT_EQ(count, numOfEntries) << "scanned count should match inserted.";


        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Test, extra_merge_on_deletion) {
        // Checks whether the deletion is properly managed (non-lazy deletion)
        // Functions tested
        // 1. Insert entries to make a height 2 tree
        // 2. Print BTree
        // 3. Delete the "unsafe one"
        // 4. Print BTree

        unsigned numOfEntries = 13;
        char key[PAGE_SIZE];

        empNameAttr.length = PAGE_SIZE / 5;  // Each node could only have 4 children

        // insert entries
        unsigned i = 1;
        for (; i <= numOfEntries; i++) {
            prepareKeyAndRid(i, key, rid, empNameAttr.length);

            ASSERT_EQ(ix.insertEntry(ixFileHandle, empNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        // print BTree, by this time the BTree should have height of 2
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                    << "indexManager::printBTree() should succeed";

        validateTree(stream, 13, 13, 2, 2);

        // delete the 2nd entry
        prepareKeyAndRid(2, key, rid, empNameAttr.length);
        ASSERT_EQ(ix.deleteEntry(ixFileHandle, empNameAttr, key, rid), success)
                                    << "indexManager::deleteEntry() should succeed.";

        // print BTree, by this time the BTree should have height of 1
        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                    << "indexManager::printBTree() should succeed";

        validateTree(stream, 12, 12, 1, 2);

    }

} // namespace PeterDBTesting