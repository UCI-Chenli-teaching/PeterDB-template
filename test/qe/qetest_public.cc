#include "test/utils/qe_test_util.h"

namespace PeterDBTesting {
    TEST_F(QE_Test, create_and_delete_table_with_index) {
        // Tables created: left
        // Indexes created: left.B, left.C
        // 1. Create an Index
        // 2. Load Data
        // 3. Create an Index
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "left";
        tableNames.emplace_back(tableName);

        // Create a table
        ASSERT_EQ(rm.createTable(tableName, attrsMap[tableName]), success)
                                    << "Create table " << tableName << " should succeed.";

        // Create an index before inserting tuples.
        ASSERT_EQ(rm.createIndex(tableName, "B"), success) << "RelationManager.createIndex() should succeed.";
        ASSERT_EQ(glob(".idx").size(), 1) << "There should be two index files now.";

        // Insert tuples.
        populateTable(tableName, 100);

        // Create an index after inserting tuples - should reflect the currently existing tuples.
        ASSERT_EQ(rm.createIndex(tableName, "C"), success) << "RelationManager.createIndex() should succeed.";
        ASSERT_EQ(glob(".idx").size(), 2) << "There should be two index files now.";

        destroyFile = false; // prevent from double deletion

        // Destroy the file
        ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should succeed.";
        ASSERT_FALSE(fileExists(tableName)) << "The file " << tableName << " should not exist now.";
        ASSERT_EQ(glob(".idx").size(), 0) << "There should be no index file now.";

        // Delete Catalog
        ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";

    }

    TEST_F(QE_Test, table_scan_with_int_filter) {

        // Filter -- TableScan as input, on an Integer Attribute
        // SELECT * FROM LEFT WHERE B <= 51
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "left";
        createAndPopulateTable(tableName, {"A", "B", "C"}, 1000);

        PeterDB::TableScan ts(rm, tableName);

        // Set up condition
        unsigned compVal = 51;
        PeterDB::Condition cond{"left.B", PeterDB::LE_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;

        // Create Filter
        PeterDB::Filter filter(&ts, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 1000; i++) {
            unsigned a = i % 203;
            unsigned b = (i + 10) % 197;
            float c = (float) (i % 167) + 50.5f;
            if (b <= 51)
                expected.emplace_back(
                        "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b) + ", left.C: " +
                        std::to_string(c));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 100 == 0);
        }

    }

    TEST_F(QE_Test, table_scan_with_varchar_filter) {
        // Mandatory for all
        // 1. Filter -- on TypeVarChar Attribute
        // SELECT * FROM leftvarchar where B = "llllllllllll"

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "leftvarchar";
        createAndPopulateTable(tableName, {"B", "A"}, 1000);

        // Set up TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Set up condition
        PeterDB::Condition cond{"leftvarchar.B", PeterDB::EQ_OP, false, "", {PeterDB::TypeVarChar, inBuffer}};
        unsigned length = 12;
        *(unsigned *) ((char *) cond.rhsValue.data) = length;
        // "llllllllllll"
        for (unsigned i = 0; i < length; ++i) {
            *(char *) ((char *) cond.rhsValue.data + sizeof(unsigned) + i) = 12 + 96;
        }

        // Create Filter
        PeterDB::Filter filter(&ts, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 1000; i++) {
            unsigned a = i + 20;
            unsigned len = (i % 26) + 1;
            std::string b = std::string(len, '\0');
            for (int j = 0; j < len; j++) {
                b[j] = 96 + len;
            }
            if (len == 12) {
                expected.emplace_back("leftvarchar.A: " + std::to_string(a) + ", leftvarchar.B: " + b);
            }
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 10 == 0);
        }
    }

    TEST_F(QE_Test, index_scan_with_real_filter) {
        // 1. Filter -- IndexScan as input, on TypeReal attribute
        // SELECT * FROM RIGHT WHERE C >= 110.0

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "right";
        createAndPopulateTable(tableName, {"D", "C", "B"}, 1000);

        // Set up IndexScan
        PeterDB::IndexScan is(rm, tableName, "C");

        // Set up condition
        float compVal = 110.0;
        PeterDB::Condition cond{"right.C", PeterDB::GE_OP, false, "", {PeterDB::TypeReal, inBuffer}};
        *(float *) cond.rhsValue.data = compVal;

        // Create Filter
        PeterDB::Filter filter(&is, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 1000; i++) {
            unsigned b = i % 251 + 20;
            float c = (float) (i % 261) + 25.5f;
            unsigned d = i % 179;
            if (c >= 110) {
                expected.emplace_back(
                        "right.B: " + std::to_string(b) + ", right.C: " + std::to_string(c) + ", right.D: " +
                        std::to_string(d));
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 100 == 0);
        }

    }

    TEST_F(QE_Test, table_scan_with_project) {
        // Project -- TableScan as input
        // SELECT C,D FROM RIGHT

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "right";
        createAndPopulateTable(tableName, {}, 1000);

        // Set up TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Projector
        PeterDB::Project project(&ts, {"right.D", "right.C"});

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(project.getAttributes(attrs), success) << "Project.getAttributes() should succeed.";
        while (project.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        expected.reserve(1000);
        for (int i = 0; i < 1000; i++) {
            float c = (float) (i % 261) + 25.5f;
            unsigned d = i % 179;

            expected.emplace_back("right.D: " + std::to_string(d) + ", right.C: " + std::to_string(c));

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 100 == 0);
        }

    }

    TEST_F(QE_Test, bnljoin) {
        // 1. BNLJoin -- on TypeInt Attribute
        // SELECT * FROM left, right where left.B = right.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "A", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"D", "B", "C"}, 100);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, leftTableName);
        PeterDB::TableScan rightIn(rm, rightTableName);
        PeterDB::Condition cond{"left.B", PeterDB::EQ_OP, true, "right.B"};

        // Create BNLJoin
        PeterDB::BNLJoin bnlJoin(&leftIn, &rightIn, cond, 5);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(bnlJoin.getAttributes(attrs), success) << "BNLJoin.getAttributes() should succeed.";
        while (bnlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b1 == b2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i]);
        }

    }

    TEST_F(QE_Test, bnljoin_with_filter) {
        // Functions Tested
        // 1. BNLJoin -- on TypeInt Attribute
        // 2. Filter -- on TypeInt Attribute
        // SELECT * FROM left, right WHERE left.B = right.B AND right.B >= 100

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, 100);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");

        // Create BNLJoin
        PeterDB::BNLJoin bnlJoin(&leftIn, &rightIn, {"left.B", PeterDB::EQ_OP, true, "right.B"}, 5);

        int compVal = 100;

        // Create Filter
        PeterDB::Condition cond{"right.B", PeterDB::GE_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;
        PeterDB::Filter filter(&bnlJoin, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b2 >= 100 && b1 == b2) {

                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i]);
        }
    }

    TEST_F(QE_Test, inljoin) {
        // 1. INLJoin -- on TypeReal Attribute
        // SELECT * from left, right WHERE left.C = right.C

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, 100);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::IndexScan rightIn(rm, "right", "C");
        PeterDB::Condition cond{"left.C", PeterDB::EQ_OP, true, "right.C"};

        // Create INLJoin
        PeterDB::INLJoin inlJoin(&leftIn, &rightIn, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(inlJoin.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (inlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (c1 == c2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 10 == 0);
        }
    }

    TEST_F(QE_Test, inljoin_with_filter_and_project) {
        // 1. Filter
        // 2. Project
        // 3. INLJoin
        // SELECT A1.A, A1.C, right.* FROM (SELECT * FROM left WHERE left.B < 75) A1, right WHERE A1.C = right.C

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, 100);

        // Create Filter
        PeterDB::IndexScan leftIn(rm, leftTableName, "B");

        int compVal = 75;
        PeterDB::Condition cond{"left.B", PeterDB::LT_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;

        leftIn.setIterator(NULL, cond.rhsValue.data, true, false);
        PeterDB::Filter filter(&leftIn, cond);

        // Create Project
        PeterDB::Project project(&filter, {"left.A", "left.C"});

        // Create Join
        PeterDB::IndexScan rightIn(rm, rightTableName, "C");
        PeterDB::INLJoin inlJoin(&project, &rightIn, {"left.C", PeterDB::EQ_OP, true, "right.C"});

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(inlJoin.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (inlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b1 < 75 && c1 == c2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.C: " + std::to_string(c1) + ", right.B: " +
                            std::to_string(b2) + ", right.C: " + std::to_string(c2) + ", right.D: " +
                            std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 10 == 0);
        }

    }

    TEST_F(QE_Test, table_scan_with_max_aggregation) {
        // 1. Basic aggregation - max
        // SELECT max(left.B) from left

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "left";
        createAndPopulateTable(tableName, {"B", "C"}, 3000);

        // Create TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Aggregate
        PeterDB::Aggregate agg(&ts, {"left.B", PeterDB::TypeInt, 4}, PeterDB::MAX);

        ASSERT_EQ(agg.getAttributes(attrs), success) << "Aggregate.getAttributes() should succeed.";
        ASSERT_NE(agg.getNextTuple(outBuffer), QE_EOF) << "Aggregate.getNextTuple() should succeed.";
        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager.printTuple() should succeed.";
        checkPrintRecord("MAX(left.B): 196", stream.str());
        ASSERT_EQ(agg.getNextTuple(outBuffer), QE_EOF) << "Only 1 tuple should be returned for MAX.";

    }

    TEST_F(QE_Test, index_scan_with_avg_aggregation) {
        // 1. Basic aggregation - AVG
        // SELECT AVG(right.B) from left

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "right";
        createAndPopulateTable(tableName, {"B", "C"}, 3000);

        // Create IndexScan
        PeterDB::IndexScan is(rm, tableName, "B");

        // Create Aggregate
        PeterDB::Aggregate agg(&is, {"right.B", PeterDB::TypeInt, 4}, PeterDB::AVG);

        ASSERT_EQ(agg.getAttributes(attrs), success) << "Aggregate.getAttributes() should succeed.";
        ASSERT_NE(agg.getNextTuple(outBuffer), QE_EOF) << "Aggregate.getNextTuple() should succeed.";
        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager.printTuple() should succeed.";
        checkPrintRecord("AVG(right.B): 144.522", stream.str());
        ASSERT_EQ(agg.getNextTuple(outBuffer), QE_EOF) << "Only 1 tuple should be returned for AVG.";

    }

    TEST_F(QE_Test, ghjoin_on_int) {
        // Extra credit
        // 1. GHJoin -- on TypeInt Attribute
        // SELECT * from left, right WHERE left.B = right.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        unsigned numPartitions = 10;

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, 10000);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {}, 10000);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");

        PeterDB::Condition cond{"left.B", PeterDB::EQ_OP, true, "right.B"};

        int numFiles = glob("").size();
        // Create GHJoin - on heap so we can control its life cycle
        auto *ghJoin = new PeterDB::GHJoin(&leftIn, &rightIn, cond, numPartitions);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(ghJoin->getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (ghJoin->getNextTuple(outBuffer) != QE_EOF) {

            ASSERT_GE(glob("").size(), numFiles + 20) << "There should be at least 20 partition files created.";

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 10000; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 10000; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b1 == b2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 50000 == 0);
        }

        delete ghJoin;
        ASSERT_EQ(glob("").size(), numFiles) << "GHJoin should clean after itself.";
    }

    TEST_F(QE_Test, ghjoin_on_real) {
        // Extra credit
        // 1. GHJoin -- on TypeReal Attribute
        // SELECT * from left, right WHERE left.C = right.C

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        unsigned numPartitions = 7;

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, 10000);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {}, 10000);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");
        PeterDB::Condition cond{"left.C", PeterDB::EQ_OP, true, "right.C"};

        int numFiles = glob("").size();
        // Create GHJoin - on heap so we can control its life cycle
        auto *ghJoin = new PeterDB::GHJoin(&leftIn, &rightIn, cond, numPartitions);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(ghJoin->getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (ghJoin->getNextTuple(outBuffer) != QE_EOF) {

            ASSERT_GE(glob("").size(), numFiles + 14) << "There should be at least 20 partition files created.";

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 10000; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 10000; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (c1 == c2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " + std::to_string(c2)
                            + ", right.D: " + std::to_string(d));
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 50000 == 0);
        }

        delete ghJoin;
        ASSERT_EQ(glob("").size(), numFiles) << "GHJoin should clean after itself.";
    }

    TEST_F(QE_Test, table_scan_with_group_min_aggregation) {
        // Extra credit
        // Aggregate -- MIN (with GroupBy)
        // SELECT group.B, MIN(group.A) FROM group GROUP BY group.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "group";
        createAndPopulateTable(tableName, {}, 10000);

        // Create TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Aggregate
        PeterDB::Aggregate agg(&ts, {"group.A", PeterDB::TypeInt, 4}, {"group.B", PeterDB::TypeInt, 4}, PeterDB::MIN);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(agg.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (agg.getNextTuple(outBuffer) != QE_EOF) {

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 1; i < 6; i++) {
            expected.emplace_back("group.B: " + std::to_string(i) + ", MIN(group.A): " + std::to_string(i));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, true);
        }

    }

    TEST_F(QE_Test, table_scan_with_group_sum_aggregation) {
        // Extra credit
        // Aggregate -- SUM (with GroupBy)
        // SELECT group.B, SUM(group.A) FROM group GROUP BY group.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "group";
        createAndPopulateTable(tableName, {}, 10000);

        // Create TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Aggregate
        PeterDB::Aggregate agg(&ts, {"group.A", PeterDB::TypeInt, 4}, {"group.B", PeterDB::TypeInt, 4}, PeterDB::SUM);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(agg.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (agg.getNextTuple(outBuffer) != QE_EOF) {

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 1; i < 6; i++) {
            expected.emplace_back("group.B: " + std::to_string(i) + ", SUM(group.A): " + std::to_string(i * 2000));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, true);
        }

    }

} // namespace PeterDBTesting