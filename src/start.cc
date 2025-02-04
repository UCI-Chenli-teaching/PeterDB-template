#include <string>

#include "include/rm_utils.h"
#include "src/include/cli.h"

using namespace std;
using namespace PeterDB;

bool DEMO = false;

PeterDB::CLI* cli;

void exec(const string& command)
{
    cout << ">>> " << command << endl;
    cli->process(command);
}

// A small helper to build the "data" buffer for (id, name, age)
// in the format required by rbfm (null-indicators + fields).
void preparePeopleRecord(int idVal, const std::string &nameVal, int ageVal, void *buffer) {
    // We have 3 fields => null-indicator size = ceil(3/8)=1 byte
    // Suppose none of them is null => null-indicator = 0x00
    unsigned char nullByte = 0;
    memcpy(buffer, &nullByte, 1);

    int offset = 1;

    // 1) id (int)
    memcpy((char*)buffer + offset, &idVal, sizeof(int));
    offset += sizeof(int);

    // 2) name (varChar => [4-byte length][characters...])
    int nameLen = (int)nameVal.size();
    memcpy((char*)buffer + offset, &nameLen, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)buffer + offset, nameVal.c_str(), nameLen);
    offset += nameLen;

    // 3) age (int)
    memcpy((char*)buffer + offset, &ageVal, sizeof(int));
    offset += sizeof(int);

    // That’s it. The total record size is offset bytes,
    // but "insertTuple" doesn’t need that size explicitly.
}

void testCreateInsertRead() {
    // 1) Build a descriptor for "People": (id INT, name VARCHAR(50), age INT)
    vector<Attribute> peopleDescriptor;
    {
        Attribute attr;
        attr.name = "id";
        attr.type = TypeInt;
        attr.length = 4;
        peopleDescriptor.push_back(attr);

        attr.name = "name";
        attr.type = TypeVarChar;
        attr.length = 50; // max length
        peopleDescriptor.push_back(attr);

        attr.name = "age";
        attr.type = TypeInt;
        attr.length = 4;
        peopleDescriptor.push_back(attr);
    }

    // 2) Create the table via RelationManager
    RelationManager &rm = RelationManager::instance();

    RC rc = rm.createTable("People", peopleDescriptor);
    if (rc != 0) {
        cout << "[TEST] createTable(\"People\") failed. rc=" << rc << endl;
        return;
    }
    cout << "[TEST] createTable(\"People\") success." << endl;

    // 3) Insert two tuples
    // Prepare the data in the same layout RBFM expects (null-indicators + fields).
    char data1[200];
    char data2[200];
    memset(data1, 0, 200);
    memset(data2, 0, 200);

    preparePeopleRecord(1, "Alice", 21, data1);
    preparePeopleRecord(2, "Bob",   32, data2);

    RID rid1, rid2;
    rc = rm.insertTuple("People", data1, rid1);
    if (rc != 0) {
        cout << "[TEST] insertTuple #1 failed. rc=" << rc << endl;
    } else {
        cout << "[TEST] insertTuple #1 success => RID(" << rid1.pageNum << "," << rid1.slotNum << ")" << endl;
    }

    rc = rm.insertTuple("People", data2, rid2);
    if (rc != 0) {
        cout << "[TEST] insertTuple #2 failed. rc=" << rc << endl;
    } else {
        cout << "[TEST] insertTuple #2 success => RID(" << rid2.pageNum << "," << rid2.slotNum << ")" << endl;
    }

    // 4) Read the two tuples back and print them
    // We'll use the same "peopleDescriptor" we used to create the table,
    // but you can also fetch it by scanning "Columns" if you want to test that flow.
    char returnedData[200];

    // read #1
    rc = rm.readTuple("People", rid1, returnedData);
    if (rc != 0) {
        cout << "[TEST] readTuple #1 failed. rc=" << rc << endl;
    } else {
        cout << "[TEST] readTuple #1 success => " << endl;
        RecordBasedFileManager::instance().printRecord(peopleDescriptor, returnedData, cout);
        cout << endl;
    }

    // read #2
    rc = rm.readTuple("People", rid2, returnedData);
    if (rc != 0) {
        cout << "[TEST] readTuple #2 failed. rc=" << rc << endl;
    } else {
        cout << "[TEST] readTuple #2 success => " << endl;
        RecordBasedFileManager::instance().printRecord(peopleDescriptor, returnedData, cout);
        cout << endl;
    }

    // Optionally, you can do more checks, like scanning "People" with conditions, etc.
}

int main()
{
    cli = PeterDB::CLI::Instance();
    if (DEMO)
    {
        exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");
        exec("create table ages Age = int, Explanation = varchar(50)");
        exec("create table salary Salary = int, Explanation = varchar(50)");
        exec("create table company CompName = varchar(50), Age = int");

        exec("load employee employee_5");
        exec("load ages ages_90");
        exec("load salary salary_5");
        exec("load company company_7");

        exec("create index Age on employee");
        exec("create index Age on ages");
        exec("create index Salary on employee");
        exec("create index Salary on salary");
        exec("create index Age on company");
    }

    /* testing */
    RelationManager &rm = RelationManager::instance();
    // rm.deleteCatalog();
    // rm.createCatalog();
    RC rc = rm.deleteTable("People");
    cout << "status " << rc;
    // testCreateInsertRead();
    /* testing */

    // cli->start();

    return 0;
}



/* testing backup */
// {
//         // 1) Get the single instance of the RelationManager
//         RelationManager& rm = RelationManager::instance();
//
//         // 2) Create the catalog (i.e., "Tables" and "Columns")
//         RC rc = rm.createCatalog();
//         cout << "[TEST] createCatalog rc=" << rc << endl;
//         if (rc != 0)
//         {
//             cout << "[TEST] createCatalog failed. Aborting test." << endl;
//             return 0;
//         }
//
//         // 3) We'll open the "Tables" file and print out all rows
//         {
//             FileHandle tablesFile;
//             rc = RecordBasedFileManager::instance().openFile("Tables", tablesFile);
//             if (rc != 0)
//             {
//                 cout << "[TEST] openFile(\"Tables\") failed rc=" << rc << endl;
//             }
//             else
//             {
//                 // Build the descriptor for "Tables"
//                 std::vector<Attribute> tablesDesc;
//                 getTablesRecordDescriptor(tablesDesc);
//
//                 // We'll do an RBFM scan with NO condition => read ALL rows
//                 RBFM_ScanIterator scanIter;
//                 std::vector<std::string> projection = {"table-id", "table-name", "file-name"};
//                 rc = RecordBasedFileManager::instance().scan(tablesFile,
//                                                              tablesDesc,
//                                                              "", // conditionAttribute
//                                                              NO_OP, // compOp
//                                                              nullptr, // value
//                                                              projection,
//                                                              scanIter);
//                 if (rc != 0)
//                 {
//                     cout << "[TEST] scan(\"Tables\") init failed, rc=" << rc << endl;
//                 }
//                 else
//                 {
//                     // Iterate through results
//                     RID rid;
//                     char returnedData[200];
//
//                     cout << "[TEST] --- Content of 'Tables' table ---" << endl;
//                     while (true)
//                     {
//                         rc = scanIter.getNextRecord(rid, returnedData);
//                         if (rc == RBFM_EOF)
//                         {
//                             break;
//                         }
//                         if (rc != 0)
//                         {
//                             cout << "[TEST] getNextRecord error rc=" << rc << endl;
//                             break;
//                         }
//                         // Print the row
//                         RecordBasedFileManager::instance().printRecord(tablesDesc, returnedData, cout);
//                         cout << endl;
//                     }
//                     scanIter.close();
//                 }
//
//                 RecordBasedFileManager::instance().closeFile(tablesFile);
//             }
//         }
//
//         // 4) We'll open the "Columns" file and print out all rows
//         {
//             FileHandle columnsFile;
//             rc = RecordBasedFileManager::instance().openFile("Columns", columnsFile);
//             if (rc != 0)
//             {
//                 cout << "[TEST] openFile(\"Columns\") failed rc=" << rc << endl;
//             }
//             else
//             {
//                 // Build the descriptor for "Columns"
//                 std::vector<Attribute> columnsDesc;
//                 getColumnsRecordDescriptor(columnsDesc);
//
//                 // We'll do an RBFM scan with NO condition => read ALL rows
//                 RBFM_ScanIterator scanIter;
//                 std::vector<std::string> projection = {
//                     "table-id", "column-name", "column-type", "column-length", "column-position"
//                 };
//                 rc = RecordBasedFileManager::instance().scan(columnsFile,
//                                                              columnsDesc,
//                                                              "", // no condition
//                                                              NO_OP, // compOp
//                                                              nullptr,
//                                                              projection,
//                                                              scanIter);
//                 if (rc != 0)
//                 {
//                     cout << "[TEST] scan(\"Columns\") init failed, rc=" << rc << endl;
//                 }
//                 else
//                 {
//                     // Iterate through results
//                     RID rid;
//                     char returnedData[300];
//
//                     cout << "[TEST] --- Content of 'Columns' table ---" << endl;
//                     while (true)
//                     {
//                         rc = scanIter.getNextRecord(rid, returnedData);
//                         if (rc == RBFM_EOF)
//                         {
//                             break;
//                         }
//                         if (rc != 0)
//                         {
//                             cout << "[TEST] getNextRecord error rc=" << rc << endl;
//                             break;
//                         }
//                         // Print the row
//                         RecordBasedFileManager::instance().printRecord(columnsDesc, returnedData, cout);
//                         cout << endl;
//                     }
//                     scanIter.close();
//                 }
//
//                 RecordBasedFileManager::instance().closeFile(columnsFile);
//             }
//         }
//
//         // 5) Done. Optionally, check your disk to see the "Tables" and "Columns" files.
//         //    You can also manually open them in a hex editor to observe the raw data.
//     }
/* testing backup */

