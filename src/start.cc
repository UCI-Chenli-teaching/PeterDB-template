#include <string>

#include "src/include/cli.h"

using namespace std;
using namespace PeterDB;

bool DEMO = false;

PeterDB::CLI *cli;

void exec(const string& command) {
    cout << ">>> " << command << endl;
    cli->process(command);
}

int main() {

    cli = PeterDB::CLI::Instance();
    if (DEMO) {
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
    vector<Attribute> recordDescriptor;
    {
        Attribute attr;
        attr.name = "age";
        attr.type = TypeInt;
        attr.length = 4;
        recordDescriptor.push_back(attr);

        attr.name = "height";
        attr.type = TypeReal;
        attr.length = 4;
        recordDescriptor.push_back(attr);

        attr.name = "name";
        attr.type = TypeVarChar;
        attr.length = 100; // max string length
        recordDescriptor.push_back(attr);
    }

    // 2) Create/open a record-based file
    // --------------------------------------------------
    PagedFileManager &pfm = PagedFileManager::instance();
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    const std::string fileName = "/home/ali/CLionProjects/PeterDB-template/db_files/test_scan";

    // Cleanup if file exists
    pfm.destroyFile(fileName);

    // Create a new file
    RC rc = rbfm.createFile(fileName);
    cout << "[TEST] createFile rc=" << rc << endl;

    // Open it
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    cout << "[TEST] openFile rc=" << rc << endl;

    // 3) Insert some test records
    //    We'll create 5 records with different ages, heights, names
    // --------------------------------------------------
    // record format for "data":
    //   [1-byte or more null-indicator][(age)4bytes][(height)4bytes][(Varchar-len)4bytes + chars]
    //   => We'll build them carefully.

    // Helper to build the "api record" for (age, height, name)
    auto prepareRecord = [&](int ageVal, float heightVal, const std::string &nameVal, void *buffer) {
        // 1) Null-indicator for 3 fields => ceil(3/8) = 1 byte
        //    Suppose none are null => 0x00
        unsigned char nullByte = 0x00;
        memcpy(buffer, &nullByte, 1);

        // 2) age (4 bytes)
        int offset = 1;
        memcpy((char*)buffer + offset, &ageVal, sizeof(int));
        offset += 4;

        // 3) height (4 bytes)
        memcpy((char*)buffer + offset, &heightVal, sizeof(float));
        offset += 4;

        // 4) name => [4-byte length][characters...]
        int nameLen = (int)nameVal.size();
        memcpy((char*)buffer + offset, &nameLen, sizeof(int));
        offset += 4;

        memcpy((char*)buffer + offset, nameVal.c_str(), nameLen);
        offset += nameLen;

        return offset; // total bytes used
    };

    vector<RID> rids; // store the RIDs we get from inserts
    {
        char recordBuf[200];
        // Insert 5 sample records
        struct Sample {
            int age;
            float height;
            string name;
        } samples[5] = {
            {25, 5.6f, "Alice"},
            {30, 5.8f, "Bob"},
            {35, 6.0f, "Charlie"},
            {40, 5.9f, "Diana"},
            {27, 6.2f, "Eric"}
        };

        for (auto &s : samples) {
            int sizeUsed = prepareRecord(s.age, s.height, s.name, recordBuf);

            RID rid;
            rc = rbfm.insertRecord(fileHandle, recordDescriptor, recordBuf, rid);
            if (rc == 0) {
                rids.push_back(rid);
            } else {
                cout << "[TEST] insertRecord failed for " << s.name << endl;
            }
        }
    }

    // 4) Use RBFM scan with a condition: e.g., "age >= 30"
    // --------------------------------------------------
    // We'll define:
    //   conditionAttribute = "age"
    //   compOp = GE_OP
    //   value = int(30)
    // We'll project out all attributes: "age","height","name"

    RBFM_ScanIterator scanIter;
    {
        string conditionAttr = "age";
        CompOp compOp = GE_OP; // >=
        int compValue = 33;    // we'll pass &compValue
        vector<string> projection;
        projection.push_back("age");
        projection.push_back("height");
        projection.push_back("name");

        rc = rbfm.scan(fileHandle,
                       recordDescriptor,
                       conditionAttr,
                       compOp,
                       &compValue,
                       projection,
                       scanIter);
        if (rc != 0) {
            cout << "[TEST] scan() init failed rc=" << rc << endl;
        }
    }

    // 5) Fetch and print results
    // --------------------------------------------------
    {
        RID rid;
        char returnedData[200]; // should be enough for 3 fields

        // We'll read until RBFM_EOF
        while (true) {
            rc = scanIter.getNextRecord(rid, returnedData);
            if (rc == RBFM_EOF) {
                cout << "[TEST] -- SCAN complete --" << endl;
                break;
            }
            if (rc != 0) {
                cout << "[TEST] getNextRecord error rc=" << rc << endl;
                break;
            }

            // We can print the record with rbfm.printRecord()
            rc = rbfm.printRecord(recordDescriptor, returnedData, cout);
            if (rc != 0) {
                cout << "[TEST] printRecord error" << endl;
            }
            else {
                cout << endl; // printRecord typically doesn't add extra newline
            }
        }

        // close the scan
        scanIter.close();
    }

    // 6) Cleanup
    // --------------------------------------------------
    // close the file
    rbfm.closeFile(fileHandle);

    // or you can keep the file around for debugging
    pfm.destroyFile(fileName);
    /* testing */

    // cli->start();

    return 0;
}
