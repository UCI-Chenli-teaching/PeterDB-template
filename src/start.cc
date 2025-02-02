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
    {
        // 1) Get the single instance of the RelationManager
        RelationManager& rm = RelationManager::instance();

        // 2) Create the catalog (i.e., "Tables" and "Columns")
        RC rc = rm.createCatalog();
        cout << "[TEST] createCatalog rc=" << rc << endl;
        if (rc != 0)
        {
            cout << "[TEST] createCatalog failed. Aborting test." << endl;
            return 0;
        }

        // 3) We'll open the "Tables" file and print out all rows
        {
            FileHandle tablesFile;
            rc = RecordBasedFileManager::instance().openFile("Tables", tablesFile);
            if (rc != 0)
            {
                cout << "[TEST] openFile(\"Tables\") failed rc=" << rc << endl;
            }
            else
            {
                // Build the descriptor for "Tables"
                std::vector<Attribute> tablesDesc;
                getTablesRecordDescriptor(tablesDesc);

                // We'll do an RBFM scan with NO condition => read ALL rows
                RBFM_ScanIterator scanIter;
                std::vector<std::string> projection = {"table-id", "table-name", "file-name"};
                rc = RecordBasedFileManager::instance().scan(tablesFile,
                                                             tablesDesc,
                                                             "", // conditionAttribute
                                                             NO_OP, // compOp
                                                             nullptr, // value
                                                             projection,
                                                             scanIter);
                if (rc != 0)
                {
                    cout << "[TEST] scan(\"Tables\") init failed, rc=" << rc << endl;
                }
                else
                {
                    // Iterate through results
                    RID rid;
                    char returnedData[200];

                    cout << "[TEST] --- Content of 'Tables' table ---" << endl;
                    while (true)
                    {
                        rc = scanIter.getNextRecord(rid, returnedData);
                        if (rc == RBFM_EOF)
                        {
                            break;
                        }
                        if (rc != 0)
                        {
                            cout << "[TEST] getNextRecord error rc=" << rc << endl;
                            break;
                        }
                        // Print the row
                        RecordBasedFileManager::instance().printRecord(tablesDesc, returnedData, cout);
                        cout << endl;
                    }
                    scanIter.close();
                }

                RecordBasedFileManager::instance().closeFile(tablesFile);
            }
        }

        // 4) We'll open the "Columns" file and print out all rows
        {
            FileHandle columnsFile;
            rc = RecordBasedFileManager::instance().openFile("Columns", columnsFile);
            if (rc != 0)
            {
                cout << "[TEST] openFile(\"Columns\") failed rc=" << rc << endl;
            }
            else
            {
                // Build the descriptor for "Columns"
                std::vector<Attribute> columnsDesc;
                getColumnsRecordDescriptor(columnsDesc);

                // We'll do an RBFM scan with NO condition => read ALL rows
                RBFM_ScanIterator scanIter;
                std::vector<std::string> projection = {
                    "table-id", "column-name", "column-type", "column-length", "column-position"
                };
                rc = RecordBasedFileManager::instance().scan(columnsFile,
                                                             columnsDesc,
                                                             "", // no condition
                                                             NO_OP, // compOp
                                                             nullptr,
                                                             projection,
                                                             scanIter);
                if (rc != 0)
                {
                    cout << "[TEST] scan(\"Columns\") init failed, rc=" << rc << endl;
                }
                else
                {
                    // Iterate through results
                    RID rid;
                    char returnedData[300];

                    cout << "[TEST] --- Content of 'Columns' table ---" << endl;
                    while (true)
                    {
                        rc = scanIter.getNextRecord(rid, returnedData);
                        if (rc == RBFM_EOF)
                        {
                            break;
                        }
                        if (rc != 0)
                        {
                            cout << "[TEST] getNextRecord error rc=" << rc << endl;
                            break;
                        }
                        // Print the row
                        RecordBasedFileManager::instance().printRecord(columnsDesc, returnedData, cout);
                        cout << endl;
                    }
                    scanIter.close();
                }

                RecordBasedFileManager::instance().closeFile(columnsFile);
            }
        }

        // 5) Done. Optionally, check your disk to see the "Tables" and "Columns" files.
        //    You can also manually open them in a hex editor to observe the raw data.
    }
    /* testing */

    // cli->start();

    return 0;
}
