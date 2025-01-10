#include <string>

#include "src/include/cli.h"

using namespace std;

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

    PeterDB::PagedFileManager &pagedFileManager = PeterDB::PagedFileManager::instance();;
    PeterDB::FileHandle &fileHandle = *(new PeterDB::FileHandle());
    const std::string fileName = "/home/ali/CLionProjects/PeterDB-template/db_files/testpage";
    // pagedFileManager.destroyFile(fileName);
    // int code = pagedFileManager.createFile(fileName);
    // cout << "createFile " << code << endl;
    int code = pagedFileManager.openFile(fileName, fileHandle);
    // char data[PAGE_SIZE] = "test string 3"; // Create a buffer with 4096 bytes of data
    // code = fileHandle.appendPage(data);
    // cout<<"code " << code <<endl;
    // fileHandle.writePage(0, data);
    // cout << "writePage " << code << endl;
    // unsigned numPages = fileHandle.getNumberOfPages();
    char data[PAGE_SIZE];
    code = fileHandle.readPage(0, data);
    if (code != 0) {
        cout << "Error reading page" << endl;
    } else {
        cout << "Page data: " << data << endl;
    }

    // cli->start();

    return 0;
}
