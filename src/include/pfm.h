#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <fstream>
#include <ios>
#include <string>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    enum class CounterType {
        READ_PAGE_COUNTER = 0,
        WRITE_PAGE_COUNTER,
        APPEND_PAGE_COUNTER,
        NUMBER_OF_PAGES
    };

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        unsigned numberOfPages;
        std::fstream *file;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC increaseCounter(CounterType counterType);                        // Increase counter
        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
    };

} // namespace PeterDB

#endif // _pfm_h_