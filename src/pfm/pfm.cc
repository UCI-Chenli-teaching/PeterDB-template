#include "src/include/pfm.h"

#include <fstream>
#include <iostream>
using namespace std;


namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (ifstream(fileName)) {
            return -1; // Return an error code if the file already exists
        }

        ofstream file(fileName, std::ios::binary);
        if (!file) {
            return -1; // Return an error code if the file could not be created
        }

        unsigned int header[PAGE_SIZE] = {0, 0, 0, 0};
        file.write(reinterpret_cast<const char*>(header), PAGE_SIZE); // Write the header to the file
        if (file.fail()) {
            return -1; // Return an error code if the write operation failed
        }
        file.flush(); // Ensure the data is written to the file
        file.close();

        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if (remove(fileName.c_str()) != 0) {
            return -1; // Return an error code if the file could not be deleted
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        auto *file = new std::fstream(fileName, std::ios::in | std::ios::out | std::ios::binary);
        if (!file->is_open()) {
            delete file;
            return -1; // Return an error code if the file could not be opened
        }
        fileHandle.file = file;
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.file->close();
        delete fileHandle.file;
        fileHandle.file = nullptr;
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        numberOfPages = 0;
    }

    RC FileHandle::increaseCounter(CounterType counterType) {
        // Read the existing 4 counters (16 bytes) from the file header
        file->seekg(0, ios::beg);

        unsigned int counters[4];
        file->read(reinterpret_cast<char*>(counters), PAGE_SIZE);
        if (file->fail()) {
            return -1;
        }

        // Increase the chosen counter by 1
        //    (They are in the order: readPageCounter=0, writePageCounter=1,
        //     appendPageCounter=2, numberOfPages=3)
        counters[static_cast<unsigned int>(counterType)]++;

        // Write the updated counters back to the file header
        file->seekp(0, ios::beg);
        file->write(reinterpret_cast<const char*>(counters), PAGE_SIZE);
        if (file->fail()) {
            return -1;
        }
        file->flush();

        // Update our in-memory variables to reflect the new values
        readPageCounter   = counters[0];
        writePageCounter  = counters[1];
        appendPageCounter = counters[2];
        numberOfPages     = counters[3];

        return 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;  // File not open
        }

        // Check if pageNum is within range
        //    Use getNumberOfPages() to get the total number of pages in the file
        unsigned totalPages = getNumberOfPages();
        if (pageNum >= totalPages) {
            return -1; // Page number out of bounds
        }

        // Calculate the offset
        //    The first 16 bytes are header, so skip that.
        //    Then skip (pageNum * PAGE_SIZE) bytes to reach the page.
        std::streamoff offset = (1 + static_cast<std::streamoff>(pageNum)) * PAGE_SIZE;

        // Seek to the correct offset for reading
        file->seekg(offset, std::ios::beg);
        if (file->fail()) {
            return -1; // Seek failed
        }

        file->read(reinterpret_cast<char*>(data), PAGE_SIZE);
        if (file->fail()) {
            return -1; // Read failed
        }

        // 6. Increment the readPageCounter
        RC rc = increaseCounter(CounterType::READ_PAGE_COUNTER);
        if (rc != 0) {
            return -1; // Could not increment the counter
        }

        return 0; // Success
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;  // File not open
        }

        // Validate that pageNum exists (i.e., it should be within the current number of pages)
        unsigned totalPages = getNumberOfPages();
        if (pageNum >= totalPages) {
            return -1; // Page number out of bounds
        }

        // Calculate the correct offset:
        //    skip the first 16 bytes (header) + pageNum * PAGE_SIZE
        std::streamoff offset = (1 + static_cast<std::streamoff>(pageNum)) * PAGE_SIZE;

        // Seek to that offset for writing
        file->seekp(offset, std::ios::beg);
        if (file->fail()) {
            return -1; // Seek failed
        }

        file->write(reinterpret_cast<const char*>(data), PAGE_SIZE);
        if (file->fail()) {
            return -1; // Write failed
        }
        file->flush();

        // Increase the WRITE_PAGE_COUNTER
        RC rc = increaseCounter(CounterType::WRITE_PAGE_COUNTER);
        if (rc != 0) {
            return -1; // Could not increment the counter
        }

        return 0; // Success
    }

    RC FileHandle::appendPage(const void *data) {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;
        }

        file->seekp(0, ios::end); // Move to the end of the file
        file->write(reinterpret_cast<const char*>(data), PAGE_SIZE); // Write the data
        if (file->fail()) {
            return -1; // Return an error code if the write operation failed
        }
        file->flush(); // Ensure the data is written to the file

        increaseCounter(CounterType::APPEND_PAGE_COUNTER); // Increase the appendPageCounter
        increaseCounter(CounterType::NUMBER_OF_PAGES); // Increase the numberOfPages
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;
        }

        // Read the existing 4 counters (16 bytes) from the file header
        file->seekg(0, ios::beg);

        unsigned int counters[4];
        file->read(reinterpret_cast<char*>(counters), PAGE_SIZE);
        if (file->fail()) {
            return -1;
        }

        return counters[static_cast<unsigned int>(CounterType::NUMBER_OF_PAGES)];
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB