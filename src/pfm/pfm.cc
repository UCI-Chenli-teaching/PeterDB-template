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

        file->seekg(0, std::ios::beg);

        unsigned int counters[4];
        file->read(reinterpret_cast<char*>(counters), sizeof(counters));
        if (file->fail()) {
            return -1;
        }
        fileHandle.readPageCounter = counters[static_cast<unsigned int>(CounterType::READ_PAGE_COUNTER)];
        fileHandle.writePageCounter = counters[static_cast<unsigned int>(CounterType::WRITE_PAGE_COUNTER)];
        fileHandle.appendPageCounter = counters[static_cast<unsigned int>(CounterType::APPEND_PAGE_COUNTER)];
        fileHandle.numberOfPages = counters[static_cast<unsigned int>(CounterType::NUMBER_OF_PAGES)];

        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        unsigned int counters[4];
        counters[static_cast<unsigned int>(CounterType::NUMBER_OF_PAGES)] = fileHandle.numberOfPages;
        counters[static_cast<unsigned int>(CounterType::READ_PAGE_COUNTER)] = fileHandle.readPageCounter;
        counters[static_cast<unsigned int>(CounterType::WRITE_PAGE_COUNTER)] = fileHandle.writePageCounter;
        counters[static_cast<unsigned int>(CounterType::APPEND_PAGE_COUNTER)] = fileHandle.appendPageCounter;

        fileHandle.file->seekp(0, std::ios::beg);
        fileHandle.file->write(reinterpret_cast<const char*>(counters), sizeof(counters));
        if (fileHandle.file->fail()) {
            return -1;
        }
        fileHandle.file->flush();


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
        // file->seekg(0, std::ios::beg);

        // Read the existing 4 counters (16 bytes) from the file header
        // unsigned int counters[4];
        // file->read(reinterpret_cast<char*>(counters), sizeof(counters));
        // if (file->fail()) {
        //     return -1;
        // }
        //
        // counters[static_cast<unsigned int>(counterType)]++;

        // // Write the updated counters back to the file header
        // file->seekp(0, std::ios::beg);
        // file->write(reinterpret_cast<const char*>(counters), sizeof(counters));
        // if (file->fail()) {
        //     return -1;
        // }
        // file->flush();


        switch (counterType) {
            case CounterType::READ_PAGE_COUNTER:
                readPageCounter++;
                break;
            case CounterType::WRITE_PAGE_COUNTER:
                writePageCounter++;
                break;
            case CounterType::APPEND_PAGE_COUNTER:
                appendPageCounter++;
                break;
            case CounterType::NUMBER_OF_PAGES:
                numberOfPages++;
                break;
        }

        return 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;  // File not open
        }

        unsigned totalPages = getNumberOfPages();
        if (pageNum >= totalPages) {
            return -1; // Page number out of bounds
        }

        std::streamoff offset = (1 + static_cast<std::streamoff>(pageNum)) * PAGE_SIZE;

        file->seekg(offset, std::ios::beg);
        if (file->fail()) {
            return -1; // Seek failed
        }

        file->read(reinterpret_cast<char*>(data), PAGE_SIZE);
        if (file->fail()) {
            return -1; // Read failed
        }

        RC rc = increaseCounter(CounterType::READ_PAGE_COUNTER);
        if (rc != 0) {
            return -1; // Could not increment the counter
        }

        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;  // File not open
        }

        unsigned totalPages = getNumberOfPages();
        if (pageNum >= totalPages) {
            return -1; // Page number out of bounds
        }

        std::streamoff offset = (1 + static_cast<std::streamoff>(pageNum)) * PAGE_SIZE;

        file->seekp(offset, std::ios::beg);
        if (file->fail()) {
            return -1; // Seek failed
        }

        file->write(reinterpret_cast<const char*>(data), PAGE_SIZE);
        if (file->fail()) {
            return -1; // Write failed
        }
        file->flush();

        RC rc = increaseCounter(CounterType::WRITE_PAGE_COUNTER);
        if (rc != 0) {
            return -1; // Could not increment the counter
        }

        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        // Safety checks
        if (file == nullptr || !file->is_open()) {
            return -1;
        }

        file->seekp(0, ios::end); // Move to the end of the file
        file->write(reinterpret_cast<const char*>(data), PAGE_SIZE);
        if (file->fail()) {
            return -1; // Return an error code if the write operation failed
        }
        file->flush();

        increaseCounter(CounterType::APPEND_PAGE_COUNTER);
        increaseCounter(CounterType::NUMBER_OF_PAGES);
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return numberOfPages;
    }


    RC FileHandle::collectCounterValues(unsigned &readPageCount,
                                    unsigned &writePageCount,
                                    unsigned &appendPageCount) {
        // safety check
        if (file == nullptr || !file->is_open()) {
            return -1;
        }

        readPageCount   = readPageCounter;
        writePageCount  = writePageCounter;
        appendPageCount = appendPageCounter;

        return 0;
    }

} // namespace PeterDB