#include "src/include/rbfm.h"

#include <cmath>
#include <cstring>

#include "src/include/rbfm_utils.h"

namespace PeterDB
{
    RecordBasedFileManager& RecordBasedFileManager::instance()
    {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager()
    {
        pagedFileManager = &PagedFileManager::instance();
    }

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager&) = default;

    RecordBasedFileManager& RecordBasedFileManager::operator=(const RecordBasedFileManager&) = default;

    RC RecordBasedFileManager::createFile(const std::string& fileName)
    {
        return pagedFileManager->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string& fileName)
    {
        return pagedFileManager->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string& fileName, FileHandle& fileHandle)
    {
        return pagedFileManager->openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle& fileHandle)
    {
        return pagedFileManager->closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                            const std::vector<Attribute> &recordDescriptor,
                                            const void *data,
                                            RID &rid)
    {
        unsigned recordSize = computeRecordSize(recordDescriptor, data);

        // Find a page that can accommodate recordSize + 4 (for new slot)
        unsigned totalPages = fileHandle.getNumberOfPages();
        if (totalPages == (unsigned) -1) {
            // error getting #pages
            return -1;
        }

        // If file is empty, create the first page
        if (totalPages == 0) {
            char newPage[PAGE_SIZE];
            initNewPage(newPage);
            if (fileHandle.appendPage(newPage) != 0) {
                return -1;
            }
            totalPages = fileHandle.getNumberOfPages(); // now 1
        }

        int targetPage = -1;

        // (A) First try the last page
        {
            char pageBuf[PAGE_SIZE];
            if (fileHandle.readPage(totalPages - 1, pageBuf) != 0) {
                return -1;
            }
            if (canPageHoldRecord(pageBuf, recordSize)) {
                targetPage = (int) (totalPages - 1);
            }
        }

        // (B) If last page can't hold, scan from page 0..(last-2)
        if (targetPage < 0 && totalPages > 1) {
            for (unsigned p = 0; p < totalPages - 1; p++) {
                char pageBuf[PAGE_SIZE];
                if (fileHandle.readPage(p, pageBuf) != 0) {
                    return -1;
                }
                if (canPageHoldRecord(pageBuf, recordSize)) {
                    targetPage = p;
                    break;
                }
            }
        }

        // (C) If still no page found, append a new empty page
        if (targetPage < 0) {
            char newPage[PAGE_SIZE];
            initNewPage(newPage);
            if (fileHandle.appendPage(newPage) != 0) {
                return -1;
            }
            // newly appended page index
            targetPage = (int) fileHandle.getNumberOfPages() - 1;
        }

        // Read the chosen page
        char pageData[PAGE_SIZE];
        if (fileHandle.readPage((unsigned)targetPage, pageData) != 0) {
            return -1;
        }

        unsigned short freeOffset = getFreeSpaceOffset(pageData);
        unsigned short numSlots   = getNumSlots(pageData);

        std::memcpy(pageData + freeOffset, data, recordSize);

        setSlotInfo(pageData, numSlots, freeOffset, (unsigned short)recordSize);

        setNumSlots(pageData, numSlots + 1);
        setFreeSpaceOffset(pageData, freeOffset + recordSize);

        if (fileHandle.writePage((unsigned)targetPage, pageData) != 0) {
            return -1;
        }

        rid.pageNum = (unsigned)targetPage;
        rid.slotNum = numSlots;

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                          const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid,
                                          void *data) {
        // Validate the pageNum
        unsigned pageNum = rid.pageNum;
        unsigned totalPages = fileHandle.getNumberOfPages();
        if (pageNum >= totalPages || totalPages == (unsigned) -1) {
            // page out of range or error getting pages
            return -1;
        }

        // Read the page into a local buffer
        char pageData[PAGE_SIZE];
        RC rc = fileHandle.readPage(pageNum, pageData);
        if (rc != 0) {
            // could not read page from disk
            return -1;
        }

        // Validate the slotNum
        unsigned short slotNum = rid.slotNum;
        unsigned short numSlots = getNumSlots(pageData);
        if (slotNum >= numSlots) {
            // slot out of range
            return -1;
        }

        // Get this slot’s (offset, length)
        unsigned short offset, length;
        getSlotInfo(pageData, slotNum, offset, length);

        // If length == 0 means "deleted record"
        if (length == 0) {
            return -1; // record was deleted
        }

        // Copy the record bytes from page into 'data'
        //    For Project 1, we stored the record "as is", so we can just memcpy out
        std::memcpy(data, pageData + offset, length);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                            const RID& rid)
    {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute>& recordDescriptor, const void* data,
                                           std::ostream& out)
    {
        unsigned numFields = (unsigned)recordDescriptor.size();
        if (numFields == 0)
        {
            // Nothing to print
            return 0;
        }

        int nullIndicatorSize = (int)ceil((double)numFields / 8);

        const unsigned char* nullIndicator = (const unsigned char*)data;

        int offset = nullIndicatorSize;

        for (unsigned i = 0; i < numFields; i++)
        {
            if (i > 0)
            {
                out << ", ";
            }

            out << recordDescriptor[i].name << ": ";

            int bytePos = i / 8; // which byte
            int bitPosInByte = 7 - (i % 8); // which bit (0..7) from the left
            bool isNull = (nullIndicator[bytePos] & (1 << bitPosInByte)) != 0;

            if (isNull)
            {
                out << "NULL";
                // no offset advancement, because no data bytes for this field
                continue;
            }

            switch (recordDescriptor[i].type)
            {
            case TypeInt:
                {
                    int value;
                    memcpy(&value, (char*)data + offset, sizeof(int));
                    offset += sizeof(int);
                    out << value; // print integer
                    break;
                }

            case TypeReal:
                {
                    float value;
                    memcpy(&value, (char*)data + offset, sizeof(float));
                    offset += sizeof(float);
                    out << value;
                    break;
                }

            case TypeVarChar:
                {
                    int length;
                    memcpy(&length, (char*)data + offset, sizeof(int));
                    offset += sizeof(int);

                    std::string str;
                    str.resize(length);
                    memcpy(&str[0], (char*)data + offset, length);
                    offset += length;

                    out << str;
                    break;
                }

            default:
                {
                    // Should not happen if we only have (int, real, varchar)
                    return -1;
                }
            } // end switch
        }

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                            const void* data, const RID& rid)
    {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                             const RID& rid, const std::string& attributeName, void* data)
    {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                    const std::string& conditionAttribute, const CompOp compOp, const void* value,
                                    const std::vector<std::string>& attributeNames,
                                    RBFM_ScanIterator& rbfm_ScanIterator)
    {
        return -1;
    }
} // namespace PeterDB
