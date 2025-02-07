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

    RC RecordBasedFileManager::insertRecord(FileHandle& fileHandle,
                                            const std::vector<Attribute>& recordDescriptor,
                                            const void* data,
                                            RID& rid)
    {
        unsigned recordSize = computeRecordSize(recordDescriptor, data);
        unsigned totalPages = fileHandle.getNumberOfPages();

        if (totalPages == 0)
        {
            char newPage[PAGE_SIZE];
            initNewPage(newPage);
            if (fileHandle.appendPage(newPage) != 0)
            {
                return -1;
            }
            totalPages = fileHandle.getNumberOfPages(); // now 1
        }

        int targetPage = -1;

        {
            char pageBuf[PAGE_SIZE];
            if (fileHandle.readPage(totalPages - 1, pageBuf) != 0)
            {
                return -1;
            }
            if (canPageHoldRecord(pageBuf, recordSize))
            {
                targetPage = static_cast<int>(totalPages - 1);
            }
        }

        if (targetPage < 0 && totalPages > 1)
        {
            for (unsigned p = totalPages - 1; p > 0 - 1; p++)
            {
                char pageBuf[PAGE_SIZE];
                if (fileHandle.readPage(p, pageBuf) != 0)
                {
                    return -1;
                }
                if (canPageHoldRecord(pageBuf, recordSize))
                {
                    targetPage = static_cast<int>(p);
                    break;
                }
            }
        }

        if (targetPage < 0)
        {
            char newPage[PAGE_SIZE];
            initNewPage(newPage);
            if (fileHandle.appendPage(newPage) != 0)
            {
                return -1;
            }
            targetPage = static_cast<int>(fileHandle.getNumberOfPages() - 1);
        }

        char pageData[PAGE_SIZE];
        if (fileHandle.readPage(static_cast<unsigned>(targetPage), pageData) != 0)
        {
            return -1;
        }

        unsigned short numSlots = getNumSlots(pageData);
        unsigned short freeOffset = getFreeSpaceOffset(pageData);

        bool reuseEmptySlot = false;
        unsigned short slotToUse = 0;

        for (unsigned short i = 0; i < numSlots; i++)
        {
            unsigned short sOffset, sLength;
            getSlotInfo(pageData, i, sOffset, sLength);

            if (sLength == 0)
            {
                reuseEmptySlot = true;
                slotToUse = i;
                break;
            }
        }

        if (!reuseEmptySlot)
        {
            slotToUse = numSlots;
            setNumSlots(pageData, numSlots + 1);
        }

        std::memcpy(pageData + freeOffset, data, recordSize);
        setSlotInfo(pageData, slotToUse, freeOffset, static_cast<unsigned short>(recordSize));
        setFreeSpaceOffset(pageData, static_cast<unsigned short>(freeOffset + recordSize));

        if (fileHandle.writePage(static_cast<unsigned>(targetPage), pageData) != 0)
        {
            return -1;
        }

        rid.pageNum = static_cast<unsigned>(targetPage);
        rid.slotNum = slotToUse;

        return 0;
    }


    RC RecordBasedFileManager::readRecord(FileHandle& fileHandle,
                                          const std::vector<Attribute>& recordDescriptor,
                                          const RID& rid,
                                          void* data)
    {
        char pageData[PAGE_SIZE];
        RC rc = fileHandle.readPage(rid.pageNum, pageData);
        if (rc != 0)
        {
            return -1;
        }

        unsigned short numSlots = getNumSlots(pageData);
        if (rid.slotNum >= numSlots) return -1;
        unsigned short offset, length;
        getSlotInfo(pageData, rid.slotNum, offset, length);
        if (length == 0)
        {
            return -1; // deleted record
        }

        if (isTombstone(length))
        {
            RID fwd;
            readTombstone(pageData, offset, fwd);
            return readRecord(fileHandle, recordDescriptor, fwd, data);
        }

        char rawSlotBuf[length];
        memcpy(rawSlotBuf, pageData + offset, length);

        unsigned numFields = (unsigned)recordDescriptor.size();
        unsigned nullIndicatorSize = (unsigned)ceil((double)numFields / 8.0);

        memset(data, 0, nullIndicatorSize);

        unsigned oldNumFields = (unsigned)recordDescriptor.size();

        unsigned oldNullSize = (unsigned)ceil((double)oldNumFields / 8.0);
        if (oldNullSize > length)
        {
            return -1;
        }

        const unsigned char* oldNulls = (unsigned char*)rawSlotBuf;
        unsigned readOffset = oldNullSize;

        unsigned char* newNulls = (unsigned char*)data;

        unsigned writeOffset = nullIndicatorSize;

        for (unsigned i = 0; i < numFields; i++)
        {
            int bytePos = i / 8;
            int bitPos = 7 - (i % 8);

            bool oldIsNull = false;
            if (bytePos < (int)oldNullSize)
            {
                oldIsNull = (oldNulls[bytePos] & (1 << bitPos)) != 0;
            }
            if (readOffset >= length)
            {
                oldIsNull = true;
            }

            if (oldIsNull)
            {
                newNulls[bytePos] |= (1 << bitPos);
                continue;
            }

            switch (recordDescriptor[i].type)
            {
            case TypeInt:
            case TypeReal:
                {
                    if (readOffset + 4 > length)
                    {
                        newNulls[bytePos] |= (1 << bitPos);
                    }
                    else
                    {
                        memcpy((char*)data + writeOffset, rawSlotBuf + readOffset, 4);
                        writeOffset += 4;
                        readOffset += 4;
                    }
                    break;
                }
            case TypeVarChar:
                {
                    // first read 4 bytes => length of string
                    if (readOffset + 4 > length)
                    {
                        // no space => set NULL
                        newNulls[bytePos] |= (1 << bitPos);
                        break;
                    }
                    int varLen;
                    memcpy(&varLen, rawSlotBuf + readOffset, 4);

                    // check if we have enough bytes for the string itself
                    if (readOffset + 4 + varLen > length)
                    {
                        // truncated => set NULL
                        newNulls[bytePos] |= (1 << bitPos);
                    }
                    else
                    {
                        // copy length + string
                        memcpy((char*)data + writeOffset, rawSlotBuf + readOffset, 4 + varLen);
                        writeOffset += (4 + varLen);
                        readOffset += (4 + varLen);
                    }
                    break;
                }
            default:
                {
                    // unknown type => treat as error or null
                    return -1;
                }
            }
        }

        return 0;
    }


    RC RecordBasedFileManager::deleteRecord(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                            const RID& rid)
    {
        char pageData[PAGE_SIZE];
        if (fileHandle.readPage(rid.pageNum, pageData) != 0)
        {
            return -1;
        }

        unsigned short numSlots = getNumSlots(pageData);
        if (rid.slotNum >= numSlots)
        {
            return -1;
        }

        unsigned short offset, length;
        getSlotInfo(pageData, rid.slotNum, offset, length);

        if (length == 0)
        {
            // Already deleted => nothing more to do
            return 0;
        }

        if (isTombstone(length))
        {
            // It's a tombstone => read the pointer
            RID fwd;
            readTombstone(pageData, offset, fwd);

            // Recursively delete the *real* record
            deleteRecord(fileHandle, recordDescriptor, fwd);

            // Now remove the tombstone (6 bytes) from this page
            shiftDataInPage(pageData, offset, TOMBSTONE_SIZE);
            adjustSlotOffsets(pageData, offset, TOMBSTONE_SIZE);
            markSlotDeleted(pageData, rid.slotNum);
        }
        else
        {
            // This slot contains an actual record => remove it
            shiftDataInPage(pageData, offset, length);
            adjustSlotOffsets(pageData, offset, length);
            markSlotDeleted(pageData, rid.slotNum);
        }

        if (fileHandle.writePage(rid.pageNum, pageData) != 0)
        {
            return -1;
        }

        return 0;
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
        char pageData[PAGE_SIZE];
        if (fileHandle.readPage(rid.pageNum, pageData) != 0)
        {
            return -1;
        }

        unsigned short numSlots = getNumSlots(pageData);
        if (rid.slotNum >= numSlots) return -1;

        unsigned short oldOffset, oldLength;
        getSlotInfo(pageData, rid.slotNum, oldOffset, oldLength);

        // If slot is deleted already
        if (oldLength == 0)
        {
            return -1; // or treat as "no-op"
        }

        // If it's a tombstone, follow it and update in the forwarding location
        if (isTombstone(oldLength))
        {
            RID fwd;
            readTombstone(pageData, oldOffset, fwd);
            return updateRecord(fileHandle, recordDescriptor, data, fwd);
        }

        // compute new record size
        unsigned newSize = computeRecordSize(recordDescriptor, data);

        // === CASE 1: newSize <= oldLength ===
        if (newSize <= oldLength)
        {
            memcpy(pageData + oldOffset, data, newSize);

            short diff = oldLength - newSize; // this is >= 0
            if (diff > 0)
            {
                // remove the 'diff' bytes from the page
                shiftDataInPage(pageData, (unsigned short)(oldOffset + newSize), (unsigned short)diff);
                // fix slot offsets
                adjustSlotOffsets(pageData, (unsigned short)(oldOffset + newSize), (unsigned short)diff);
            }

            // update the slot's new length
            setSlotInfo(pageData, rid.slotNum, oldOffset, (unsigned short)newSize);

            if (fileHandle.writePage(rid.pageNum, pageData) != 0)
            {
                return -1;
            }
            return 0;
        }

        // === CASE 2: newSize > oldLength ===
        // Easiest to do the "delete + re-insert in same slot if possible" approach:
        // 1) Temporarily remove the old record & compact the page
        // 2) See if the new record fits now
        // 3) If yes, store in same slot
        // 4) If no, create a tombstone and insert the record elsewhere

        // 1) Delete + compact
        shiftDataInPage(pageData, oldOffset, oldLength);
        adjustSlotOffsets(pageData, oldOffset, oldLength);
        markSlotDeleted(pageData, rid.slotNum);

        // 2) Check if new record fits
        unsigned short freeOffset = getFreeSpaceOffset(pageData);
        unsigned short slotDirStart = PAGE_SIZE - 2 * sizeof(unsigned short) - numSlots * 4;
        // we still have the same number of slots, but rid.slotNum is free
        // We need newSize more bytes + 0 for a new slot (We reuse the slot, so no new slot overhead.)
        if (freeOffset + newSize <= slotDirStart)
        {
            memcpy(pageData + freeOffset, data, newSize);
            setSlotInfo(pageData, rid.slotNum, freeOffset, (unsigned short)newSize);
            setFreeSpaceOffset(pageData, (unsigned short)(freeOffset + newSize));

            if (fileHandle.writePage(rid.pageNum, pageData) != 0)
            {
                return -1;
            }
            return 0;
        }
        else
        {
            // not enough space -> tombstone
            if (fileHandle.writePage(rid.pageNum, pageData) != 0)
            {
                return -1;
            }

            // now insert record in some other page
            RID newLocation;
            RC rc = insertRecord(fileHandle, recordDescriptor, data, newLocation);
            if (rc != 0) return -1;

            // re-read the old page (since we wrote it out) to store tombstone
            if (fileHandle.readPage(rid.pageNum, pageData) != 0)
            {
                return -1;
            }

            // again, ensure slot is still free, then see if we have at least 6 bytes to store the tombstone
            freeOffset = getFreeSpaceOffset(pageData);
            if (freeOffset + TOMBSTONE_SIZE > slotDirStart)
            {
                // TODO: In a corner case where even 6 bytes won't fit, we might expand your approach
                //  to move the tombstone to a new page, etc. For simplicity, return error
                return -1;
            }

            // store tombstone data in oldOffset
            setSlotInfo(pageData, rid.slotNum, freeOffset, TOMBSTONE_LENGTH);
            writeTombstone(pageData, freeOffset, newLocation);

            // advance free offset by 6
            setFreeSpaceOffset(pageData, (unsigned short)(freeOffset + TOMBSTONE_SIZE));

            if (fileHandle.writePage(rid.pageNum, pageData) != 0)
            {
                return -1;
            }
            return 0;
        }
    }

    RC RecordBasedFileManager::readAttribute(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                             const RID& rid, const std::string& attributeName, void* data)
    {
        int attrIndex = findAttributeIndex(recordDescriptor, attributeName);
        if (attrIndex < 0)
        {
            return -1;
        }

        char pageData[PAGE_SIZE];
        if (fileHandle.readPage(rid.pageNum, pageData) != 0)
        {
            return -1;
        }

        unsigned short numSlots = getNumSlots(pageData);
        if (rid.slotNum >= numSlots)
        {
            return -1;
        }

        unsigned short offset, length;
        getSlotInfo(pageData, rid.slotNum, offset, length);

        if (length == 0)
        {
            return -1;
        }

        if (isTombstone(length))
        {
            RID fwd;
            readTombstone(pageData, offset, fwd);
            return readAttribute(fileHandle, recordDescriptor, fwd, attributeName, data);
        }

        char recordBuf[PAGE_SIZE];
        memcpy(recordBuf, pageData + offset, length);

        return parseSingleAttribute(recordBuf,
                                    (unsigned)length,
                                    recordDescriptor,
                                    (unsigned)attrIndex,
                                    data);
    }

    RC RecordBasedFileManager::scan(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                    const std::string& conditionAttribute, const CompOp compOp, const void* value,
                                    const std::vector<std::string>& attributeNames,
                                    RBFM_ScanIterator& rbfm_ScanIterator)
    {
        return rbfm_ScanIterator.init(fileHandle,
                                      recordDescriptor,
                                      conditionAttribute,
                                      compOp,
                                      value,
                                      attributeNames);
    }

    RC RBFM_ScanIterator::init(FileHandle& fh,
                               const std::vector<Attribute>& recordDesc,
                               const std::string& condAttr,
                               const CompOp op,
                               const void* value,
                               const std::vector<std::string>& attrNames)
    {
        fileHandle = &fh;
        recordDescriptor = recordDesc;
        conditionAttribute = condAttr;
        compOp = op;
        attributeNames = attrNames;
        isOpen = true;

        currentPage = 0;
        currentSlot = 0;
        totalPages = fileHandle->getNumberOfPages();

        compValue.clear();
        if (value != nullptr && op != NO_OP)
        {
            AttrType condType = TypeInt; // default fallback
            for (auto& attr : recordDescriptor)
            {
                if (attr.name == conditionAttribute)
                {
                    condType = attr.type;
                    break;
                }
            }

            const char* bytes = reinterpret_cast<const char*>(value);
            switch (condType)
            {
            case TypeInt:
            case TypeReal:
                {
                    // 4 bytes
                    compValue.assign(bytes, bytes + 4);
                    break;
                }
            case TypeVarChar:
                {
                    // The first 4 bytes store the length
                    int varLen = 0;
                    std::memcpy(&varLen, bytes, sizeof(int));
                    // total = 4 bytes + varLen
                    compValue.resize(sizeof(int) + varLen);
                    std::memcpy(compValue.data(), bytes, sizeof(int) + varLen);
                    break;
                }
            default:
                // Should not happen with only (int, float, varchar)
                break;
            }
        }

        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID& rid, void* data)
    {
        if (!isOpen || fileHandle == nullptr)
        {
            return RBFM_EOF;
        }

        char recordData[PAGE_SIZE];

        while (currentPage < totalPages)
        {
            char pageBuf[PAGE_SIZE];
            if (fileHandle->readPage(currentPage, pageBuf) != 0)
            {
                currentPage++;
                currentSlot = 0;
                continue;
            }

            unsigned short numSlots = getNumSlots(pageBuf);

            while (currentSlot < numSlots)
            {
                unsigned short offset, length;
                getSlotInfo(pageBuf, currentSlot, offset, length);
                if (length == 0 || isTombstone(length))
                {
                    currentSlot++;
                    continue;
                }

                rid.pageNum = currentPage;
                rid.slotNum = currentSlot;
                currentSlot++;

                RC rc = RecordBasedFileManager::instance().readRecord(
                    *fileHandle,
                    recordDescriptor,
                    rid,
                    recordData);

                if (rc != 0)
                {
                    continue;
                }

                bool passCond = checkRecordCondition(recordData,
                                                     PAGE_SIZE,
                                                     recordDescriptor,
                                                     conditionAttribute,
                                                     compOp,
                                                     compValue.empty() ? nullptr : &compValue[0]);
                if (!passCond)
                {
                    continue;
                }

                RC prc = projectRecord(recordData,
                                       PAGE_SIZE,
                                       recordDescriptor,
                                       attributeNames,
                                       data);
                if (prc != 0)
                {
                    continue;
                }

                return 0;
            }

            currentPage++;
            currentSlot = 0;
        }

        return RBFM_EOF;
    }


    RC RBFM_ScanIterator::close()
    {
        isOpen = false;
        compValue.clear();
        return 0;
    }
} // namespace PeterDB
