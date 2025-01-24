#include <cstring>
#include <cmath>
#include <src/include/pfm.h>
#include <src/include/rbfm.h>
#include "src/include/rbfm_utils.h"

namespace PeterDB
{
    void initNewPage(void* pageData)
    {
        std::memset(pageData, 0, PAGE_SIZE);
        unsigned short freeOffset = 0;
        unsigned short numSlots = 0;

        // last 4 bytes:
        //  [2 bytes: freeSpaceOffset][2 bytes: numSlots]
        std::memcpy((char*)pageData + PAGE_SIZE - SIZE_OF_UNSIGNED_SHORT,
                    &freeOffset, SIZE_OF_UNSIGNED_SHORT);
        std::memcpy((char*)pageData + PAGE_SIZE - 2 * SIZE_OF_UNSIGNED_SHORT,
                    &numSlots, SIZE_OF_UNSIGNED_SHORT);
    }

    unsigned short getNumSlots(const void* pageData)
    {
        unsigned short numSlots;
        std::memcpy(&numSlots,
                    (char*)pageData + PAGE_SIZE - 2 * SIZE_OF_UNSIGNED_SHORT,
                    SIZE_OF_UNSIGNED_SHORT);
        return numSlots;
    }

    void setNumSlots(void* pageData, unsigned short numSlots)
    {
        std::memcpy((char*)pageData + PAGE_SIZE - 2 * SIZE_OF_UNSIGNED_SHORT,
                    &numSlots,
                    SIZE_OF_UNSIGNED_SHORT);
    }

    unsigned short getFreeSpaceOffset(const void* pageData)
    {
        unsigned short freeOffset;
        std::memcpy(&freeOffset,
                    (char*)pageData + PAGE_SIZE - SIZE_OF_UNSIGNED_SHORT,
                    SIZE_OF_UNSIGNED_SHORT);
        return freeOffset;
    }

    void setFreeSpaceOffset(void* pageData, unsigned short freeSpaceOffset)
    {
        std::memcpy((char*)pageData + PAGE_SIZE - SIZE_OF_UNSIGNED_SHORT,
                    &freeSpaceOffset,
                    SIZE_OF_UNSIGNED_SHORT);
    }

    void getSlotInfo(const void* pageData, unsigned short slotNum,
                     unsigned short& offset, unsigned short& length)
    {
        // Each slot is 4 bytes
        // The slotNum=0 is located at: PAGE_SIZE - 4 - 2*sizeof(unsigned short
        size_t slotBase =
            PAGE_SIZE - 2 * SIZE_OF_UNSIGNED_SHORT // skip freeOffset + numSlots
            - (slotNum + 1) * 4; // skip N slot entries

        std::memcpy(&offset, (char*)pageData + slotBase, SIZE_OF_UNSIGNED_SHORT);
        std::memcpy(&length, (char*)pageData + slotBase + 2, SIZE_OF_UNSIGNED_SHORT);
    }

    void setSlotInfo(void* pageData, unsigned short slotNum,
                     unsigned short offset, unsigned short length)
    {
        size_t slotBase =
            PAGE_SIZE - 2 * SIZE_OF_UNSIGNED_SHORT
            - (slotNum + 1) * 4;

        std::memcpy((char*)pageData + slotBase, &offset, SIZE_OF_UNSIGNED_SHORT);
        std::memcpy((char*)pageData + slotBase + 2, &length, SIZE_OF_UNSIGNED_SHORT);
    }

    static inline unsigned short getNullIndicatorSize(unsigned numFields)
    {
        return (unsigned short)std::ceil((double)numFields / 8.0);
    }

    unsigned computeRecordSize(const std::vector<Attribute>& recordDescriptor,
                               const void* data)
    {
        unsigned numFields = (unsigned)recordDescriptor.size();
        unsigned short nullBytes = getNullIndicatorSize(numFields);

        // Start reading after the null-indicator region
        const unsigned char* nullIndicator = (const unsigned char*)data;
        unsigned offset = nullBytes;

        for (unsigned i = 0; i < numFields; i++)
        {
            // Check if field i is NULL
            int bytePos = i / 8;
            int bitPos = 7 - (i % 8);
            bool isNull = (nullIndicator[bytePos] & (1 << bitPos)) != 0;

            if (!isNull)
            {
                switch (recordDescriptor[i].type)
                {
                case TypeInt: // 4 bytes
                    offset += 4;
                    break;
                case TypeReal: // 4 bytes
                    offset += 4;
                    break;
                case TypeVarChar:
                    {
                        // first 4 bytes => length
                        int varLength;
                        std::memcpy(&varLength, (char*)data + offset, sizeof(int));
                        offset += sizeof(int) + varLength;
                        break;
                    }
                default:
                    // Just in case
                    break;
                }
            }
            // if isNull => no bytes for the field
        }

        return offset; // total bytes from 0..offset-1
    }

    bool canPageHoldRecord(const void* pageData, unsigned recordSize)
    {
        // We'll also need +4 bytes for a new slot entry
        unsigned short freeOffset = getFreeSpaceOffset(pageData);
        unsigned short numSlots = getNumSlots(pageData);

        // The start of the slot directory is at:
        // PAGE_SIZE - 2*SIZE_OF_UNSIGNED_SHORT - (numSlots * 4)
        unsigned short slotDirStart =
            PAGE_SIZE - 2 * SIZE_OF_UNSIGNED_SHORT - (numSlots * 4);

        // We want: freeOffset + recordSize <= slotDirStart - 4
        // because we need 4 bytes for one new slot entry
        return (freeOffset + recordSize <= slotDirStart - 4);
    }

    void markSlotDeleted(void *pageData, unsigned short slotNum) {
        setSlotInfo(pageData, slotNum, 0, 0);
    }

    void shiftDataInPage(void *pageData, unsigned short start, unsigned short amount) {
        unsigned short freeOffset = getFreeSpaceOffset(pageData);
        unsigned short bytesToMove = freeOffset - (start + amount);

        // Slide the region downward:
        memmove((char*)pageData + start,
                (char*)pageData + start + amount,
                bytesToMove);

        // The new "next free byte" is 'amount' closer to the start
        setFreeSpaceOffset(pageData, (unsigned short)(freeOffset - amount));
    }

    void adjustSlotOffsets(void *pageData, unsigned short start, unsigned short amount) {
        unsigned short numSlots = getNumSlots(pageData);

        for (unsigned short s = 0; s < numSlots; s++) {
            unsigned short sOffset, sLength;
            getSlotInfo(pageData, s, sOffset, sLength);

            // Only adjust if this slot is not deleted (sLength!=0) AND its offset > 'start'
            if (sLength != 0 && sOffset > start) {
                sOffset = (unsigned short)(sOffset - amount);
                setSlotInfo(pageData, s, sOffset, sLength);
            }
        }
    }
}
