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
        std::memcpy((char*)pageData + PAGE_SIZE - sizeof(unsigned short),
                    &freeOffset, sizeof(unsigned short));
        std::memcpy((char*)pageData + PAGE_SIZE - 2 * sizeof(unsigned short),
                    &numSlots, sizeof(unsigned short));
    }

    unsigned short getNumSlots(const void* pageData)
    {
        unsigned short numSlots;
        std::memcpy(&numSlots,
                    (char*)pageData + PAGE_SIZE - 2 * sizeof(unsigned short),
                    sizeof(unsigned short));
        return numSlots;
    }

    void setNumSlots(void* pageData, unsigned short numSlots)
    {
        std::memcpy((char*)pageData + PAGE_SIZE - 2 * sizeof(unsigned short),
                    &numSlots,
                    sizeof(unsigned short));
    }

    unsigned short getFreeSpaceOffset(const void* pageData)
    {
        unsigned short freeOffset;
        std::memcpy(&freeOffset,
                    (char*)pageData + PAGE_SIZE - sizeof(unsigned short),
                    sizeof(unsigned short));
        return freeOffset;
    }

    void setFreeSpaceOffset(void* pageData, unsigned short freeSpaceOffset)
    {
        std::memcpy((char*)pageData + PAGE_SIZE - sizeof(unsigned short),
                    &freeSpaceOffset,
                    sizeof(unsigned short));
    }

    void getSlotInfo(const void* pageData, unsigned short slotNum,
                     unsigned short& offset, unsigned short& length)
    {
        // Each slot is 4 bytes
        // The slotNum=0 is located at: PAGE_SIZE - 4 - 2*sizeof(unsigned short
        size_t slotBase =
            PAGE_SIZE - 2 * sizeof(unsigned short) // skip freeOffset + numSlots
            - (slotNum + 1) * 4; // skip N slot entries

        std::memcpy(&offset, (char*)pageData + slotBase, sizeof(unsigned short));
        std::memcpy(&length, (char*)pageData + slotBase + 2, sizeof(unsigned short));
    }

    void setSlotInfo(void* pageData, unsigned short slotNum,
                     unsigned short offset, unsigned short length)
    {
        size_t slotBase =
            PAGE_SIZE - 2 * sizeof(unsigned short)
            - (slotNum + 1) * 4;

        std::memcpy((char*)pageData + slotBase, &offset, sizeof(unsigned short));
        std::memcpy((char*)pageData + slotBase + 2, &length, sizeof(unsigned short));
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
        // PAGE_SIZE - 2*sizeof(unsigned short) - (numSlots * 4)
        unsigned short slotDirStart =
            PAGE_SIZE - 2 * sizeof(unsigned short) - (numSlots * 4);

        // We want: freeOffset + recordSize <= slotDirStart - 4
        // because we need 4 bytes for one new slot entry
        return (freeOffset + recordSize <= slotDirStart - 4);
    }
}
