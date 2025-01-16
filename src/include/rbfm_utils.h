#ifndef RBFM_UTILS_H
#define RBFM_UTILS_H

#include "src/include/rbfm.h"

namespace PeterDB {
    // Initialize an empty page (set freeSpaceOffset=0, numSlots=0)
    void initNewPage(void *pageData);

    // Retrieve/set the number of slots in this page
    unsigned short getNumSlots(const void *pageData);
    void setNumSlots(void *pageData, unsigned short numSlots);

    // Retrieve/set the free-space offset in this page
    unsigned short getFreeSpaceOffset(const void *pageData);
    void setFreeSpaceOffset(void *pageData, unsigned short freeSpaceOffset);

    // Retrieve/set slot (offset, length)
    void getSlotInfo(const void *pageData, unsigned short slotNum,
                     unsigned short &offset, unsigned short &length);
    void setSlotInfo(void *pageData, unsigned short slotNum,
                     unsigned short offset, unsigned short length);

    // (parses null-indicator + fields to find total size)
    unsigned computeRecordSize(const std::vector<Attribute> &recordDescriptor,
                               const void *data);

    // Check if there's enough space in pageData for recordSize + new slot
    bool canPageHoldRecord(const void *pageData, unsigned recordSize);
}

#endif
