#ifndef RBFM_UTILS_H
#define RBFM_UTILS_H

#define SIZE_OF_UNSIGNED_SHORT sizeof(unsigned short)

#include "src/include/rbfm.h"

namespace PeterDB {
    // A special marker for tombstones:
    static const unsigned short TOMBSTONE_LENGTH = 0xFFFF;
    // Enough bytes for storing pageNum(4) + slotNum(2)
    static const unsigned short TOMBSTONE_SIZE   = 6;

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

    // Mark the specified slot as deleted (offset=0, length=0).
    void markSlotDeleted(void *pageData, unsigned short slotNum);

    // Shift the bytes in [start+amount .. freeOffset-1] down by 'amount' bytes,
    // and reduce the freeSpaceOffset by 'amount'.
    void shiftDataInPage(void *pageData, unsigned short start, unsigned short amount);

    // For each valid slot whose offset is greater than 'start', subtract 'amount' from its offset.
    void adjustSlotOffsets(void *pageData, unsigned short start, unsigned short amount);

    // Check if the length indicates a tombstone
    bool isTombstone(unsigned short length);

    // Write a tombstone (the forwarding RID) into 'pageData + offset'.
    void writeTombstone(void *pageData, unsigned short offset, const RID &newLocation);

    // Read a tombstone's forwarding RID from 'pageData + offset'.
    void readTombstone(const void *pageData, unsigned short offset, RID &dest);

    /**
     * Find index of 'attributeName' in 'recordDescriptor'.
     * Returns -1 if not found.
     */
    int findAttributeIndex(const std::vector<Attribute> &recordDescriptor,
                           const std::string &attributeName);

    /**
     * Parse a single attribute from the raw record data into the output buffer.
     * The recordData format is the same as used in insertRecord/readRecord:
     *   [n-byte null-indicator][field1-data][field2-data]...
     *
     * Output buffer layout for the single attribute will be:
     *   [1-byte null-indicator][attribute bytes if not NULL].
     */
    RC parseSingleAttribute(const void *recordData,
                            unsigned recordSize,
                            const std::vector<Attribute> &recordDescriptor,
                            unsigned attrIndex,
                            void *outData);

    bool checkRecordCondition(const void *fullRecord,
                          unsigned recordSize,
                          const std::vector<PeterDB::Attribute> &recordDescriptor,
                          const std::string &conditionAttribute,
                          const CompOp compOp,
                          const void *compValue);

    RC projectRecord(const void *fullRecord,
                 unsigned recordSize,
                 const std::vector<PeterDB::Attribute> &recordDescriptor,
                 const std::vector<std::string> &attributeNames,
                 void *outData);

    /*
     * A function that, given:
     *   - raw slotData (the *old* record as stored),
     *   - slotLen (bytes in slotData),
     *   - desiredSchema (the *new or current* schema),
     * produces a standard “nullIndicator + fields” byte-array in `reinterpretedData`
     * that exactly fits desiredSchema.size() attributes.
     */
    RC adjustRecordToNewSchema(const void *slotData,
                           unsigned slotLen,
                           const std::vector<Attribute> &desiredSchema,
                           void *reinterpretedData);
}

#endif
