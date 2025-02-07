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

    bool isTombstone(unsigned short length) {
        return (length == TOMBSTONE_LENGTH);
    }

    void writeTombstone(void *pageData, unsigned short offset, const RID &newLocation) {
        memcpy((char*)pageData + offset, &newLocation.pageNum, sizeof(unsigned));
        memcpy((char*)pageData + offset + sizeof(unsigned), &newLocation.slotNum, sizeof(unsigned short));
    }

    void readTombstone(const void *pageData, unsigned short offset, RID &dest) {
        memcpy(&dest.pageNum, (char*)pageData + offset, sizeof(unsigned));
        memcpy(&dest.slotNum, (char*)pageData + offset + sizeof(unsigned), sizeof(unsigned short));
    }

    int findAttributeIndex(const std::vector<Attribute> &recordDescriptor,
                           const std::string &attributeName) {
        for (int i = 0; i < (int)recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == attributeName) {
                return i;
            }
        }
        return -1; // not found
    }

    RC parseSingleAttribute(const void *recordData,
                            unsigned recordSize,
                            const std::vector<Attribute> &recordDescriptor,
                            unsigned attrIndex,
                            void *outData) {
        if (attrIndex >= recordDescriptor.size()) {
            return -1; // invalid index
        }

        unsigned numFields = (unsigned)recordDescriptor.size();
        unsigned nullIndicatorSize = (unsigned)std::ceil((double)numFields / 8.0);

        if (recordSize < nullIndicatorSize) {
            return -1;
        }

        // The recordData layout: [n-byte nullIndicator][field data...]
        const unsigned char *nullIndicator = static_cast<const unsigned char*>(recordData);
        unsigned currentOffset = nullIndicatorSize;

        for (unsigned i = 0; i < numFields; i++) {
            int bytePos = i / 8;
            int bitPosInByte = 7 - (i % 8);
            bool isNull = (nullIndicator[bytePos] & (1 << bitPosInByte)) != 0;

            if (i == attrIndex) {
                // Prepare a 1-byte null indicator
                unsigned char *singleNullIndicator = static_cast<unsigned char*>(outData);
                *singleNullIndicator = 0; // default 0

                if (isNull) {
                    singleNullIndicator[0] = 0x80;
                }
                else {
                    char *outRaw = reinterpret_cast<char*>(outData);
                    // outRaw + 1 => space for the actual attribute data

                    switch (recordDescriptor[i].type) {
                        case TypeInt: {
                            // Must ensure we don't exceed recordSize
                            if (currentOffset + sizeof(int) > recordSize) return -1;
                            std::memcpy(outRaw + 1,
                                        (char*)recordData + currentOffset,
                                        sizeof(int));
                            break;
                        }
                        case TypeReal: {
                            if (currentOffset + sizeof(float) > recordSize) return -1;
                            std::memcpy(outRaw + 1,
                                        (char*)recordData + currentOffset,
                                        sizeof(float));
                            break;
                        }
                        case TypeVarChar: {
                            if (currentOffset + sizeof(int) > recordSize) return -1;
                            int varLen;
                            std::memcpy(&varLen, (char*)recordData + currentOffset, sizeof(int));

                            // check bounds
                            if (currentOffset + sizeof(int) + (unsigned)varLen > recordSize) {
                                return -1;
                            }

                            std::memcpy(outRaw + 1,
                                        (char*)recordData + currentOffset,
                                        sizeof(int));

                            std::memcpy(outRaw + 1 + sizeof(int),
                                        (char*)recordData + currentOffset + sizeof(int),
                                        varLen);
                            break;
                        }
                        default:
                            return -1; // unknown type
                    }
                }

                // Done extracting the attribute
                return 0;
            }
            else {
                // Skip over the i-th field if it's not NULL
                if (!isNull) {
                    switch (recordDescriptor[i].type) {
                        case TypeInt:
                        case TypeReal:
                            if (currentOffset + 4 > recordSize) return -1;
                            currentOffset += 4;
                            break;
                        case TypeVarChar: {
                            if (currentOffset + sizeof(int) > recordSize) return -1;
                            int varLen;
                            std::memcpy(&varLen, (char*)recordData + currentOffset, sizeof(int));

                            if (currentOffset + sizeof(int) + (unsigned)varLen > recordSize) {
                                return -1;
                            }
                            currentOffset += sizeof(int) + varLen;
                            break;
                        }
                        default:
                            return -1;
                    }
                }
                // If null, offset does not move
            }
        }

        return -1;
    }

    bool checkRecordCondition(const void *fullRecord,
                              unsigned recordSize,
                              const std::vector<Attribute> &recordDescriptor,
                              const std::string &conditionAttribute,
                              const CompOp compOp,
                              const void *compValue) {
        if (compOp == NO_OP) return true;

        if (conditionAttribute.empty()) {
            return true;
        }

        int condIndex = findAttributeIndex(recordDescriptor, conditionAttribute);
        if (condIndex < 0) {
            return false;
        }

        char singleAttrBuf[PAGE_SIZE];
        std::memset(singleAttrBuf, 0, PAGE_SIZE);

        RC rc = parseSingleAttribute(fullRecord, recordSize, recordDescriptor, condIndex, singleAttrBuf);
        if (rc != 0) {
            return false;
        }

        bool isNull = ((unsigned char)singleAttrBuf[0] & 0x80) != 0;
        if (isNull) {
            return (compOp == NE_OP);
        }

        AttrType type = recordDescriptor[condIndex].type;
        const char *attrValue = singleAttrBuf + 1; // skip 1-byte null indicator

        switch (type) {
            case TypeInt:
            {
                int recordVal;
                std::memcpy(&recordVal, attrValue, sizeof(int));
                int condVal;
                std::memcpy(&condVal, compValue, sizeof(int));
                switch (compOp) {
                    case EQ_OP: return (recordVal == condVal);
                    case LT_OP: return (recordVal < condVal);
                    case LE_OP: return (recordVal <= condVal);
                    case GT_OP: return (recordVal > condVal);
                    case GE_OP: return (recordVal >= condVal);
                    case NE_OP: return (recordVal != condVal);
                    default:    return false;
                }
            }
            case TypeReal:
            {
                float recordVal;
                std::memcpy(&recordVal, attrValue, sizeof(float));
                float condVal;
                std::memcpy(&condVal, compValue, sizeof(float));
                switch (compOp) {
                    case EQ_OP: return (recordVal == condVal);
                    case LT_OP: return (recordVal < condVal);
                    case LE_OP: return (recordVal <= condVal);
                    case GT_OP: return (recordVal > condVal);
                    case GE_OP: return (recordVal >= condVal);
                    case NE_OP: return (recordVal != condVal);
                    default:    return false;
                }
            }
            case TypeVarChar:
            {
                int recordLen;
                std::memcpy(&recordLen, attrValue, sizeof(int));
                const char *recordStr = attrValue + 4;

                int condLen;
                std::memcpy(&condLen, (char *)compValue, sizeof(int));
                const char *condStr = (char *)compValue + 4;

                std::string recString(recordStr, recordLen);
                std::string condString(condStr, condLen);

                int cmp = recString.compare(condString);
                switch (compOp) {
                    case EQ_OP: return (cmp == 0);
                    case LT_OP: return (cmp < 0);
                    case LE_OP: return (cmp <= 0);
                    case GT_OP: return (cmp > 0);
                    case GE_OP: return (cmp >= 0);
                    case NE_OP: return (cmp != 0);
                    default:    return false;
                }
            }
            default:
                return false;
        }
    }

    RC projectRecord(const void *fullRecord,
                     unsigned recordSize,
                     const std::vector<Attribute> &recordDescriptor,
                     const std::vector<std::string> &attributeNames,
                     void *outData) {
        auto x = (unsigned)attributeNames.size();
        auto nullBytes = (unsigned)std::ceil((double)x / 8.0);

        auto *outNulls = (unsigned char *)outData;
        std::memset(outNulls, 0, nullBytes);

        unsigned writeOffset = nullBytes;

        for (unsigned i = 0; i < x; i++) {
            int indexInDesc = findAttributeIndex(recordDescriptor, attributeNames[i]);
            if (indexInDesc < 0) {
                int bytePos = i / 8;
                int bitPos = 7 - (i % 8);
                outNulls[bytePos] |= (1 << bitPos);
                continue;
            }

            char singleAttrBuf[PAGE_SIZE];
            std::memset(singleAttrBuf, 0, PAGE_SIZE);
            RC rc = parseSingleAttribute(fullRecord,
                                         recordSize,
                                         recordDescriptor,
                                         indexInDesc,
                                         singleAttrBuf);
            if (rc != 0) {
                // parse failed => treat as null
                int bytePos = i / 8;
                int bitPos = 7 - (i % 8);
                outNulls[bytePos] |= (1 << bitPos);
                continue;
            }

            bool isNull = ((unsigned char)singleAttrBuf[0] & 0x80) != 0;
            if (isNull) {
                int bytePos = i / 8;
                int bitPos = 7 - (i % 8);
                outNulls[bytePos] |= (1 << bitPos);
            }
            else {
                AttrType aType = recordDescriptor[indexInDesc].type;
                const char *fieldData = singleAttrBuf + 1; // skip null byte

                switch (aType) {
                    case TypeInt:
                    case TypeReal:
                    {
                        if (writeOffset + 4 > PAGE_SIZE) return -1; // safety
                        std::memcpy((char *)outData + writeOffset, fieldData, 4);
                        writeOffset += 4;
                        break;
                    }
                    case TypeVarChar:
                    {
                        int varLen;
                        std::memcpy(&varLen, fieldData, sizeof(int));
                        if (writeOffset + 4 + (unsigned)varLen > PAGE_SIZE) return -1;

                        std::memcpy((char *)outData + writeOffset, fieldData, 4);
                        writeOffset += 4;

                        std::memcpy((char *)outData + writeOffset,
                                    fieldData + 4,
                                    varLen);
                        writeOffset += varLen;
                        break;
                    }
                    default:
                        return -1; // unknown
                }
            }
        }

        return 0;
    }

    static void readOneFieldIfPossible(const Attribute &attr,
                                       const void *slotData,
                                       unsigned slotLen,
                                       unsigned currentOffset,
                                       bool &isNull,
                                       unsigned &consumed,
                                       char *destField /*where we store the actual field bytes*/) {
        isNull = true; // default
        consumed = 0;

        // If we don't even have 4 bytes for an Int/Real or for the length of a VarChar,
        // treat it as NULL:
        if (currentOffset >= slotLen) {
            // isNull remains true, consumed remains 0
            return;
        }

        switch (attr.type) {
            case TypeInt:
            case TypeReal:
                if (currentOffset + 4 <= slotLen) {
                    // We can read 4 bytes
                    memcpy(destField, (char*)slotData + currentOffset, 4);
                    isNull = false;
                    consumed = 4;
                }
                break;
            case TypeVarChar: {
                if (currentOffset + 4 > slotLen) {
                    // can't even read the length
                    break;
                }
                int varLen;
                memcpy(&varLen, (char*)slotData + currentOffset, 4);

                // Check if we have enough bytes: 4 for length + varLen
                if (currentOffset + 4 + varLen <= slotLen) {
                    // We can read the entire string
                    memcpy(destField, (char*)slotData + currentOffset, 4 + varLen);
                    isNull = false;
                    consumed = 4 + varLen;
                }
                break;
            }
        }
    }

    /*
     * A function that, given:
     *   - raw slotData (the *old* record as stored),
     *   - slotLen (bytes in slotData),
     *   - desiredSchema (the *new or current* schema),
     * produces a standard “nullIndicator + fields” byte-array in `reinterpretedData`
     * that exactly fits desiredSchema.size() attributes.
     *
     * This is a naive approach:
     *   - If there's not enough data to read all fields, the missing fields are NULL.
     *   - If there's leftover data after reading `desiredSchema.size()` fields, we ignore it.
     */
    RC adjustRecordToNewSchema(const void *slotData,
                               unsigned slotLen,
                               const std::vector<Attribute> &desiredSchema,
                               void *reinterpretedData) {

        unsigned numAttrs = (unsigned)desiredSchema.size();
        if (numAttrs == 0) {
            // no attributes => nothing to do
            return 0;
        }

        // size of the null-indicator for the new record
        unsigned nullIndicatorSize = (unsigned)std::ceil((double)numAttrs / 8.0);

        // zero out the output for safety
        std::memset(reinterpretedData, 0, nullIndicatorSize);

        unsigned writeOffset = nullIndicatorSize; // start writing fields after the null-indicator
        unsigned readOffset = 0; // current offset in slotData

        // For each attribute in the *new* schema, see if we can parse from old data:
        for (unsigned i = 0; i < numAttrs; i++) {
            bool isNull;
            unsigned consumed;
            char fieldBuf[PAGE_SIZE];

            // Attempt to read the i-th field from the old slot data
            readOneFieldIfPossible(desiredSchema[i],
                                   slotData,
                                   slotLen,
                                   readOffset,
                                   isNull,
                                   consumed,
                                   fieldBuf);

            if (isNull) {
                // Mark the bit in the new record's null-indicator
                int bytePos = i / 8;
                int bitPos = 7 - (i % 8);
                unsigned char *nulls = (unsigned char*)reinterpretedData;
                nulls[bytePos] |= (1 << bitPos);

                // No actual data bytes get written
            } else {
                // We have a real field:
                // For TypeInt/Real, 4 bytes
                // For TypeVarChar, 4 + varLen
                switch (desiredSchema[i].type) {
                    case TypeInt:
                    case TypeReal:
                        memcpy((char*)reinterpretedData + writeOffset, fieldBuf, 4);
                        writeOffset += 4;
                        break;
                    case TypeVarChar: {
                        int varLen;
                        memcpy(&varLen, fieldBuf, 4);

                        // copy the length + the characters
                        memcpy((char*)reinterpretedData + writeOffset, fieldBuf, 4 + varLen);
                        writeOffset += 4 + varLen;
                        break;
                    }
                }
            }

            // Whether or not it was null, advance readOffset by `consumed`.
            // This ensures we “use up” however many bytes we recognized from the old record.
            readOffset += consumed;
        }

        return 0;
    }
}
