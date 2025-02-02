#include "src/include/rm_utils.h"
#include "src/include/rbfm.h"

#include <cmath>    // for std::ceil
#include <cstring>  // for memcpy
#include <string>   // for stoi/stof

namespace PeterDB
{
    RC buildDataFromStrings(const std::vector<Attribute>& recordDescriptor,
                            const std::vector<std::string>& values,
                            void* buffer,
                            unsigned& recordSize)
    {
        // Must match number of columns
        if (values.size() != recordDescriptor.size())
        {
            return -1;
        }

        const unsigned numFields = (unsigned)recordDescriptor.size();
        const int nullIndicatorSize = (int)std::ceil((double)numFields / 8.0);

        unsigned offset = 0;

        std::vector<unsigned char> nullIndicator(nullIndicatorSize, 0);
        memcpy((char*)buffer + offset, nullIndicator.data(), nullIndicatorSize);
        offset += nullIndicatorSize;

        // Now copy each field from 'values'
        for (unsigned i = 0; i < numFields; i++)
        {
            const Attribute& attr = recordDescriptor[i];
            const std::string& valStr = values[i];

            switch (attr.type)
            {
            case TypeInt:
                {
                    int ival = std::stoi(valStr); // convert string -> int
                    memcpy((char*)buffer + offset, &ival, sizeof(int));
                    offset += sizeof(int);
                    break;
                }
            case TypeReal:
                {
                    float fval = std::stof(valStr); // convert string -> float
                    memcpy((char*)buffer + offset, &fval, sizeof(float));
                    offset += sizeof(float);
                    break;
                }
            case TypeVarChar:
                {
                    // We store 4 bytes = length, then the actual chars
                    int length = (int)valStr.size();
                    memcpy((char*)buffer + offset, &length, sizeof(int));
                    offset += sizeof(int);

                    memcpy((char*)buffer + offset, valStr.c_str(), length);
                    offset += length;
                    break;
                }
            default:
                // unknown attr.type
                return -1;
            }
        }

        recordSize = offset;
        return 0; // success
    }


    RC insertRowGeneric(FileHandle& fileHandle,
                        const std::vector<Attribute>& recordDescriptor,
                        const std::vector<std::string>& fieldValues)
    {
        char recordBuffer[PAGE_SIZE];
        unsigned recordSize = 0;
        RC rc = buildDataFromStrings(recordDescriptor, fieldValues, recordBuffer, recordSize);
        if (rc != 0)
        {
            return rc;
        }

        // Insert into the file
        RID rid;
        rc = RecordBasedFileManager::instance().insertRecord(fileHandle,
                                                             recordDescriptor,
                                                             recordBuffer,
                                                             rid);
        return rc;
    }

    void getTablesRecordDescriptor(std::vector<Attribute> &descriptor) {
        descriptor.clear();

        Attribute attr;
        // 1) table-id : INT
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // 2) table-name : VARCHAR(50)
        attr.name = "table-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        descriptor.push_back(attr);

        // 3) file-name : VARCHAR(50)
        attr.name = "file-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        descriptor.push_back(attr);
    }

    void getColumnsRecordDescriptor(std::vector<Attribute> &descriptor) {
        descriptor.clear();

        Attribute attr;
        // 1) table-id : INT
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // 2) column-name : VARCHAR(50)
        attr.name = "column-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        descriptor.push_back(attr);

        // 3) column-type : INT
        attr.name = "column-type";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // 4) column-length : INT
        attr.name = "column-length";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);

        // 5) column-position : INT
        attr.name = "column-position";
        attr.type = TypeInt;
        attr.length = 4;
        descriptor.push_back(attr);
    }
} // namespace PeterDB
