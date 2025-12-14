#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	Status status;
	AttrDesc attrDescArray[attrCnt];
    int reclen = 0;

    for (int i = 0; i < attrCnt; i++) {
        status = attrCat->getInfo(relation, 
                                  attrList[i].attrName, 
                                  attrDescArray[i]);

        if (status != OK) return status;
        if (attrDescArray[i].attrType != attrList[i].attrType)
            return ATTRTYPEMISMATCH;
		
		int currentEnd = attrDescArray[i].attrOffset + attrDescArray[i].attrLen;
        if (currentEnd > reclen) reclen = currentEnd;
    }

    char outputData[reclen];
    memset(outputData, 0, reclen);

    for (int i = 0; i < attrCnt; i++) {
        // Copy attrList data, uses attrOffset to place each value in relation order
        char* dest = outputData + attrDescArray[i].attrOffset;
        if (attrDescArray[i].attrType == INTEGER) {
            int val = atoi((char*)attrList[i].attrValue);
            memcpy(dest, &val, sizeof(int));
        }
        else if (attrDescArray[i].attrType == FLOAT) {
            float val = atof((char*)attrList[i].attrValue);
            memcpy(dest, &val, sizeof(float));
        }
        // Just copies the bytes for a string
        else memcpy(dest, attrList[i].attrValue, attrDescArray[i].attrLen);
    }

    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

    InsertFileScan insertScan(relation, status);
    if (status != OK) return status; 

    RID outRID;
    return insertScan.insertRecord(outputRec, outRID);
}

