#include "catalog.h"
#include "query.h"
#include <stdlib.h>
#include <string.h>

// forward declaration
const Status ScanSelect(const string & result,
			const int projCnt,
			const AttrDesc projNames[],
			const AttrDesc *attrDesc,
			const Operator op,
			const char *filter,
			const int reclen);


const Status QU_Select(const string & result,
		       const int projCnt,
		       const attrInfo projNames[],
		       const attrInfo *attr,
		       const Operator op,
		       const char *attrValue)
{
    cout << "Doing QU_Select " << endl;

    Status status;

    // build projection attribute descriptions
    AttrDesc projDescs[projCnt];
    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName,
                                  projNames[i].attrName,
                                  projDescs[i]);
        if (status != OK) return status;
        reclen += projDescs[i].attrLen;
    }

    // set up selection attribute (if any)
    AttrDesc selAttr;
    AttrDesc *selAttrPtr = NULL;
    const char *filter = NULL;
    int iFilter = 0;
    float fFilter = 0;
    char sFilter[MAXSTRINGLEN];

    if (attr != NULL) {
        if (attrValue == NULL) return BADCATPARM;

        status = attrCat->getInfo(attr->relName, attr->attrName, selAttr);
        if (status != OK) return status;

        if (attr->attrType != -1 && selAttr.attrType != attr->attrType)
            return ATTRTYPEMISMATCH;

        switch ((Datatype) selAttr.attrType) {
        case INTEGER:
            iFilter = atoi(attrValue);
            filter = (char *) &iFilter;
            break;
        case FLOAT:
            fFilter = (float) atof(attrValue);
            filter = (char *) &fFilter;
            break;
        case STRING:
            if (selAttr.attrLen > MAXSTRINGLEN) return ATTRTOOLONG;
            memset(sFilter, 0, sizeof(sFilter));
            strncpy(sFilter, attrValue, selAttr.attrLen);
            filter = sFilter;
            break;
        }
        selAttrPtr = &selAttr;
    }

    return ScanSelect(result, projCnt, projDescs, selAttrPtr, op, filter, reclen);
}


const Status ScanSelect(const string & result,
			const int projCnt,
			const AttrDesc projNames[],
			const AttrDesc *attrDesc,
			const Operator op,
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;

    InsertFileScan resultRel(result, status);
    if (status != OK) return status;

    string relName = attrDesc ? string(attrDesc->relName) : string(projNames[0].relName);
    HeapFileScan scan(relName, status);
    if (status != OK) return status;

    if (attrDesc == NULL)
        status = scan.startScan(0, 0, STRING, NULL, EQ);
    else
        status = scan.startScan(attrDesc->attrOffset,
                                attrDesc->attrLen,
                                (Datatype) attrDesc->attrType,
                                filter,
                                op);
    if (status != OK) return status;

    char *outData = new char[reclen];
    if (!outData) return INSUFMEM;

    Record outRec;
    outRec.data = outData;
    outRec.length = reclen;

    RID rid;
    Record rec;
    int tupleCnt = 0;

    while ((status = scan.scanNext(rid)) == OK) {
        status = scan.getRecord(rec);
        if (status != OK) {
            delete [] outData;
            return status;
        }

        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outData + offset,
                   (char *) rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        RID outRid;
        status = resultRel.insertRecord(outRec, outRid);
        if (status != OK) {
            delete [] outData;
            return status;
        }
        tupleCnt++;
    }

    delete [] outData;

    if (status != FILEEOF) return status;

    cout << "QU_Select produced " << tupleCnt << " result tuples" << endl;
    return OK;
}
