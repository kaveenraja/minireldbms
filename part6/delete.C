#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{

	Status status;
	AttrDesc attrDesc;

	attrCat->getInfo(relation, attrName, attrDesc);


	const char *filter = NULL;
	int iFilter = 0;
	float fFilter = 0;
	char sFilter[MAXSTRINGLEN];

	if(attrValue != NULL){
		switch ((Datatype) attrDesc.attrType) {
        case INTEGER:
            iFilter = atoi(attrValue);
            filter = (char *) &iFilter;
            break;
        case FLOAT:
            fFilter = (float) atof(attrValue);
            filter = (char *) &fFilter;
            break;
        case STRING:
            memset(sFilter, 0, sizeof(sFilter));
            strncpy(sFilter, attrValue, attrDesc.attrLen);
            filter = sFilter;
            break;
		}
	}


	HeapFileScan scan(relation, status);
	if(status != OK) return status;


	status = scan.startScan(attrDesc.attrOffset, attrDesc.attrLen, (Datatype)attrDesc.attrType, filter, op);
	if(status !=OK) return status;

	RID rid;
	while( (status = scan.scanNext(rid)) == OK ) {
		status = scan.deleteRecord();
		if(status != OK) return status;

	}


	return OK;
}


