#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

const Status BufMgr::allocBuf(int & frame) 
{
    int scanned = 0;
    while (scanned < (2 * numBufs)) {
        advanceClock();
        scanned++;

        BufDesc* bd = &bufTable[clockHand];

        if (!bd->valid) { // free buffer
            frame = bd->frameNo;
            return OK;
        } else if (bd->pinCnt > 0) {
            continue;
        } else if (bd->refbit) { // reset refbit if it was true before
            bd->refbit = false;
            continue;
        }

        if (bd->dirty) {
            Status s = bd->file->writePage(bd->pageNo, &bufPool[bd->frameNo]);
            if (s != OK) return UNIXERR;
            bd->dirty = false;
        }

        hashTable->remove(bd->file, bd->pageNo);

        bd->Clear();
        frame = bd->frameNo;
        return OK;
    }

    return BUFFEREXCEEDED;
}


const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    if (!file || PageNo < 1) return UNIXERR;

    int frameNo = -1;
    Status s = hashTable->lookup(file, PageNo, frameNo);
    if (s == OK) {
        BufDesc* bd = &bufTable[frameNo];
        bd->refbit = true;
        bd->pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    int victimFrame = -1;
    s = allocBuf(victimFrame);
    if (s != OK) return s; // returns BUFFEREXCEEDED

    s = file->readPage(PageNo, &bufPool[victimFrame]);
    if (s != OK) return UNIXERR;

    s = hashTable->insert(file, PageNo, victimFrame);
    if (s != OK) return HASHTBLERROR;

    bufTable[victimFrame].Set(file, PageNo);

    page = &bufPool[victimFrame];
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    int frameNo = -1;
    Status s = hashTable->lookup(file, PageNo, frameNo);
    if (s != OK) return HASHNOTFOUND;

    BufDesc* bd = &bufTable[frameNo];

    if (bd->pinCnt == 0) return PAGENOTPINNED;

    bd->pinCnt--;
    if (dirty) bd->dirty = true;

    return OK;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    if (!file) return UNIXERR;

    Status s = file->allocatePage(pageNo);
    if (s != OK) return s;

    int frameNo = -1;
    s = allocBuf(frameNo);
    if (s != OK) return s;

    bufPool[frameNo].init(pageNo);

    s = hashTable->insert(file, pageNo, frameNo);
    if (s != OK) return HASHTBLERROR;

    bufTable[frameNo].Set(file, pageNo);

    page = &bufPool[frameNo];

    return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


