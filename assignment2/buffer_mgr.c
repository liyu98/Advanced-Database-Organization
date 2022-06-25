#include<stdio.h>
#include<stdlib.h>
#include <pthread.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "rm_data_structures.h"

pthread_rwlock_t thread_lock; /* read-write thread lock */

int writeCount = 0; /* number of write page since the BUffer Pool initializes */
int bufferNum = 0;  /* the size of the buffer pool */
int hit = 0;  /* stores the count which is incremented whenever a page frame is added into the buffer pool. */
int rear = 0; /* stores the count of number of pages read from the disk */
int clockPtr = 0; /* to point to the last added page in the buffer pool */

/*
 *  FIFO Strategy
 */
void FIFO(BM_BufferPool *const bm, Frame_Data *page, int pageNum);

/*
 *  LRU Strategy
 */
void LRU(BM_BufferPool *const bm, Frame_Data *page, int pageNum);

/*
 *  CLOCK Strategy
 */
void CLOCK(BM_BufferPool *const bm, Frame_Data *page, int pageNum);


/*
 * init Page Frame
 */
Frame_Data *initPageFrame(const PageNumber pageNum);

/*
 * init Page Frame When PinPage
 */
void initPageInPin(Frame_Data *frame, BM_BufferPool *const bm, int index, int pageNum);

/*
 * check Page Frame In Memory (bufferPool)
 */
bool checkPageInMemory(Frame_Data *frame, BM_BufferPool *const bm, int index, const PageNumber pageNum);

/*
 * Execute Page Frame Replace
 */
void pageFrameReplace(Frame_Data *frame, BM_BufferPool *const bm, BM_PageHandle *const page, int pageNum);

/*
 * Thread-safe writing to block files
 */
void writeBlockWithThreadSafe(BM_BufferPool *const bm, Frame_Data *frame, int Index);

/***********************************************************
*                                                          *
*              Common function method                      *
*                                                          *
***********************************************************/

Frame_Data *initPageFrame(const PageNumber pageNum) {

  // Create reserved memory space = number of pages x space required for one page
  Frame_Data *fp = malloc(sizeof(Frame_Data) * pageNum);
  int i;
  for (i = 0; i < pageNum; i++) {
    fp[i].data = NULL;
    fp[i].pageNum = -1;
    fp[i].dirtyFlag = fp[i].fixCount = 0;
    fp[i].hitNum = fp[i].rfNum = 0;
  }
  return fp;
}

bool checkPageInMemory(Frame_Data *frame, BM_BufferPool *const bm, int index, const PageNumber pageNum) {

  if (frame[index].pageNum == pageNum) {
    /* Incrementing hit */
    hit++;
    frame[index].fixCount++;

    if (bm->strategy == RS_LRU)
      /* LRU uses the value of hit to determine the least recently used page */
      frame[index].hitNum = hit; //
    else if (bm->strategy == RS_CLOCK)
      /* to indicate that this was the last page frame examined (added to the buffer pool) */
      frame[index].hitNum = 1;
    else if (bm->strategy == RS_LFU)
      /* Incrementing Num to add one more to the count of number of times the page is used (referenced) */
      frame[index].rfNum++;
    clockPtr++;
    return true;
  }

  return false;
}

void pageFrameReplace(Frame_Data *frame, BM_BufferPool *const bm, BM_PageHandle *const page, int pageNum) {

  switch (bm->strategy) {
    case RS_FIFO:
      FIFO(bm, frame, bufferNum);
      break;

    case RS_LRU:
      LRU(bm, frame, bufferNum);
      break;

    case RS_CLOCK:
      CLOCK(bm, frame, bufferNum);
      break;

    case RS_LFU:
      printf("LFU not implemented");
      break;

    case RS_LRU_K:
      printf("LRU-K  not implemented");
      break;

    default:
      printf(" Not Implemented\n");
      break;
  }
}

/***********************************************************
*                                                          *
*              Buffer Pool Functions                       *
*                                                          *
***********************************************************/

/**
 * creates a new buffer pool with numPages page frames using the page replacement
 * strategy strategy. The pool is used to cache pages from the page file with name pageFileName.
 * @param bm the Buffer Pool Handler that the user wants to initiate
 * @param pageFileName
 * @param numPages
 * @param strategy
 * @param stratData
 * @return errorCode
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {

  if (bm == NULL) {
    return RC_NULL_ERROR;
  }
  if (pageFileName == NULL) {
    printf("pageFileName is null \n");
    return RC_NULL_ERROR;
  }
  // init BM_BufferPool's profiles
  bm->strategy = strategy;
  bm->numPages = numPages;
  bm->pageFile = (char *) pageFileName;

  // set the bufferNum with the total number of pages in memory or the buffer pool.
  bufferNum = numPages;

  // init Page Frame
  Frame_Data *page = initPageFrame(bufferNum);

  // mgmtData fill with PageFrame
  bm->mgmtData = page;
  clockPtr = 0;
  writeCount = 0;

  return RC_OK;
}

/**
 *  Destroys the buffer pool. This method should free up all resources associated with buffer pool.
 *  it should free the memory allocated for page frames. If the buffer pool contains any dirty pages,
 *  then these pages should be written back to disk before destroying the pool. It is an error to shutdown
 *  a buffer pool that has pinned pages.
 *
 * @param bm BM_BufferPool
 * @return errCode
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
  if (!bm->mgmtData) {
    return RC_NON_EXIST_BUFFERPOOL;
  }

  RC resutCode;

  // Flush all dirty pages in the Buffer Pool back to the disk
  Frame_Data *frm = (Frame_Data *) bm->mgmtData;
  resutCode = forceFlushPool(bm);
  if (resutCode != RC_OK) {
    return resutCode;
  }
  // check fixCount is not zero, return error
  int idx;
  for (idx = 0; idx < bufferNum; idx++) {
    if (frm[idx].fixCount != 0) {
      return RC_PINNED_PAGES_IN_BUFFER;
    }
  }
  // free page frame
  free(frm);
  bm->mgmtData = NULL;
  return RC_OK;
}

/**
 * Write back the data in all dirty pages in the Buffer Pool
 * @param bm  Buffer Pool Handler
 * @return errCode
 */
RC forceFlushPool(BM_BufferPool *const bm) {
  if (bm == NULL) {
    return RC_NULL_ERROR;
  }
  if (bm->mgmtData == NULL) {
    return RC_NON_EXIST_BUFFERPOOL;
  }

  Frame_Data *pf = (Frame_Data *) bm->mgmtData;

  int i;
  for (i = 0; i < bufferNum; i++) {
    if (pf[i].fixCount == 0 && pf[i].dirtyFlag == 1) {

      // Require lock
      pthread_rwlock_init(&thread_lock, NULL);
      pthread_rwlock_wrlock(&thread_lock);

      // Opening page file available on disk, Writing block of data to the page file on disk
      SM_FileHandle h;
      openPageFile(bm->pageFile, &h);
      writeBlock(pf[i].pageNum, &h, pf[i].data);
      pf[i].dirtyFlag = 0;

      // Increase the writeCount which records the number of writes done by the buffer manager.
      writeCount++;

      // Release unlock
      pthread_rwlock_unlock(&thread_lock);
      pthread_rwlock_destroy(&thread_lock);
    }
  }
  return RC_OK;
}

/***********************************************************
*                                                          *
*              Page Management Functions.                  *
*                                                          *
***********************************************************/
/**
 * This function marks the page as dirty indicating that the data of the page has been modified by the client
 * @param bm
 * @param page
 * @return errorCode
 */
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
  if (bm == NULL) {
    return RC_NULL_ERROR;
  }
  if (page == NULL) {
    return RC_NULL_ERROR;
  }

  Frame_Data *pf = (Frame_Data *) bm->mgmtData;
  int i, j;
  j = bufferNum;
  //遍历当前的page frame
  for (i = 0; i < j; i++) {
    if (pf[i].pageNum == page->pageNum) {
      pf[i].dirtyFlag = 1;
      return RC_OK;
    }
  }
  return RC_ERROR;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
  if (bm == NULL) {
    return RC_NULL_ERROR;
  }
  if (page == NULL) {
    return RC_NULL_ERROR;
  }

  Frame_Data *pf = (Frame_Data *) bm->mgmtData;
  int i, j;
  // Traverse all the buffer pool pageframe
  j = bufferNum;
  for (i = 0; i < j; i++) {
    if (pf[i].pageNum == page->pageNum) {
      pf[i].fixCount--;
      break;
    }
  }
  return RC_OK;
}

/**
 *
 * @param bm
 * @param page
 * @return
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {

  Frame_Data *pf = (Frame_Data *) bm->mgmtData;
  int idx;
  for (idx = 0; idx < bufferNum; idx++) {
    if (pf[idx].pageNum == page->pageNum) {
      // Require lock
      pthread_rwlock_init(&thread_lock, NULL);
      pthread_rwlock_wrlock(&thread_lock);
      SM_FileHandle h;
      openPageFile(bm->pageFile, &h);
      writeBlock(pf[idx].pageNum, &h, pf[idx].data);
      /* Mark page as undirty because the modified page has been written to disk */
      pf[idx].dirtyFlag = 0;
      /* Increase the writeCount which records the number of writes done by the buffer manager. */
      writeCount++;
      // Release unlock
      pthread_rwlock_unlock(&thread_lock);
      pthread_rwlock_destroy(&thread_lock);
    }
  }
  return RC_OK;
}

void initPageInPin(Frame_Data *frame, BM_BufferPool *const bm, int index, int pageNum) {
  SM_FileHandle fh;
  openPageFile(bm->pageFile, &fh);
  frame[index].data = (SM_PageHandle) malloc(PAGE_SIZE);
  readBlock(pageNum, &fh, frame[index].data);
  frame[index].pageNum = pageNum;
  frame[index].fixCount = 1;
  frame[index].rfNum = 0;
  rear++;
  hit++;

  if (bm->strategy == RS_LRU)
    // LRU uses the value of hit to determine the least recently used page
    frame[index].hitNum = hit;
  else if (bm->strategy == RS_CLOCK)
    frame[index].hitNum = 1;

}

/**
 *	Pin the page with the requested pageNum in the BUffer Pool
 *	If the page is not in the Buffer Pool, load it from the file to the Buffer Pool
 * @param bm BM_BufferPool
 * @param page BM_PageHandle
 * @param pageNum
 * @return errorCode
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
  if (bm == NULL) {
    return RC_NULL_ERROR;
  }
  if (page == NULL) {
    return RC_NULL_ERROR;
  }
  /*Checking if buffer pool is empty and this is the first page to be pin*/
  Frame_Data *pfData = (Frame_Data *) bm->mgmtData;
  if (pfData[0].pageNum == -1) {
    // pfData[0].pageNum is -1,  Reading page from disk and initializing page frame's content in the buffer pool
    // Reading page from disk and initializing page frame's content in the buffer pool

    // Require lock
    pthread_rwlock_init(&thread_lock, NULL);
    pthread_rwlock_wrlock(&thread_lock);

    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    pfData[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
    ensureCapacity(pageNum, &fh);
    readBlock(pageNum, &fh, pfData[0].data);

    // Release unlock
    pthread_rwlock_unlock(&thread_lock);
    pthread_rwlock_destroy(&thread_lock);

    pfData[0].pageNum = pageNum;
    pfData[0].hitNum = hit;
    pfData[0].rfNum = 0;
    pfData[0].fixCount++;
    rear = hit = 0;

    page->pageNum = pageNum;
    page->data = pfData[0].data;

    return RC_OK;
  }

  /* Determine if buffer_bool is full */
  int idx;
  bool bFull = true;

  for (idx = 0; idx < bufferNum; idx++) {
    if (pfData[idx].pageNum != -1) {
      // Checking if page is in memory
      // if (pfData[idx].pageNum == pageNum) {
        if (checkPageInMemory(pfData, bm, idx, pageNum) == true) {
          page->pageNum = pageNum;
          bFull = false;
          page->data = pfData[idx].data;
          break;
        }
      // }
    } else {
      initPageInPin(pfData, bm, idx, pageNum);
      page->pageNum = pageNum;
      page->data = pfData[idx].data;
      bFull = false;
      break;
    }
  }

  // If bFull = true, then it means that the buffer is full and we must replace an
  // existing page using page replacement strategy
  if (bFull == true) {
    Frame_Data *newFrame = (Frame_Data *) malloc(sizeof(Frame_Data));

    SM_FileHandle h;
    openPageFile(bm->pageFile, &h);
    newFrame->data = (SM_PageHandle) malloc(PAGE_SIZE);
    readBlock(pageNum, &h, newFrame->data);

    newFrame->pageNum = pageNum;
    newFrame->dirtyFlag = newFrame->rfNum  = 0;
    newFrame->fixCount = 1;
    hit++; rear++;

    if (bm->strategy == RS_LRU)
      newFrame->hitNum = hit;
    else if (bm->strategy == RS_CLOCK)
      newFrame->hitNum = 1;

    page->pageNum = pageNum;
    page->data = newFrame->data;

    pageFrameReplace(newFrame, bm, page, pageNum);
  }
  return RC_OK;
}

/***********************************************************
*                                                          *
*              Statistics Functions.                       *
*                                                          *
***********************************************************/
/**
 *  The function returns an array of PageNumbers (of size numPages) .
 *  An empty page frame is represented using the constant NO PAGE.
 * @param bm
 * @return
 */
PageNumber *getFrameContents(BM_BufferPool *const bm) {
  PageNumber *nums = malloc(sizeof(PageNumber) * bufferNum);
  Frame_Data *pf = (Frame_Data *) bm->mgmtData;
  int i = 0;
  while (i < bufferNum) {
    if (pf[i].pageNum != -1) {
      nums[i] = pf[i].pageNum;
    } else {
      nums[i] = NO_PAGE;
    }
    i++;
  }
  return nums;
}

/**
 * * The getDirtyFlags function returns an array of bools (of size numPages) where the i th element
 * is TRUE if the page stored in the i th page frame is dirty. Empty page frames are considered as clean.
 * @param bm BM_BufferPool
 * @return
 */
bool *getDirtyFlags(BM_BufferPool *const bm) {
  bool *Flags = malloc(sizeof(bool) * bufferNum);
  Frame_Data *pf = (Frame_Data *) bm->mgmtData;

  int i;
  for (i = 0; i < bufferNum; i++) {
    if (pf[i].dirtyFlag == 1) { Flags[i] = true; } else {
      Flags[i] = false;
    }
  }
  return Flags;
}

/**
 * get Fix Counts
 * @param bm
 * @return
 */
int *getFixCounts(BM_BufferPool *const bm) {

  int *fixCounts = malloc(sizeof(int) * bufferNum);
  Frame_Data *pf = (Frame_Data *) bm->mgmtData;

  int j = 0;
  while (j < bufferNum) {
    if (pf[j].fixCount != -1) {
      fixCounts[j] = pf[j].fixCount;
    } else {
      fixCounts[j] = 0;
    }
    j++;
  }
  return fixCounts;
}

int getNumReadIO(BM_BufferPool *const bm) {
  return (rear + 1);
}

int getNumWriteIO(BM_BufferPool *const bm) {
  return writeCount;
}

void writeBlockWithThreadSafe(BM_BufferPool *const bm, Frame_Data *frame, int Index) {
  // Require lock
  pthread_rwlock_init(&thread_lock, NULL);
  pthread_rwlock_wrlock(&thread_lock);

  SM_FileHandle fh;
  openPageFile(bm->pageFile, &fh);
  writeBlock(frame[Index].pageNum, &fh, frame[Index].data);

  // Increase the writeCount which records the number of writes done by the buffer manager.
  writeCount++;

  // Release unlock
  pthread_rwlock_unlock(&thread_lock);
  pthread_rwlock_destroy(&thread_lock);
}


/**
 * FIFO replacement strategy
 * @param bufferPool
 * @param page
 * @param pageNum
 */
void FIFO(BM_BufferPool *const bufferPool, Frame_Data *page, int pageNum) {

  Frame_Data *fp = (Frame_Data *) bufferPool->mgmtData;

  int i;
  int frontIdx = rear % pageNum;

  for (i = 0; i < pageNum; i++) {

    if (fp[frontIdx].fixCount == 0) {
      // If page in memory has been modified (dirtyBit = 1), then write page to disk
      if (fp[frontIdx].dirtyFlag == 1) {
        writeBlockWithThreadSafe(bufferPool, fp, frontIdx);
      }
      /* Setting page frame's content to new page's content */
      fp[frontIdx].data = page->data;
      fp[frontIdx].fixCount = page->fixCount;
      fp[frontIdx].pageNum = page->pageNum;
      fp[frontIdx].dirtyFlag = page->dirtyFlag;
      break;
    } else {
      // If the current page frame is being used by some client, move on to the next location
      frontIdx++;
      if (frontIdx % pageNum == 0) { frontIdx = 0; }
      else { frontIdx = frontIdx; }
    }
  }
}


/**
 * LRU replacement strategy
 * @param bufferPool
 * @param page
 * @param pageNum
 */
void LRU(BM_BufferPool *const bufferPool, Frame_Data *page, int pageNum) {

  if (bufferPool == NULL) return;
  Frame_Data *fr = (Frame_Data *) bufferPool->mgmtData;

  int lstHitIndex = 0, lstHitCnt = 0;
  int i;

  for (i = 0; i < pageNum; i++) {
    /* Finding page frame whose fixCount = 0 */
    if (fr[i].fixCount == 0) {
      lstHitIndex = i;
      lstHitCnt = fr[i].hitNum;
      break;
    }
  }
  /**
   * to find the least recently used, Finding the page frame having minimum hitNum
   */
  for (i = lstHitIndex + 1; i < pageNum; i++) {
    if (fr[i].hitNum < lstHitCnt) {
      lstHitIndex = i;
      lstHitCnt = fr[i].hitNum;
    }
  }

  /**
   * If page in memory has been modified (dirtyBit = 1), then write page to disk
   */
  if (fr[lstHitIndex].dirtyFlag == 1) {
    writeBlockWithThreadSafe(bufferPool, fr, lstHitIndex);
  }

  // Setting page frame's content to new page's content，Considering that there will be simultaneous writes,
  // set thread locks to ensure the synchronization mechanism
  fr[lstHitIndex].data = page->data;
  fr[lstHitIndex].pageNum = page->pageNum;
  fr[lstHitIndex].dirtyFlag = page->dirtyFlag;
  fr[lstHitIndex].fixCount = page->fixCount;
  fr[lstHitIndex].hitNum = page->hitNum;
}

/**
 * CLOCK replacement strategy
 * @param bm Buffer Pool Handler
 * @param page Buffer Page Handler
 * @param pageNum the page number of the requested page
 */
void CLOCK(BM_BufferPool *const bm, Frame_Data *page, int pageNum) {
  if (bm == NULL) return;
  Frame_Data *frame = (Frame_Data *) bm->mgmtData;

  while (true) {

//    clockPtr = (clockPtr % pageNum == 0) ? 0 : clockPtr;
    if (clockPtr % pageNum == 0) {
      clockPtr = 0;
    } else {
      clockPtr = clockPtr;
    }

    if (frame[clockPtr].hitNum == 0) {
      // If page in memory has been modified (dirtyBit = 1), then write page to disk
      if (frame[clockPtr].dirtyFlag == 1) {
        writeBlockWithThreadSafe(bm, frame, clockPtr);
      }
      // Setting page frame's content to new page's content
      frame[clockPtr].pageNum = page->pageNum;
      frame[clockPtr].data = page->data;
      frame[clockPtr].hitNum = page->hitNum;
      frame[clockPtr].dirtyFlag = page->dirtyFlag;
      frame[clockPtr].fixCount = page->fixCount;
      clockPtr++;
      break;
    } else {
      frame[clockPtr++].hitNum = 0;
    }
  }
}

//
//int main(void) {
//
//  BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
//  createPageFile("testbuffer.bin");
//
//  int i, num;
//  initBufferPool(bm, "testbuffer.bin", 30, RS_FIFO, NULL);
//
//  BM_PageHandle *h = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
//
//  num = 10;
//  for (i = 0; i < num; i++) {
//
//    pinPage(bm, h, i);
//    sprintf(h->data, "%s-%i", "Page", h->pageNum);
//
//    markDirty(bm, h);
//
//    unpinPage(bm,h);
//  }
//  free(h);
//
//
//  shutdownBufferPool(bm);
//
//  free(bm);
//
//}