#include <stdlib.h>
#include <limits.h>
#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

/***********************************************************
*                                                          *
*              Buffer Pool Data                            *
*                                                          *
***********************************************************/

/* the record holds information about page frame loaded into buffer pool */
typedef struct pageFrame {
    SM_PageHandle pageHandle;
    PageNumber pageNum;  /* the pageNum of page in the pageFile*/
    bool isDirty;        /* dirty flag: TRUE|FALSE  */
    int fixCount;        /* fixCound of the page based on the pinning/un-pinning request*/
    int rfNum;           /* reference bit per frame to be used by clock */
    int hitNum;          /* used by LRU to get the least recently used page */
    int *lru_array;      /* array to be used by LRU-K */

} pageFrame;

/* fixed count of all the frames.*/
int *fixCounts;

/* dirtyflags of all the frames */
bool *dirtyFlags;

/* pageNums of all the frames*/
PageNumber *pageNums;

/* number of read done on the buffer pool */
int numRead;

/* number of write done on the buffer pool */
int numWrite;

/*  Variables for each page replacement strategy */
int frontIndex, rearIndex, K, clock, bufferSize, hitCount;


/***********************************************************
*                                                          *
*              Page Replacement Strategy                   *
*                                                          *
***********************************************************/

/*
 *  FIFO Strategy
 */
void FIFO(BM_BufferPool *const bm, pageFrame *pFrame) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;
  int i;

  /* if the page already exists, increment its fixCount */
  for (i = 0; i < bm->numPages; i++) {
    if (pf[i].pageNum == pFrame->pageNum) {
      pf[i].fixCount++;
      return;
    }
  }

  for (i = 0; i < bm->numPages; i++) {
    /* First available slot is determined by iterating through the page table and
     * finding the first entry that has pageHandleInmemory pageNum = -1 which implies
     * that frame is not having any page */
    if (pf[i].pageNum == NO_PAGE) {
      rearIndex++;
      pf[rearIndex].pageHandle = pFrame->pageHandle;
      pf[rearIndex].pageNum = pFrame->pageNum;
      pf[rearIndex].isDirty = pFrame->isDirty;
      pf[rearIndex].fixCount = pFrame->fixCount;
      return;
    }
  }

  for (int j = 0; j < bm->numPages; j++) {
    if (pf[frontIndex].fixCount == 0) {
      /* Write content back to the disk if dirty bit is set */
      if (pf[frontIndex].isDirty == TRUE) {
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh);
        writeBlock(pf[frontIndex].pageNum, &fh, pf[frontIndex].pageHandle);
        closePageFile(&fh);
        /* Increase the writeCount which records the number of writes done by the buffer manager. */
        numWrite++;
      }

      /* Setting page frame's content to new page's content  */
      pf[frontIndex].pageHandle = pFrame->pageHandle;
      pf[frontIndex].pageNum = pFrame->pageNum;
      pf[frontIndex].isDirty = pFrame->isDirty;
      pf[frontIndex].fixCount = pFrame->fixCount;
      frontIndex++;
      frontIndex = (frontIndex % bm->numPages == 0) ? 0 : frontIndex;
      break;
    } else {
      frontIndex++;
      frontIndex = (frontIndex % bm->numPages == 0) ? 0 : frontIndex;
    }
  }
}

/*
 *  LRU Strategy
 *  The K in LRU-K represents the number of recent uses, so the LRU can be considered as LRU-1.
 *  The main purpose of LRU-K is to solve the problem of "cache pollution" of the LRU algorithm.
 *  Its core idea is to extend the criterion of "used 1 time recently" to "used K times recently".
 */
void LRU_K(BM_BufferPool *const bm, pageFrame *pFrame) {

  pageFrame *pf = (pageFrame *) bm->mgmtData;

  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].pageNum == NO_PAGE) {
      pf[i].pageHandle = pFrame->pageHandle;
      pf[i].pageNum = pFrame->pageNum;
      pf[i].isDirty = pFrame->isDirty;
      pf[i].fixCount = pFrame->fixCount;
      pf[i].lru_array[0] = hitCount;
      return;
    }
  }

  /* find the page with lowest Kth accessed time (least recently accessed) */
  int LRU_index = -1, LRU_hitNum = hitCount;
  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].fixCount == 0 && pf[i].lru_array[K - 1] < LRU_hitNum) {
      LRU_index = i;
      LRU_hitNum = pf[i].lru_array[K - 1];
    }
  }

  if (LRU_index != -1) {
    if (pf[LRU_index].isDirty == TRUE) {
      SM_FileHandle fh;
      openPageFile(bm->pageFile, &fh);
      writeBlock(pf[LRU_index].pageNum, &fh, pf[LRU_index].pageHandle);
      closePageFile(&fh);
      numWrite++;
    }
    pf[LRU_index].pageHandle = pFrame->pageHandle;
    pf[LRU_index].pageNum = pFrame->pageNum;
    pf[LRU_index].isDirty = pFrame->isDirty;
    pf[LRU_index].fixCount = pFrame->fixCount;

    for (int i = 1; i < K; i++) {
      pf[LRU_index].lru_array[i] = 0;
    }
    pf[LRU_index].lru_array[0] = hitCount;
  }
}

/*
 *  LRU Strategy
 *
 */
void LRU(BM_BufferPool *const bm, pageFrame *pFrame) {

  pageFrame *pf = (pageFrame *) bm->mgmtData;

  int i, leastHitIndex = 0, leastHitNum = 0;

  /* Interating through all the page frames in the buffer pool. */
  for (i = 0; i < bm->numPages; i++) {
    /* Finding page frame whose fixCount = 0 i.e. no client is using that page frame. */
    if (pf[i].fixCount == 0) {
      leastHitIndex = i;
      leastHitNum = pf[i].hitNum;
      break;
    }
  }

  /* Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame */
  for (i = leastHitIndex + 1; i < bufferSize; i++) {
    if (pFrame[i].hitNum < leastHitNum) {
      leastHitIndex = i;
      leastHitNum = pFrame[i].hitNum;
    }
  }

  /* If page in memory has been modified (dirtyBit = 1), then write page to disk */
  if (pFrame[leastHitIndex].isDirty == TRUE) {
    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    writeBlock(pf[leastHitIndex].pageNum, &fh, pf[leastHitIndex].pageHandle);

    // Increase the writeCount which records the number of writes done by the buffer manager.
    numWrite++;
  }

  /* Setting page frame's content to new page's content */
  pf[leastHitIndex].pageHandle = pFrame->pageHandle;
  pf[leastHitIndex].pageNum = pFrame->pageNum;
  pf[leastHitIndex].isDirty = pFrame->isDirty;
  pf[leastHitIndex].fixCount = pFrame->fixCount;
  pf[leastHitIndex].hitNum = pFrame->hitNum;
}

/*
 *  CLOCK Strategy
 *
 */
void CLOCK(BM_BufferPool *const bm, pageFrame *page) {

  pageFrame *pf = (pageFrame *) bm->mgmtData;
  while (1) {

//    clock %= bm->numPages;
    clock = (clock % bm->numPages == 0) ? 0 : clock;
//    if (pf[clock].rfNum == 0 && pf[clock].fixCount == 0) {
    if (pf[clock].rfNum == 0) {

      /* If page in memory has been modified (dirtyBit = 1), then write page to disk */
      if (pf[clock].isDirty == TRUE) {
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh);
        writeBlock(pf[clock].pageNum, &fh, pf[clock].pageHandle);
        /* Increase the writeCount which records the number of writes done by the buffer manager. */
        closePageFile(&fh);
        numWrite++;
      }

      pf[clock].pageHandle = page->pageHandle;
      pf[clock].pageNum = page->pageNum;
      pf[clock].isDirty = page->isDirty;
      pf[clock].fixCount = page->fixCount;
      pf[clock].rfNum = page->rfNum;
      clock++;
      break;
    } else {

      /* Incrementing clockPointer so that we can check the next page frame location.
       * it set hitNum = 0 so that this loop doesn't go into an infinite loop.
       */
      pf[clock++].rfNum = 0;
    }
  }
}

/*
 *  LFU Strategy
 *  The LFU algorithm eliminates data based on its historical access frequency.
 *  The core idea is that "if data has been accessed many times in the past, it will be accessed more
 *  frequently in the future."
 */
void LFU(BM_BufferPool *const bm, pageFrame *page) {

  pageFrame *pf = (pageFrame *) bm->mgmtData;

  /* Interating through all the page frames in the buffer pool */
  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].pageNum == NO_PAGE) {
      pf[i].pageHandle = page->pageHandle;
      pf[i].pageNum = page->pageNum;
      pf[i].isDirty = page->isDirty;
      pf[i].fixCount = page->fixCount;
      pf[i].rfNum = 1;
      pf[i].lru_array[0] = hitCount;
      return;
    }
  }

  // go through all the pages and find the page with lowest hitNum (least frequently used)
  // in case of a tie, choose the one which was least recently accessed
  int LFU_index = -1, LFU_hitNum = INT_MAX;
  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].fixCount == 0) {
      if (pf[i].rfNum < LFU_hitNum || (pf[i].rfNum == LFU_hitNum && pf[i].lru_array[0] < pf[LFU_index].lru_array[0])) {
        LFU_index = i;
        LFU_hitNum = pf[i].rfNum;
      }
    }
  }
  if (LFU_index != -1) {
    /* If page in memory has been modified, then write page to disk	*/
    if (pf[LFU_index].isDirty == TRUE) {
      SM_FileHandle fh;
      openPageFile(bm->pageFile, &fh);
      writeBlock(pf[LFU_index].pageNum, &fh, pf[LFU_index].pageHandle);
      closePageFile(&fh);
      numWrite++;
    }

    /* Setting page frame's content to new page's content */
    pf[LFU_index].pageHandle = page->pageHandle;
    pf[LFU_index].pageNum = page->pageNum;
    pf[LFU_index].isDirty = page->isDirty;
    pf[LFU_index].fixCount = page->fixCount;
    pf[LFU_index].rfNum = 1;
    pf[LFU_index].lru_array[0] = hitCount;
  }
}

/***********************************************************
*                                                          *
*              Buffer Pool Functions                       *
*                                                          *
***********************************************************/

/*
 * creates a new buffer pool with numPages page frames using the page replacement
 * strategy strategy. The pool is used to cache pages from the page file with name pageFileName.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {

  SM_FileHandle fh;

  RC result = openPageFile(pageFileName, &fh);
  if (result != RC_OK) {
    return RC_FILE_NOT_FOUND;
  }

  frontIndex = 0;
  rearIndex = -1;
  clock = 0;
  hitCount = 0;
  bufferSize = 0;
  numWrite = 0;
  numRead = 0;

  //K = 1;
  if (stratData == NULL) {
    K = 1;
  } else {
    // There may be exceptions
    K = *((int *) (stratData));
  }

  bm->pageFile = (char *) pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;

  // allocate memory
  pageFrame *pf = malloc(numPages * sizeof(pageFrame));

  // Intilalizing all pages in buffer pool. The values of fields (variables) in the page is either NULL or 0.
  for (int i = 0; i < numPages; i++) {
    pf[i].pageHandle = NULL;
    pf[i].pageNum = NO_PAGE;
    pf[i].isDirty = 0;
    pf[i].fixCount = 0;
    pf[i].lru_array = (int *) malloc(K * sizeof(int));
  }
  bm->mgmtData = pf;
  return RC_OK;
}

/*
 * Destroys the buffer pool. This method should free up all resources associated with buffer pool.
 *  it should free the memory allocated for page frames. If the buffer pool contains any dirty pages,
 *  then these pages should be written back to disk before destroying the pool. It is an error to shutdown
 *  a buffer pool that has pinned pages.
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
  if (!bm->mgmtData) {
    return RC_NON_EXIST_BUFFERPOOL;
  }
  pageFrame *pf = (pageFrame *) bm->mgmtData;

  // 这个需要处理异常，然后能正常完成
  // return error if trying to shutdown while there are pinned pages
  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].fixCount != 0) {
      return RC_PINNED_PAGES_IN_BUFFER;
    }
  }
  /* write all dirty pages before shutting down */
  forceFlushPool(bm);
  /* free allocated pages */
  free(pf);
  bm->mgmtData = NULL;

  return RC_OK;
}

/*
 * the function causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
 */
RC forceFlushPool(BM_BufferPool *const bm) {
  if (bm->mgmtData == NULL) {
    return RC_NON_EXIST_BUFFERPOOL;
  }

  pageFrame *pf = (pageFrame *) bm->mgmtData;

  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].fixCount == 0 && pf[i].isDirty == TRUE) {
      SM_FileHandle fh;
      openPageFile(bm->pageFile, &fh);
      writeBlock(pf[i].pageNum, &fh, pf[i].pageHandle);
      closePageFile(&fh);
      pf[i].isDirty = FALSE;
      numWrite++;
    }
  }
  return RC_OK;
}

/***********************************************************
*                                                          *
*              Page Management Functions.                  *
*                                                          *
***********************************************************/

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;

  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].pageNum == page->pageNum) {
      pf[i].isDirty = TRUE;
      return RC_OK;
    }
  }
  return RC_READ_NON_EXISTING_PAGE;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;

  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].pageNum == page->pageNum) {
      pf[i].fixCount--;
      return RC_OK;
    }
  }
  return RC_READ_NON_EXISTING_PAGE;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;

  for (int i = 0; i < bm->numPages; i++) {
    if (pf[i].pageNum == page->pageNum) {
      SM_FileHandle fh;
      openPageFile(bm->pageFile, &fh);
      writeBlock(pf[i].pageNum, &fh, pf[i].pageHandle);
      closePageFile(&fh);
      pf[i].isDirty = FALSE;

      numWrite++;
      return RC_OK;
    }
  }

  return RC_READ_NON_EXISTING_PAGE;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {
  if (bm->mgmtData == NULL) {
    return RC_NON_EXIST_BUFFERPOOL;
  }
  if (pageNum < 0) {
    return RC_READ_NON_EXISTING_PAGE;
  }
  pageFrame *pf = (pageFrame *) bm->mgmtData;
  hitCount++;

  /* check if the page already exists, if so -> increment its fixCount and update the hitNum */
  for (int i = 0; i < bm->numPages; i++) {
    // Checking if page is in memory
    if (pf[i].pageNum == pageNum) {
      pf[i].fixCount++;
      switch (bm->strategy) {
        case RS_LRU_K: {
          // shift the accesses to the right
          for (int j = K - 1; j > 0; j--) {
            pf[i].lru_array[j] = pf[i].lru_array[j - 1];
          }
        }
        case RS_LFU:
          pf[i].rfNum++;
      }
      pf[i].lru_array[0] = hitCount;
      page->pageNum = pageNum;
      clock++;
      return RC_OK;
    }
  }
  int pin_cnt = 0;
  for (int j = 0; j < bm->numPages; j++) {
    if (pf[j].fixCount != 0) {
      pin_cnt++;
    }
  }
  if (pin_cnt == bm->numPages) {
    return RC_REPLACE_WHILE_PINNED_PAGES;
  }

  pageFrame *newPage = (pageFrame *) malloc(sizeof(pageFrame));

  SM_FileHandle fh;
  openPageFile(bm->pageFile, &fh);
  newPage->pageHandle = (SM_PageHandle) malloc(PAGE_SIZE);
  ensureCapacity(pageNum + 1, &fh);

  readBlock(pageNum, &fh, newPage->pageHandle);
  newPage->pageNum = pageNum;
  newPage->isDirty = 0;
  newPage->fixCount = 1;
  page->pageNum = pageNum;
  page->data = newPage->pageHandle;
  numRead++;

  closePageFile(&fh);

  switch (bm->strategy) {
    case RS_FIFO: // Using FIFO algorithm
      FIFO(bm, newPage);
      break;
    case RS_LRU: // Using LRU algorithm, LRU is simply LRU_K with K=1
      LRU_K(bm, newPage);
      break;

    case RS_CLOCK: // Using CLOCK algorithm
      CLOCK(bm, newPage);
      break;

    case RS_LFU: // Using LFU algorithm
      LFU(bm, newPage);
      break;

    case RS_LRU_K:
      LRU_K(bm, newPage);
      break;

    default:
      printf("\nNone\n");
      break;
  }
  return RC_OK;
}

/***********************************************************
*                                                          *
*              Statistics Functions.                       *
*                                                          *
***********************************************************/
/*
 *  The function returns an array of PageNumbers (of size numPages) .
 *  An empty page frame is represented using the constant NO PAGE.
 */
PageNumber *getFrameContents(BM_BufferPool *const bm) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;
  pageNums = (PageNumber *) malloc(sizeof(PageNumber) * bm->numPages);

  int i = 0;
  while (i < bm->numPages) {
    pageNums[i] = pf[i].pageNum;
    i++;
  }
  return pageNums;
}

/*
 * The getDirtyFlags function returns an array of bools (of size numPages) where the i th element
 * is TRUE if the page stored in the i th page frame is dirty. Empty page frames are considered as clean.
 */
bool *getDirtyFlags(BM_BufferPool *const bm) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;
  dirtyFlags = (bool *) malloc(sizeof(bool) * bm->numPages);

  int i = 0;
  while (i < bm->numPages) {
    dirtyFlags[i] = pf[i].isDirty;
    i++;
  }
  return dirtyFlags;
}

/* iterate all the frames, update the value of fix count, then return result. */
int *getFixCounts(BM_BufferPool *const bm) {
  pageFrame *pf = (pageFrame *) bm->mgmtData;
  fixCounts = (int *) malloc(sizeof(int) * bm->numPages);

  int i = 0;
  while (i < bm->numPages) {
    fixCounts[i] = pf[i].fixCount;
    i++;
  }

  return fixCounts;
}

/* The getNumReadIO function returns the number of pages that have been read from disk since a buffer pool has
 * been initialized.
 */
int getNumReadIO(BM_BufferPool *const bm) {
  return numRead;
}

/* returns the number of pages written to the page file since the buffer pool has been initialized */
int getNumWriteIO(BM_BufferPool *const bm) {
  return numWrite;
}
