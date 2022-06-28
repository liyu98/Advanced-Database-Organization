#include "storage_mgr.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "test_helper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
char *testName;

// check whether two the content of a buffer pool is the same as an expected content 
// (given in the format produced by sprintPoolContent)
#define ASSERT_EQUALS_POOL(expected, bm, message)                    \
  do {                                    \
    char *real;                                \
    char *_exp = (char *) (expected);                                   \
    real = sprintPoolContent(bm);                    \
    if (strcmp((_exp),real) != 0)                    \
      {                                    \
    printf("[%s-%s-L%i-%s] FAILED: expected <%s> but was <%s>: %s\n",TEST_INFO, _exp, real, message); \
    free(real);                            \
    exit(1);                            \
      }                                    \
    printf("[%s-%s-L%i-%s] OK: expected <%s> and was <%s>: %s\n",TEST_INFO, _exp, real, message); \
    free(real);                                \
  } while(0)

// test and helper methods
static void testCreatingAndReadingDummyPages(void);

static void createDummyPages(BM_BufferPool *bm, int num);

static void checkDummyPages(BM_BufferPool *bm, int num);

static void testReadPage(void);

static void testClock(void);


// main method
int
main(void) {
  initStorageManager();
  testName = "";

  testCreatingAndReadingDummyPages();
  testReadPage();
  testClock();
}

// create n pages with content "Page X" and read them back to check whether the content is right
void
testCreatingAndReadingDummyPages(void) {
  BM_BufferPool *bm = MAKE_POOL();
  testName = "Creating and Reading Back Dummy Pages";

  CHECK(createPageFile("testbuffer.bin"));

  createDummyPages(bm, 22);
  checkDummyPages(bm, 20);

  createDummyPages(bm, 10000);
  checkDummyPages(bm, 10000);

  CHECK(destroyPageFile("testbuffer.bin"));

  free(bm);
  TEST_DONE();
}


void
createDummyPages(BM_BufferPool *bm, int num) {
  int i;
  BM_PageHandle *h = MAKE_PAGE_HANDLE();

  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

  for (i = 0; i < num; i++) {
    CHECK(pinPage(bm, h, i));
    sprintf(h->data, "%s-%i", "Page", h->pageNum);
    CHECK(markDirty(bm, h));
    CHECK(unpinPage(bm, h));
  }

  CHECK(shutdownBufferPool(bm));

  free(h);
}

void
checkDummyPages(BM_BufferPool *bm, int num) {
  int i;
  BM_PageHandle *h = MAKE_PAGE_HANDLE();
  char *expected = malloc(sizeof(char) * 512);

  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

  for (i = 0; i < num; i++) {
    CHECK(pinPage(bm, h, i));

    sprintf(expected, "%s-%i", "Page", h->pageNum);
    ASSERT_EQUALS_STRING(expected, h->data, "reading back dummy page content");

    CHECK(unpinPage(bm, h));
  }

  CHECK(shutdownBufferPool(bm));

  free(expected);
  free(h);
}

void
testReadPage() {
  BM_BufferPool *bm = MAKE_POOL();
  BM_PageHandle *h = MAKE_PAGE_HANDLE();
  testName = "Reading a page";

  CHECK(createPageFile("testbuffer.bin"));
  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

  CHECK(pinPage(bm, h, 0));
  CHECK(pinPage(bm, h, 0));

  CHECK(markDirty(bm, h));

  CHECK(unpinPage(bm, h));
  CHECK(unpinPage(bm, h));

  CHECK(forcePage(bm, h));

  CHECK(shutdownBufferPool(bm));
  CHECK(destroyPageFile("testbuffer.bin"));

  free(bm);
  free(h);

  TEST_DONE();
}

/**
 * Test case for CLOCK replacement strategy
 *
 */
void
testClock(void) {
  // expected results, the poolContents Demo expected results cited the referenced case and tested successfully
  const char *poolContents[] = {

          "[3x0],[-1 0],[-1 0],[-1 0]",
          "[3x0],[2 0],[-1 0],[-1 0]",
          "[3x0],[2 0],[0 0],[-1 0]",
          "[3x0],[2 0],[0 0],[8 0]",
          "[4 0],[2 0],[0 0],[8 0]",
          "[4 0],[2 0],[0 0],[8 0]",
          "[4 0],[2 0],[5 0],[8 0]",
          "[4 0],[2 0],[5 0],[0 0]",
          "[9 0],[2 0],[5 0],[0 0]",
          "[9 0],[8 0],[5 0],[0 0]",
          "[9 0],[8 0],[3x0],[0 0]",
          "[9 0],[8 0],[3x0],[2 0]"
  };
  const int orderRequests[] = {3, 2, 0, 8, 4, 2, 5, 0, 9, 8, 3, 2};

//  const char *poolContents[] = { "[0 0],[-1 0],[-1 0]", // request Page-0
//                                 "[0 0],[1 0],[-1 0]", // request Page-1
//                                 "[0 0],[1 0],[2 0]", // request Page-2
//                                 "[3 0],[1 0],[2 0]", // request Page-3
//                                 "[3 0],[4 0],[2 0]", // request Page-4
//                                 "[3 0],[4 1],[2 0]", // request Page-4
//                                 "[3 0],[4 1],[5x0]", // request Page-5
//                                 "[6x0],[4 1],[5x0]", // request Page-6
//                                 "[6x0],[4 1],[0x0]", // request Page-0
//                                 "[6x0],[4 0],[0x0]", // unpin Page-4
//                                 "[6 0],[4 0],[0 0]" // forceFlushPool()
//  };
//
//  const int requests[] = { 0, 1, 2, 3, 4, 4, 5, 6, 0 };

  int i;
  int snapshot = 0;
  BM_BufferPool *bm = MAKE_POOL();
  BM_PageHandle *h = MAKE_PAGE_HANDLE();
  testName = "Testing CLOCK page replacement";

  CHECK(createPageFile("testbuffer.bin"));
  createDummyPages(bm, 110);
  CHECK(initBufferPool(bm, "testbuffer.bin", 4, RS_CLOCK, NULL));

  int numberofcycles = 11;
  for (i = 0; i < numberofcycles; i++) {
    pinPage(bm, h, orderRequests[i]);
    if (orderRequests[i] == 3)
      markDirty(bm, h);
    unpinPage(bm, h);
    ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "check pool content using pages");
  }

  forceFlushPool(bm);
  // check number of write IOs
  ASSERT_EQUALS_INT(2, getNumWriteIO(bm), "check number of write I/Os");
  ASSERT_EQUALS_INT(10, getNumReadIO(bm), "check number of read I/Os");

  CHECK(shutdownBufferPool(bm));
  CHECK(destroyPageFile("testbuffer.bin"));

  free(bm);
  free(h);
  TEST_DONE();
}