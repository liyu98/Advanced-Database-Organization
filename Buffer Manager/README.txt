## Introduction:
The main motive of this assignment is to implement a buffer manager which manages a fixed number of pages in the menory that represent pages from the page file managed by the storage manager implemented in the previous assignment. The bummer manager should be capable of handeling more than one open buffer pool at the same time. There should be the implementation of at lease 2 replacement stratigies FIFO and LRU.

##Directory Structure:
dberror.c
dberror.h
Makefile
buffer_mgr.h
buffer_mgr.c
dt.h
buffer_mgr_stat.h
storage_mgr.c
storage_mgr.h
test_assign2_1.c
test_assign2_2.c
test_helper.h
rm_data_structures.h
    
## Requirements
[GNU GCC Compiler](https://gcc.gnu.org/)

###  function introduction
initBufferPool - Here we use this function to create and initialize a new buffer pool using pageframes(numPages) and page replacement strategy.
shutdownBufferPool - Here we use this function to destroy the Buffer pool. 
forceFlushPool - This function is used to write all dirty pages from the buffer pool to the disk with fix count 0. 

### Functions for Buffer Manager Interface Access Pages
markDirty - This function is used to mark the page as dirty. It sets the dirtybit of the page to 1. 
unpinPage - This function is used to remove the page from the buffer pool.
forcePage - This function is used to write the current content of the page to the page file on the disk. 
pinPage - This function pins a page with a page number(pageNum). 

### Function for Statistics Interface
*getFrameContents - This function creats an array of pagenumber (of size pageNum) where the ith element is the number of page stored in the ith page frame. 

*getDirtyFlags - This function returns an array of booleans (of size pageNum) where the ith element is TRUE if the page stored in the ith page frame is dirty.

*getFixCounts - This function returns an array of int (of size pageNum) where the ith element is the fix count of the page stored in the ith page frame.

getNumReadIO - This function returns the number of pages that have been read to page file since the initialization of buffer pool.

getNumWriteIO - This function returns the number of pages that have been written to page file since the initialization of buffer pool.

### Extensions：Have implemented these optional extensions

* Make buffer pool functions thread-safe：Use pthread_rwlock_t read-write thread lock to ensure that reads and writes to blocks are thread-safe.
* Implement CLOCK page replacement strategies.


### Test Cases:
testCreatingAndReadingDummyPages - Here in this function a dummy page is created and read and once this is done the page is destroyed. 
createDummyPages - Here in this functhon dummy pages are created. 
checkDummyPages - Here this function is used to read dummy pages page by page and then unpinned and also the function checks the dummy pages. 
testReadPage - Here this function pins the page that is to be read and then after reading it unpins the page and destroys it. 
testFIFO - Here this function pins the page and the contents of the page are checked and then unpinned. Then a force flush is done after the last page is unpinned. It also checks the number of read and write inputs. At last when everyting is dont the buffer pool is shut down and the testing is complete. 
testLRU - Here this function reads the first 5 pages and then directly unpins them assuming no modifications are made. The pages are read to change the order of the LRU. Then the pages are replaced and checks that it happens in LRU order.
testError - Here this function requests an additional file after the pinpage of bufferpools is done and later on destroys the buffer pool then it try to pinpage with negative pagenumber before destroying the buffer pool. For the uninitialized buffer pool the shutdown buffer pool, flush buffer pool and pinpage is done for the unopened files. Lastly it unpins, force flush and make dirty that is not in the pool and then removes the page file.
additional - testClock;

#### There are two test cases.
At the project's directory run following command on terminal:
1. with default test case:
Compile : make test_assign2_1
Run : ./test_assign2_1

2. With additional test cases:
Compile : make test_assign2_2
Run : ./test_assign2_2

3. revert:
make clean

4. Compile All:
Run : make all

### Note
1. Implemented additional replacement strategy CLOCK/Second Chance.
2. Additional test cases for CLOCK replacement strategy is added in ```test_assign2_2.c``` file.
To test the expected result,the poolContents Demo expected results cited the referenced case and tested successfully
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
3. Added additional method writeBlockWithThreadSafe to ensure thread safety.

### References
1. https://stackoverflow.com/questions/5833094/get-a-timestamp-in-c-in-microseconds
2. https://stackoverflow.com/questions/39625579/c-overflow-in-implicit-constant-conversion-woverflow/39625680
3. https://cs.stackexchange.com/questions/24011/clock-page-replacement-algorithm-already-existing-pages
4. https://www.geeksforgeeks.org/lru-cache-implementation/




