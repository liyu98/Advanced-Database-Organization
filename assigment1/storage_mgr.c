#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#include "storage_mgr.h"

/*
 * The result code in the function/method, avoid repeated definitions,need to be initialized.
 */
RC resultCode;

/*
 * Initiate Storage Manager：
 * Complete variable initialization settings.
 */
void initStorageManager(void) {
  printf("Programming Assignment I: Storage Manager \n");
  printf("Group7 Member: DONGKAI QI, YU LI, ZHENYA ZHANG \n");
  printf("Sucessfully Initate Storage Manager >>> \n");

  resultCode = -1;
}

/***********************************************************
*                                                          *
*              Page File Related Methods.                  *
*                                                          *
***********************************************************/
/*
 * Create a new page ﬁle fileName. The initial ﬁle size should be one page. This method should ﬁll this
 * single page with ’\0’ bytes.
 *
 */
RC createPageFile(char *fileName) {
  /* Check if the filename is null or empty */
  if ((NULL == fileName) || (strlen(fileName) == 0)) {
    return RC_FILE_NAME_EMPTY;
  }
  /* Check whether the file exists */
  FILE *file = fopen(fileName, "rb");
  if (file) {
    printf("File exist.\n");
    fclose(file);
    return RC_FILE_EXIST;
  } else {
    /* Create binary file */
    file = fopen(fileName, "wb");
    if (!file) {
      printf("File create failed.\n");
      return RC_FILE_CREATE_FAILED;
    }
  }
  /* Allocate one page space with the initial value '\0' if use malloc method, need memset */
  char *buff = (char *) calloc(PAGE_SIZE, sizeof(char));
  resultCode = -1;
  if (fwrite(buff, sizeof(char), PAGE_SIZE, file) == 0) {
    /* Write the block of content into the file. If could not write into the file return WRITE_FAILED ERROR. */
    printf("File write failed.\n");
    resultCode = RC_WRITE_FAILED;
  } else {
    resultCode = RC_OK;
  }
  free(buff);
  fclose(file);
  return resultCode;
}

/*
 * Opens an existing page ﬁle. Should return RC FILE NOT FOUND if the ﬁle does not exist.
 * If opening the ﬁle is successful, then the ﬁelds of this ﬁle handle should be initialized
 * with the information about the opened ﬁle.
*/
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
  /* Check if the filename is null or empty */
  if ((NULL == fileName) || (strlen(fileName) == 0)) {
    return RC_FILE_NOT_FOUND;
  }
  /* open the page file */
  FILE *file = fopen(fileName, "rb+");
  if (NULL == file) {
    resultCode = RC_FILE_NOT_FOUND;
  }
  else {
    /* Set the position of the end of file */
    fseek(file, 0L, SEEK_END);
    /* get file size, length from the tail to the head of the page file is equal bytes of the page file. */

    // struct stat st;
    // if(stat(fileName,&st)!=0){
    //   return RC_FILE_HANDLE_NOT_INIT;
    // }
    // int totalNumPages = st.st_size;

    long filesize = ftell(file);
    if (filesize == -1) {
      /* if failed to fetch the offset, return failed. */
      resultCode = RC_FILE_HANDLE_NOT_INIT;
    }
    else {
      /* init file handle with opened file information */
      fHandle->fileName = fileName;
      fHandle->totalNumPages = (int) (filesize / PAGE_SIZE);
      fHandle->curPagePos = 0;
      fHandle->mgmtInfo = file;
      resultCode = RC_OK;
    }
  }
  return resultCode;
}

/*
 * Close the open page file
 */
RC closePageFile(SM_FileHandle *fHandle) {
  // check that the file handle exists
  if (NULL== fHandle)
    return RC_FILE_NOT_FOUND;

  FILE *fp = (FILE *) fHandle->mgmtInfo;
  if (!fHandle->mgmtInfo)
    return RC_FILE_HANDLE_NOT_INIT;

  return fclose(fp) != 0 ? RC_FILE_NOT_FOUND : RC_OK;;
}

/*
 *  destroy (delete) a page file.
 */
RC destroyPageFile(char *fileName) {
  /* Check if the filename is null or empty */
  if ((NULL == fileName) || (strlen(fileName) == 0)) {
    return RC_FILE_NOT_FOUND;
  }

  /* delete file */
  if (remove(fileName) == 0) {
    return RC_OK;
  } else {
    printf("file deleted fail./n");
    return RC_FAILED_DEL_FAIL;
  }
}

/***********************************************************
*                                                          *
*            Blocks of Page File Related Methods.          *
*              (read/write Block)                          *
*                                                          *
***********************************************************/

/*
 * The method reads the block at position pageNum from a file and stores its content in the memory pointed
 * to by the memPage page handle.If the file has less than pageNum pages, the method should return
 * RC_READ_NON_EXISTING_PAGE.
*/

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

  /* check if the file handle pointer exists */
  if (NULL == fHandle) {
    return RC_FILE_HANDLE_NOT_INIT;
  }

  if (NULL == fHandle->mgmtInfo) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  /* PageNum is not in the scope of the available pages  */
  if (pageNum > fHandle->totalNumPages || pageNum < 0) {
    return RC_READ_NON_EXISTING_PAGE;
  }

  resultCode = fseek(fHandle->mgmtInfo, ((pageNum) * PAGE_SIZE), SEEK_SET);
  if (resultCode == 0) {

    // reading to requested page.
    int size = fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    if (size == PAGE_SIZE){
      fHandle->curPagePos = pageNum;
      resultCode = RC_OK;
    }
  }

  return  resultCode;
}

/*
 * Write a page to disk using an absolute position.
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

  /* check if the file handle pointer exists */
  if (!fHandle) {
    return RC_FILE_HANDLE_NOT_INIT;
  }

  if (!fHandle->mgmtInfo){
    return RC_FILE_HANDLE_NOT_INIT;
  }

  if (pageNum < 0){
    return RC_FILE_HANDLE_NOT_INIT;
  }

  /* checking the file capacity, expands the file if necessary to write at pageNum */
  if ((resultCode = ensureCapacity(pageNum, fHandle)) != RC_OK){
    printf("check file capacity fail./n");
    return resultCode;
  }

  /* change the pointer to the correct page, update current page position based on fseek */
  if (fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET) != 0){
    return RC_FILE_HANDLE_NOT_INIT;
  }

  fHandle->curPagePos = pageNum;
  /* writes memPage to the file and Force the data in the buffer to be written back to the file to avoid data loss */
  if (fwrite(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo) != 1 || fflush(fHandle->mgmtInfo) != 0){
    return RC_WRITE_FAILED;
  }

  return RC_OK;
}

/*
 * If the file has less than numberOfPages pages then increase the size to numberOfPages.
 */
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

  if (NULL == fHandle) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  /* checking the capacity, calculating number of pages to add */
  if (fHandle->totalNumPages >= numberOfPages) {
    return RC_OK;
  } else {
    int i, num_Add;
    num_Add = numberOfPages - fHandle->totalNumPages;
    for (i = 0; i < num_Add; i++) {
      /* call to append empty block */
      appendEmptyBlock(fHandle);
    }
    return RC_OK;
  }
}

/*
 * Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
 */
RC appendEmptyBlock(SM_FileHandle *fHandle) {

  FILE *file = fHandle->mgmtInfo;

  fseek(file, 0L, SEEK_END);
  resultCode = -1;
  /* allocate the empty space with the initial value '\0'. */
  char *buff = (char *)calloc(PAGE_SIZE, sizeof(char));

  if (fwrite(buff, sizeof(char), PAGE_SIZE, file) == 0)
    resultCode = RC_WRITE_FAILED;
  else {
    /* totalNumPages & curPagePos Matching appears */
    fHandle->totalNumPages += 1;
    fHandle->curPagePos = fHandle->totalNumPages;
    resultCode = RC_OK;
  }
  free(buff);

  return resultCode;

}

/*
 * Return the current page position in a file.
 * Note: This is a logical location positioning.
 */
int getBlockPos(SM_FileHandle *fHandle) {
  if (!fHandle) {
    printf("desired file related data is not initialized./n");
    return RC_FILE_HANDLE_NOT_INIT;
  }
  return fHandle->curPagePos;
}

/*
 * Read the first block in the file
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

  int index = 0;
  resultCode = readBlock(index, fHandle, memPage);
  if (resultCode == RC_OK){
    fHandle->curPagePos = 0;
  }
  return resultCode;
}

/*
 * Read the previous page relative to the curPagePos of the file.
 * The curPagePos should be moved to the page that was read.
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos - 1;
  resultCode = readBlock(pageNum, fHandle, memPage);
  if (resultCode == RC_OK){
    fHandle->curPagePos = pageNum;
  }

  return resultCode;
}

/*
 * Read the current page relative to the curPagePos of the file.
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->curPagePos, fHandle, memPage);
}

/*
 * Read the next page relative to the curPagePos of the file.
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos + 1;
  resultCode = readBlock(pageNum, fHandle, memPage);
  if (resultCode == RC_OK){
    fHandle->curPagePos = pageNum;
  }

  return resultCode;
}

/*
 * Read the last page relative to the curPagePos of the file.
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->totalNumPages - 1;
  resultCode = readBlock(pageNum, fHandle, memPage);
  if (resultCode == RC_OK) {
    fHandle->curPagePos = pageNum;
  }

  return resultCode;
}

/*
 * Write a page to disk using the current position.
 */
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return writeBlock(fHandle->curPagePos, fHandle, memPage);
}




//
//int main() {
//  initStorageManager();
////  createPageFile("test1.bin");
//
//  SM_FileHandle fh;
//  int result = openPageFile("test1.bin", &fh);
//  printf("结果为 %d\n", result);
//  result = closePageFile(&fh);
//  printf("结果为 %d\n", result);
//
//  writeCurrentBlock(&fh, NULL);
//
//  readFirstBlock(&fh, NULL);
//
//  readPreviousBlock(&fh, NULL);
//
//
//  return 0;
//}
