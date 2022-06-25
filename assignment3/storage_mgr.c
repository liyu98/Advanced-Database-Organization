#include<stdio.h>
#include<stdlib.h>
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
//  printf("Programming Assignment III: Record Manager \n");
//  printf("Group7 Member: DONGKAI QI, YU LI, ZHENYA ZHANG \n");
//  printf("Sucessfully Initate Storage Manager >>> \n");

  resultCode = -1;
}

/***********************************************************
*                                                          *
*              Page File Related Methods.                  *
*                                                          *
***********************************************************/
/*
 * Create a new page ﬁle fileName. The initial ﬁle size should be one page. This method should ﬁll this
 * single page with ’\0’ bytes. The file data structures:
 * ｜MataData 4｜Page1 4096｜Page2 4096｜Page3 4096｜...|PageN 4096|
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
    file = fopen(fileName, "wb+");
    if (!file) {
      printf("File create failed.\n");
      return RC_FILE_CREATE_FAILED;
    }
  }

  /* Write MataData with initial value 1，Assignment 2 modifies the structure and removes  */
  int *size;
  int numPages = 1;
//  size = &numPages;
//  fwrite(size,sizeof(int),1,file);
//  fseek(file, MetaData_Size, SEEK_SET);

  /* Allocate one page space with the initial value '\0' if use malloc method, need memset */
  SM_PageHandle *pagebuffer = (SM_PageHandle *) calloc(PAGE_SIZE, sizeof(char));

  /* Write page to the file */
  resultCode = fwrite(pagebuffer, PAGE_SIZE, 1, file);

  /* Free the allocated memory */
  free(pagebuffer);

  if (resultCode < 1) {
    /* Write the block of content into the file. If could not write into the file return WRITE_FAILED ERROR. */
    printf("File write failed.\n");
    return RC_WRITE_FAILED;
  }

  resultCode = fclose(file);
  if (resultCode != 0) {
    return RC_FILE_CLOSE_FAILED;
  }

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
  FILE *file = fopen(fileName, "r+");
  if (NULL == file) {
    resultCode = RC_FILE_NOT_FOUND;
  }
  /* return error code if if file seeks unsuccessful */
  resultCode = fseek(file, 0, SEEK_END);
  if (resultCode != 0) {
    return RC_FILE_SEEK_ERROR;
  }

  // Get fileSize
  int fileSize = ftell(file);
  int num = fileSize / PAGE_SIZE;

  resultCode = fseek(file, 0, SEEK_SET);
  if (resultCode != 0) {
    return RC_FILE_SEEK_ERROR;
  }

  fHandle->curPagePos = 0;
  fHandle->fileName = fileName;
  fHandle->totalNumPages = num;
  fHandle->mgmtInfo = file;

  return RC_OK;
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


  // Close the file
  resultCode = fclose(fHandle->mgmtInfo);

  // Return error code if file wasn't closed successfully
  if (resultCode != 0) {
    return RC_FILE_CLOSE_FAILED;
  }
  return resultCode;
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
  if (fHandle->totalNumPages < (pageNum + 1) || pageNum < 0) {
    return RC_READ_NON_EXISTING_PAGE;
  }

  int offset = sizeof(char) * pageNum * PAGE_SIZE;
  resultCode = fseek(fHandle->mgmtInfo, offset, SEEK_SET);
  if (resultCode != 0) {
    return RC_FILE_SEEK_ERROR;
  }
  fHandle->curPagePos = pageNum;
  /* To check if read succesfully  */
  resultCode = fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
  if (resultCode != PAGE_SIZE) {
    return RC_READ_NON_EXISTING_PAGE;
  }
  return resultCode;
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

  /* if the page number is not right, such as pageNum >= total, return failed. */
  if (fHandle->totalNumPages < (pageNum + 1)) {
    return RC_WRITE_FAILED;
  }

  resultCode = fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE * sizeof(char), SEEK_SET);
  if (resultCode != 0) {
    return RC_WRITE_FAILED;
  }

  fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
  fHandle->curPagePos = pageNum;

  return RC_OK;
}


/*
 * If the file has less than numberOfPages pages then increase the size to numberOfPages.
 */
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
  if (NULL == fHandle) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  //if already has enough capacity, return
  if (fHandle->totalNumPages >= numberOfPages) {
    return RC_OK;
  }
  int i;
  int n = numberOfPages - fHandle->totalNumPages;
  for (i = 0; i < n; i++) {
    appendEmptyBlock(fHandle);
  }
  return RC_OK;
}

/*
 * Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
 */
RC appendEmptyBlock(SM_FileHandle *fHandle) {


  FILE *file = fHandle->mgmtInfo;

  resultCode = fseek(file, 0, SEEK_END);
  if (resultCode != 0) {
    return RC_WRITE_FAILED;
  }
  /* allocate the empty space with the initial value '\0'. */
  SM_PageHandle buff = (char *) calloc(PAGE_SIZE, sizeof(char));

  if (fwrite(buff, sizeof(char), PAGE_SIZE, file) == 0)
    resultCode = RC_WRITE_FAILED;
  else {
    /* totalNumPages & curPagePos Matching appears */
    fHandle->totalNumPages = fHandle->totalNumPages + 1;
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
  return readBlock(index, fHandle, memPage);
}

/*
 * Read the previous page relative to the curPagePos of the file.
 * The curPagePos should be moved to the page that was read.
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos - 1;
  return readBlock(pageNum, fHandle, memPage);
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
  return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

/*
 * Read the last page relative to the curPagePos of the file.
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}


//write in current position
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
