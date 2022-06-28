#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>

#include "storage_mgr.h"

/*
 * The result code in the function/method, avoid repeated definitions,need to be initialized.
 */
RC resultCode;

FILE *file;

/*
 * Initiate Storage Manager：
 * Complete variable initialization settings.
 */
void initStorageManager(void) {
//  printf("Programming Assignment II: Buffer Manager \n");
//  printf("Group7 Member: DONGKAI QI, YU LI, ZHENYA ZHANG \n");
//  printf("Sucessfully Initate Storage Manager >>> \n");

  resultCode = -1;
  file = NULL;
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
 * 06/24 modify
 */
RC createPageFile(char *fileName) {
  /* Check if the filename is null or empty */
  if ((NULL == fileName) || (strlen(fileName) == 0)) {
    return RC_FILE_NAME_EMPTY;
  }
  file = fopen(fileName, "rb");
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
  file = fopen(fileName, "r");
  if (file == NULL) {
    return RC_FILE_NOT_FOUND;
  } else {
    // struct stat st;
    // if(stat(fileName,&st)!=0){
    //   return RC_FILE_HANDLE_NOT_INIT;
    // }
    // int totalNumPages = st.st_size;
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    // Get fileSize
    // int fileSize = ftell(file);
    struct stat fileInfo;
    if (fstat(fileno(file), &fileInfo) < 0){
      return RC_ERROR;
    }
    fHandle->totalNumPages = fileInfo.st_size / PAGE_SIZE;
    fclose(file);
    return RC_OK;
  }
}
/*
 * Close the open page file
 */
RC closePageFile(SM_FileHandle *fHandle) {
  if (NULL == fHandle)
    return RC_FILE_NOT_FOUND;
//
//  FILE *fp = (FILE *) fHandle->mgmtInfo;
//  if (!fHandle->mgmtInfo){
//    return RC_FILE_HANDLE_NOT_INIT;
//  }

  if (file != NULL)
    fclose(file);
  return RC_OK;
}

/**
 * destroy PageFile
 * @param fileName
 * @return
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

/**
 * reads the block at position pageNum from a file,stores its content in the memory
 * @param pageNum
 * @param fHandle
 * @param memPage
 * @return ErrorCode
 */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

  /* check if the file handle pointer exists */
  if (NULL == fHandle) {
    return RC_FILE_HANDLE_NOT_INIT;
  }

  if (pageNum > fHandle->totalNumPages || pageNum < 0)
    return RC_READ_NON_EXISTING_PAGE;

  file = fopen(fHandle->fileName, "r");

  // Checking if file was successfully opened.
  if (file == NULL)
    return RC_FILE_NOT_FOUND;

  int seek = fseek(file, (pageNum * PAGE_SIZE), SEEK_SET);
  if (seek == 0) {
    if (fread(memPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE)
      return RC_ERROR;
  } else {
    return RC_READ_NON_EXISTING_PAGE;
  }

  fHandle->curPagePos = ftell(file);
  fclose(file);

  return RC_OK;
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
  resultCode = readBlock(index, file, memPage);
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

/**
 * Write one page BACK to the file (disk) start from absolute position.
 * @param pageNum
 * @param fHandle
 * @param memPage
 * @return errorCode
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (pageNum > fHandle->totalNumPages || pageNum < 0){
    return RC_WRITE_FAILED;
  }
  // Opening file stream，Checking if file was successfully opened.
  file = fopen(fHandle->fileName, "r+");
  if (file == NULL){
    return RC_FILE_NOT_FOUND;
  }

  if (pageNum == 0) {
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
    int i;
    for (i = 0; i < PAGE_SIZE; i++) {
      // check file is ending in between writing
      // Writing a character from memPage to page file
      if (feof(file))
        appendEmptyBlock(fHandle);
      fputc(memPage[i], file);
    }

    fHandle->curPagePos = ftell(file);
    fclose(file);
  } else {
    fHandle->curPagePos = pageNum * PAGE_SIZE;
    fclose(file);
    writeCurrentBlock(fHandle, memPage);
  }
  return RC_OK;
}


/**
 * Write a page to disk using the current position.
 * @param fHandle
 * @param memPage
 * @return errCode
 */
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (fHandle == NULL) {
    return RC_NULL_ERROR;
  }

  file = fopen(fHandle->fileName, "r+");
  if (file == NULL){
    return RC_FILE_NOT_FOUND;
  }

  appendEmptyBlock(fHandle);

  fseek(file, fHandle->curPagePos, SEEK_SET);
  fwrite(memPage, sizeof(char), strlen(memPage), file);
  fHandle->curPagePos = ftell(file);
  fclose(file);

  return RC_OK;
}

/*
 * Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
 */
/**
 * ensureCapacity
 * @param fHandle
 * @return errCoce
 */
RC appendEmptyBlock(SM_FileHandle *fHandle) {
  if (fHandle == NULL) {
    return RC_NULL_ERROR;
  }

  SM_PageHandle emptyBlock = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

  // move the cursor (pointer) position to the begining of the file stream.
  int Seek = fseek(file, 0, SEEK_END);
  if (Seek == 0) {
    fwrite(emptyBlock, sizeof(char), PAGE_SIZE, file);
  } else {
    free(emptyBlock);
    return RC_WRITE_FAILED;
  }

  free(emptyBlock);

  fHandle->totalNumPages++;
  return RC_OK;
}

/*
 * If the file has less than numberOfPages pages then increase the size to numberOfPages.
 */
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
  file = fopen(fHandle->fileName, "a");

  if (file == NULL){
    return RC_FILE_NOT_FOUND;
  }

  while (numberOfPages > fHandle->totalNumPages){
    appendEmptyBlock(fHandle);
  }
  fclose(file);
  return RC_OK;
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