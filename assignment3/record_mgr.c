#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <unistd.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>


//DataStructures
typedef struct TableData {
    int num_of_tuples;    // total number of tuples in the table
    int free_page;        // first free page which has empty slots in table
    BM_PageHandle pHandle;    // Buffer Manager PageHandle
    BM_BufferPool bm;    // Buffer Manager Buffer Pool

} Table_Data;

// Structure for Scanning tuples in Table
typedef struct ScanData {
    BM_PageHandle ph;
    RID rid; // current row that is being scanned 
    int count; // no. of tuples scanned till now
    Expr *condition; // expression to be checked

} RM_ScanMgmtData;

int count = 0;
int size = 35;
Table_Data *tableData;

void initPage(char *nmtp, Schema *schema);

RC initReserverSchemaPage(SM_FileHandle *fHandle, char *data);

Schema *initSchema(SM_PageHandle pageHandle, int numAttrs);
//writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)

void fillRecordByZero(Record *record, int size);


// table and manager

// 初始化 Record Manager setp 2
RC initRecordManager(void *mgmtData) {
  // initialize Storage Manager inside Record Manager
  initStorageManager();
  return RC_OK;
}


extern RC shutdownRecordManager() {

  return RC_OK;
}

//todo:更改注释
void initPage(char *nmtp, Schema *schema) {

  int i;

  memset(nmtp, 0, PAGE_SIZE);
  // Number of tuples
  *(int *) nmtp = 0;
  // increment char pointer
  nmtp = nmtp + sizeof(int);

  // First free page is 1 because page 0 is for schema and other info
  *(int *) nmtp = 1;
  // increment char pointer
  nmtp = nmtp + sizeof(int);
  //set num of attributes
  *(int *) nmtp = schema->numAttr;
  nmtp += sizeof(int);
  // set keySize of Attributes
  *(int *) nmtp = schema->keySize;
  nmtp += sizeof(int);

  for (i = 0; i < schema->numAttr; i++) {
    // set Attribute Name
    strncpy(nmtp, schema->attrNames[i], 10);
    nmtp = nmtp + 10;
    // Set the Data Types of Attribute
    *(int *) nmtp = (int) schema->dataTypes[i];
    nmtp = nmtp + sizeof(int);
    // Set the typeLength of Attribute
    *(int *) nmtp = (int) schema->typeLength[i];
    nmtp = nmtp + sizeof(int);

  }

}


RC initReserverSchemaPage(SM_FileHandle *fHandle, char *data) {

  // Write Schema To 0th page of file
  //todo:注意代码
  int resultCode = writeBlock(0, &fHandle, data);
  if (resultCode != RC_OK) {
    return resultCode;
  }
//  // Closing File after writing
//  resultCode = closePageFile(&fHandle);
//  if (resultCode != RC_OK){
//    return resultCode;
//  }
  return RC_OK;
}

void fillRecordByZero(Record *record, int size){

  record->data = (char *) malloc(size);
  // set char pointer to data of record
  char *temp = record->data;
  // set tombstone '0' because record is still empty todo
  *temp = '0';
  temp += sizeof(char);
  *temp = '\0'; // set null value to record after tombstone

}


/***********************************************************
*                                                          *
*              Table Related Methods.                     *
*                                                          *
***********************************************************/


// Create Table with filename "name"
// 创建表，如果本地表不存在，则创建本地文件
// createTable("test_table_r",schema)
/**
 *
 * @param name
 * @param schema
 * @return
 */
RC createTable(char *name, Schema *schema) {

  printf("being create Table\n");

  if (schema == NULL) {
    return RC_NULL_ERROR;
  }
  RC resultCode;
  //判断表名是否存在
  resultCode = access(name, F_OK);

  if (resultCode == -1) {
    // 创本地表文件
    resultCode = createPageFile(name);

    if (resultCode != RC_OK) {
      return resultCode;
    }
  } else if (resultCode == 0) {
    return RC_RM_TABLE_EXISTS;
  } else {
    return resultCode;
  }


  // Allocate memory for table management data
  tableData = (Table_Data *) malloc(sizeof(Table_Data));

  //todo:需要重写
  initBufferPool(&tableData->bm, name, size, RS_LFU, NULL);

  char schemaData[PAGE_SIZE];
  char *ph_data = schemaData;
  // 初始化page头部文件，记录schema的信息
  initPage(ph_data, schema);

  SM_FileHandle fileHandle;
  // open Created File
  resultCode = openPageFile(name, &fileHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  // resultCode = initReserverSchemaPage(&fileHandle, data);

  // Write Schema To 0th page of file
  //todo:注意代码
  resultCode = writeBlock(0, &fileHandle, schemaData);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  // Closing File after writing
  resultCode = closePageFile(&fileHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  return RC_OK;
}

// 打开表
// openTable(table, "test_table_r");
/**
 *
 * @param rel
 * @param name
 * @return
 */
RC openTable(RM_TableData *rel, char *name) {

  //判断rel是否为空
  if (rel == NULL) {
    return RC_NULL_ERROR;
  }
  //

  SM_PageHandle smph;
  int numAttrs, i;

  rel->mgmtData = tableData;        // Set mgmtData
  rel->name = strdup(name);        // Set mgmtData

  //todo pingPage
  pinPage(&tableData->bm, &tableData->pHandle, (PageNumber) 0); // pinning the 0th page

  //读取字段结构
  smph = (char *) tableData->pHandle.data;    // set char to pointer to 0th page data

  tableData->num_of_tuples = *(int *) smph;    // retrieve the total number of tuples from file
  //printf("Num of Tuples: %d \n", tableMgmt->num_of_tuples );
  smph =  smph + sizeof(int);

  tableData->free_page = *(int *) smph;    // retrieve the free page
  //printf("First free page: %d \n", tableMgmt->free_page );
  smph = smph + sizeof(int);

  numAttrs = *(int *) smph;        // retrieve the number of Attributes
  smph = smph + sizeof(int);

  // Set all values to Schema object

  // 初始化表结构到RM_TableData的schema
  rel->schema = initSchema(smph,numAttrs);
          //schema; // set schema object to rel object
  // Unpin after reading
  unpinPage(&tableData->bm, &tableData->pHandle);
  forcePage(&tableData->bm, &tableData->pHandle);
  return RC_OK;

}


//todo: 重新注释
/**
 *
 * @param rel
 * @return
 */
RC closeTable(RM_TableData *rel) {

  Table_Data *table;
  // set rel->mgmtData value to tableMgmt before Closing
  table = rel->mgmtData;
  shutdownBufferPool(&table->bm);    //  Shutdown Buffer Pool

  if(rel->schema!=NULL){
    freeSchema(rel->schema);
  }
  rel->schema = NULL;
  rel->name = NULL;
  rel->mgmtData = NULL;

  return RC_OK;
}

// Delete the Table  file
// todo: 注释
/**
 * 主要是 Delete the Table  file
 * @param name
 * @return
 */
RC deleteTable(char *name) {
  if(name==NULL){
    printf("talbe name is null\n");
    return RC_NULL_ERROR;
  }
  //free(tableMgmt);
  RC resultCode;
  // removing  file
  resultCode = destroyPageFile(name);
  return resultCode;
}

/**
 * Get the total number of records stored in table
 * @param rel  RM_TableData
 * @return the total number of records(tuples)
 */
int getNumTuples(RM_TableData *rel) {

  if(rel==NULL){
    return RC_NULL_ERROR;
  }
  if(rel->mgmtData==NULL){
    return RC_NULL_ERROR;
  }

  Table_Data *tmt;
  tmt = rel->mgmtData;

  return tmt->num_of_tuples;
}


//
// give First Free Slot of Particular Page 
int getFreeSlot(char *data, int recordSize) {

  int i;
  int totalSlots = floor(PAGE_SIZE / recordSize);

  for (i = 0; i < totalSlots; i++) {

    if (data[i * recordSize] != '#') {

      //printf("data[ slot num : %d] contains: %c  \n\n", i, data[i * recordSize]);
      return i;

    }
  }
  return -1;
}




/***********************************************************
*                                                          *
*              Record Related Methods.                     *
*                                                          *
***********************************************************/


// Creating a Blank record in Memory
/**
 * create a blank Record
 * @param record
 * @param schema
 * @return
 */
RC createRecord(Record **record, Schema *schema) {
  // allocate memory to empty record, 需要释放
  Record *r = (Record *) malloc(sizeof(Record));

  // get Record Size
  int recordSize = getRecordSize(schema);

  // Allocate memory for data of record
  fillRecordByZero(r,recordSize);
//
//  r->data = (char *) malloc(recordSize);
//  // set char pointer to data of record
//  char *temp = r->data;
//  // set tombstone '0' because record is still empty todo
//  *temp = '0';
//  temp += sizeof(char);
//  *temp = '\0'; // set null value to record after tombstone

  r->id.page = -1; // page number is not fixed for empty record which is in memory
  r->id.slot = -1; // slot number is not fixed for empty record which is in memory

  *record = r; // set tempRecord to Record
  return RC_OK;
}


// Free Record after used
/**
 *
 * @param record
 * @return
 */
RC freeRecord(Record *record) {
  if(record == NULL) {
    return RC_NULL_ERROR;
  }

  free(record->data);
  record->data = NULL;

  // Free memory of record
  free(record);

  return RC_OK;
}

// handling records in a table
/**
 * insert Record
 * @param rel
 * @param record
 * @return
 */
RC insertRecord(RM_TableData *rel, Record *record) {

  Table_Data *tableMgmt;
  tableMgmt = rel->mgmtData;

  RID *rid = &record->id;    // set rid from current Record


  char *data;
  char *slotAddr;

  int recordSize = getRecordSize(rel->schema);    // record size of particular Record

  rid->page = tableMgmt->free_page; // set First Free page to current page
  //printf("page number : %d \n", rid->page );
  pinPage(&tableMgmt->bm, &tableMgmt->pHandle, rid->page);    // pinning first free page
  //printf("Create Record\n");

  data = tableMgmt->pHandle.data;    // set character pointer to current page's data
  rid->slot = getFreeSlot(data, recordSize);    // retrieve first free slot of current pinned page

  while (rid->slot == -1) {
    unpinPage(&tableMgmt->bm, &tableMgmt->pHandle);    // if pinned page doesn't have free slot the unpin that page

    rid->page++;    // increment page number
    pinPage(&tableMgmt->bm, &tableMgmt->pHandle, rid->page);
    data = tableMgmt->pHandle.data;
    rid->slot = getFreeSlot(data, recordSize);
  }

  //printf(" \nSlot Number : %d \n", rid->slot);
  slotAddr = data;

  //write record
  markDirty(&tableMgmt->bm, &tableMgmt->pHandle);

  slotAddr += rid->slot * recordSize;
  *slotAddr = '#';
  slotAddr++;

  memcpy(slotAddr, record->data + 1, recordSize - 1);

  //////////print to check
/*	
	char *temp = slotAddr;
	printf("1ST ATTR: %d \t", *(int*)temp);
			temp += sizeof(int);
			
			char * string = (char *)malloc(5);
			
			strncpy(string,temp , 4);
			string[5] = '\0';
			
			printf("2nd ATTR: %s \t", string);
			temp += 4;
			
			printf("3RD ATTR: %d \t", *(int*)temp);
			
		//	free(string);
	
		
		
*/
  //////////////////////////////


  unpinPage(&tableMgmt->bm, &tableMgmt->pHandle);

  tableMgmt->num_of_tuples++;

  /////////////////////////////////////////////// to be removed

  pinPage(&tableMgmt->bm, &tableMgmt->pHandle, 0);
  data = tableMgmt->pHandle.data;

  //unpinPage(&tableMgmt->bm, &tableMgmt->pHandle);

  return RC_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Delete Record From Table having particular RID

extern RC deleteRecord(RM_TableData *rel, RID id) {

  Table_Data *tableMgmt = rel->mgmtData;
  pinPage(&tableMgmt->bm, &tableMgmt->pHandle, id.page); // pinning page which have record that needs to be deleted
  tableMgmt->num_of_tuples--; //update number of tuples
  tableMgmt->free_page = id.page; //update first free page

  char *data = tableMgmt->pHandle.data; // set character pointer to pinned page data
  *(int *) data = tableMgmt->num_of_tuples; // retrieve number of tuples

  int recordSize = getRecordSize(rel->schema);  // get Record Size
  data += id.slot * recordSize; // set pointer to perticular slot of record

  *data = '0'; // set tombstone '0' for deleted record

  markDirty(&tableMgmt->bm, &tableMgmt->pHandle); // mark page as Dirty because of deleted record
  unpinPage(&tableMgmt->bm, &tableMgmt->pHandle); // unpin the page after deletion
  //move to slot

  // need to reduce the number of tuples in 0th page

  return RC_OK;
}

// Update particular Record of Table
extern RC updateRecord(RM_TableData *rel, Record *record) {

  Table_Data *tableMgmt = rel->mgmtData;
  pinPage(&tableMgmt->bm, &tableMgmt->pHandle, record->id.page); // pinning the age have record which we need to Update
  char *data;
  RID id = record->id;

  data = tableMgmt->pHandle.data;
  int recordSize = getRecordSize(rel->schema); // get Record size
  data += id.slot * recordSize; // set pointer to desire slot

  *data = '#'; // set tombstone as '#' because it will become non-empty record

  data++; // increment data pointer by 1

  memcpy(data, record->data + 1,
         recordSize - 1); // write new record to old record means update the record with new record

  markDirty(&tableMgmt->bm, &tableMgmt->pHandle); // mark page as dirty because record is updated
  unpinPage(&tableMgmt->bm, &tableMgmt->pHandle); // unpinning the page after updation

  return RC_OK;
}

// get particular record from Table
extern RC getRecord(RM_TableData *rel, RID id, Record *record) {
  Table_Data *tableMgmt = rel->mgmtData;

  //printf("\nId.Page %d\n Id.Slot : %d\n",id.page,id.slot);
  //forceFlushPool(&tableMgmt->bm);

  pinPage(&tableMgmt->bm, &tableMgmt->pHandle, id.page); // pinning the age have record which we need to GET

  int recordsize = getRecordSize(rel->schema); // get Record Size
  char *slotAddr = tableMgmt->pHandle.data;
  slotAddr += id.slot * recordsize;
  if (*slotAddr != '#')
    return RC_RM_NO_TUPLE_WITH_GIVEN_RID; // return code for not matching record in the table
  else {
    char *target = record->data; // set pointer to data of records
    *target = '0';
    target++;
    memcpy(target, slotAddr + 1, recordsize - 1); // het record data
    record->id = id; // set Record ID

    // Comment The code of Print

    char *slotAddr1 = record->data;

    // printf("\n Tomestone : %c", *slotAddr1);

    char *temp1 = slotAddr1 + 1; // increment pointer to next attribute

    // printf("\t1ST ATTR: %d \t", *(int*)temp1);
    temp1 += sizeof(int);
    char *string = (char *) malloc(5);
    strncpy(string, temp1, 4);
    string[4] = '\0';
    printf("2nd ATTR: %s \t", string);
    temp1 += 4;
    printf("3RD ATTR: %d \n", *(int *) temp1);
    free(string);

  }
  unpinPage(&tableMgmt->bm, &tableMgmt->pHandle); // Unpin the page after getting record
  return RC_OK;
}

// scan is Start with this method
int flag;

extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {


  closeTable(rel);
  openTable(rel, "test_table_r");

  RM_ScanMgmtData *scanMgmt;
  scanMgmt = (RM_ScanMgmtData *) malloc(sizeof(RM_ScanMgmtData)); //allocate memory for scanManagement Data

  scan->mgmtData = scanMgmt;

  scanMgmt->rid.page = 1;      // start scan from 1st page
  scanMgmt->rid.slot = 0;      // start scan from 1st slot
  scanMgmt->count = 0;         // count of n number of scan
  scanMgmt->condition = cond; // set the condition of scan
  Table_Data *tmt;

  tmt = rel->mgmtData;
  tmt->num_of_tuples = 10;    // set the number of tuples
  scan->rel = rel;
  flag = 0;
  return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *record) {


  RM_ScanMgmtData *scanMgmt;
  scanMgmt = (RM_ScanMgmtData *) scan->mgmtData;
  Table_Data *tmt;
  tmt = (Table_Data *) scan->rel->mgmtData;    //tableMgmt;

  Value *result = (Value *) malloc(sizeof(Value));

  static char *data;

  int recordSize = getRecordSize(scan->rel->schema);
  int totalSlots = floor(PAGE_SIZE / recordSize);
  //  printf("\n\nOutside while with total tuples: %d\n\n",tmt->num_of_tuples);
  if (tmt->num_of_tuples == 0)
    return RC_RM_NO_MORE_TUPLES;


  while (scanMgmt->count <= tmt->num_of_tuples) {  //scan until all tuples are scanned
    //	printf("\n\nInside while with scan count : %d , tot Tuples : %d \n\n",scanMgmt->count, tmt->num_of_tuples );
    if (scanMgmt->count <= 0) {
      //    printf("Inside scanMgmt->count <= 0 \n");
      scanMgmt->rid.page = 1;
      scanMgmt->rid.slot = 0;

      pinPage(&tmt->bm, &scanMgmt->ph, scanMgmt->rid.page);
      data = scanMgmt->ph.data;
    } else {
      scanMgmt->rid.slot++;
      if (scanMgmt->rid.slot >= totalSlots) {
        //	printf("SLOTS FULLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL\n");
        scanMgmt->rid.slot = 0;
        scanMgmt->rid.page++;
      }

      pinPage(&tmt->bm, &scanMgmt->ph, scanMgmt->rid.page);
      data = scanMgmt->ph.data;
    }

    data += scanMgmt->rid.slot * recordSize;
    //	printf("\tSLOT NUMBER : %d \t",scanMgmt->rid.slot );
    //	printf("PAGE NUMBER : %d \n",scanMgmt->rid.page );

    record->id.page = scanMgmt->rid.page;
    record->id.slot = scanMgmt->rid.slot;
    //memcpy(record->data, data, recordSize);
    scanMgmt->count++;

    char *target = record->data;
    *target = '0';
    target++;

    memcpy(target, data + 1, recordSize - 1);

    /*
        char *slotAddr1 = record->data;
        printf("\n Tombstone : %c", *slotAddr1);
        char *temp1 = slotAddr1+1;
        printf("\t1ST ATTR: %d \t", *(int*)temp1);
        temp1 += sizeof(int);
        char * string = (char *)malloc(5);
        strncpy(string,temp1 , 4);
        string[4] = '\0';
        printf("2nd ATTR: %s \t", string);
        temp1 += 4;
        printf("3RD ATTR: %d \t", *(int*)temp1);
        free(string);
    */

    if (scanMgmt->condition != NULL) {
      evalExpr(record, (scan->rel)->schema, scanMgmt->condition, &result);
      //printf("result: %s \n",result->v.boolV);
    }

    if (result->v.boolV == TRUE) {  //v.BoolV is true when the condition checks out
      unpinPage(&tmt->bm, &scanMgmt->ph);
      flag = 1;
      return RC_OK;  //return RC_Ok
    }
  }

  unpinPage(&tmt->bm, &scanMgmt->ph);
  scanMgmt->rid.page = 1;
  scanMgmt->rid.slot = 0;
  scanMgmt->count = 0;

  return RC_RM_NO_MORE_TUPLES;

}

/// Close Scan after finishing it
extern RC closeScan(RM_ScanHandle *scan) {
  RM_ScanMgmtData *scanMgmt = (RM_ScanMgmtData *) scan->mgmtData;
  Table_Data *tableMgmt = (Table_Data *) scan->rel->mgmtData;

  //if incomplete scan
  if (scanMgmt->count > 0) {
    unpinPage(&tableMgmt->bm, &scanMgmt->ph); // unpin the page
    scanMgmt->rid.page = 1; // reset scanMgmt to 1st page
    scanMgmt->rid.slot = 0; // reset scanMgmt to 1st slot
    scanMgmt->count = 0; // reset count to 0
  }

  // Free mgmtData

  scan->mgmtData = NULL;
  free(scan->mgmtData);
  return RC_OK;
}


// dealing with schemas and give the Record Size
/**
 * todo 注释
 * @param schema
 * @return
 */
extern int getRecordSize(Schema *schema) {
  int recordSize = 0;
  int i = 0; // offset set to zero
  for (i = 0; i < schema->numAttr; i++) {
    // loop for total number of attribute
    switch (schema->dataTypes[i]) {
      // check the data types of attributes
      case DT_STRING:
        // if data types is string then increment offset according to its typeLength
        recordSize = recordSize + schema->typeLength[i];
        break;
      case DT_INT:
        // if data types is INT then increment offset to size of INTEGER
        recordSize = recordSize + sizeof(int);
        break;
      case DT_FLOAT:
        recordSize = recordSize + sizeof(float); // if data types is FLOAT then increment offset to size of FLOAT
        break;
      case DT_BOOL:
        recordSize = recordSize + sizeof(bool); // if data types is BOOLEAN then increment offset to size of BOOLEAN
        break;
    }
  }

  recordSize = recordSize + 1; // plus 1 for end of string
  return recordSize;
}

// Create Schema of Table


/***********************************************************
*                                                          *
*              Schema Related Methods.                     *
*                                                          *
***********************************************************/

/**
 * Create a new Schema
 * @param numAttr: number of Attrs.
 * @param attrNames
 * @param dataTypes
 * @param typeLength: an array of length for each attribute.
 * @param keySize: number of keys.
 * @param keys: an array of the positions of the key attributes.
 * @return Schema
 */

/* 创建 Schema liyu */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  //printf("begin create schema\n");
  if (numAttr <= 0) {
    printf("numAttr is null\n");
    return NULL;
  }
  if (attrNames == NULL) {
    printf("attrNames is null\n");
    return NULL;
  }
  if (typeLength <= 0) {
    printf("typeLength is null\n");
    return NULL;
  }
  /* Create and Allocate memory to schema */
  Schema *S = (Schema *) malloc(sizeof(Schema));
  S->attrNames = attrNames;
  S->numAttr = numAttr;
  S->dataTypes = dataTypes;
  S->typeLength = typeLength;
  S->keySize = keySize;
  S->keyAttrs = keys;
  return S;
}

//
//Schema *schema;
//schema = (Schema *) malloc(sizeof(Schema));
//
//schema->numAttr = numAttrs;
//schema->attrNames = (char **) malloc(sizeof(char *) * numAttrs);
//schema->dataTypes = (DataType *) malloc(sizeof(DataType) * numAttrs);
//schema->typeLength = (int *) malloc(sizeof(int) * numAttrs);
//
//for (i = 0; i < numAttrs; i++)
//schema->attrNames[i] = (char *) malloc(10); //10 is max field length
//
//
//for (i = 0; i < schema->numAttr; i++) {
//
//strncpy(schema->attrNames[i], pHandle, 10);
//pHandle += 10;
//
//schema->dataTypes[i] = *(int *) pHandle;
//pHandle += sizeof(int);
//
//schema->typeLength[i] = *(int *) pHandle;
//pHandle += sizeof(int);
//}

/* 释放 Free schema liyu */

/**
 *
 * @param schema
 * @return
 */
RC freeSchema(Schema *schema) {
  if (schema != NULL) {

    if (schema->attrNames != NULL) {
      free(schema->attrNames);
    }
    if (schema->dataTypes != NULL) {
      free(schema->dataTypes);
    }
    if (schema->typeLength != NULL) {
      free(schema->typeLength);
    }
    if (schema->keyAttrs != NULL) {
      free(schema->keyAttrs);
    }

    schema->numAttr = -1;
    schema->attrNames = NULL;
    schema->dataTypes = NULL;
    schema->typeLength = NULL;
    schema->keyAttrs = NULL;
    schema->keySize = -1;

    free(schema);
  } else {
    printf("attrNames is null\n");
    return RC_NULL_ERROR;
  }
  return RC_OK;
}


/**
 *
 * @param pageHandle
 * @param numAttrs
 * @return
 */
Schema *initSchema(SM_PageHandle pageHandle, int numAttrs) {


  Schema *schema;
  schema = (Schema *) malloc(sizeof(Schema));

  schema->numAttr = numAttrs;
  schema->attrNames = (char **) malloc(sizeof(char *) * numAttrs);
  schema->dataTypes = (DataType *) malloc(sizeof(DataType) * numAttrs);
  schema->typeLength = (int *) malloc(sizeof(int) * numAttrs);

  int i;
  for (i = 0; i < numAttrs; i++)
    schema->attrNames[i] = (char *) malloc(10); //10 is max field length


  for (i = 0; i < schema->numAttr; i++) {

    strncpy(schema->attrNames[i], pageHandle, 10);
    pageHandle += 10;

    schema->dataTypes[i] = *(int *) pageHandle;
    pageHandle += sizeof(int);

    schema->typeLength[i] = *(int *) pageHandle;
    pageHandle += sizeof(int);
  }

  return schema;

}


// dealing with records and attribute values



// return position of particular attribute
RC attrOffset(Schema *schema, int attrNum, int *result) {
  int offset = 1;
  int attrPos = 0;

  for (attrPos = 0; attrPos < attrNum; attrPos++)  // loop for number of attribute
    switch (schema->dataTypes[attrPos]) // check dataTypes of attributes
    {
      case DT_STRING:
        offset += schema->typeLength[attrPos]; // if data types is string then increment offset according to its typeLength
        break;
      case DT_INT:
        offset += sizeof(int); // if data types is INT then increment offset to size of INTEGER
        break;
      case DT_FLOAT:
        offset += sizeof(float); // if data types is FLOAT then increment offset to size of FLOAT
        break;
      case DT_BOOL:
        offset += sizeof(bool); // if data types is BOOLEAN then increment offset to size of BOOLEAN
        break;
    }

  *result = offset;
  return RC_OK;
}


// get Attribute from record
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

  int offset = 0;

  attrOffset(schema, attrNum, &offset); // to offset to the givent attribute number

  Value *tempValue = (Value *) malloc(sizeof(Value)); // allocate memory to value object

  char *string = record->data;

  string += offset;
//	printf("\n attrNum : %d \n\n", attrNum);
  if (attrNum == 1) {
    schema->dataTypes[attrNum] = 1;
  }

  switch (schema->dataTypes[attrNum]) {
    case DT_INT: // get attribute value from record of type Integer
    {
      int val = 0;
      memcpy(&val, string, sizeof(int));
      tempValue->v.intV = val;
      tempValue->dt = DT_INT;
    }
      break;
    case DT_STRING: // get attribute value from record of type String
    {
      // printf("\n\n DT_STRING \n");
      tempValue->dt = DT_STRING;

      int len = schema->typeLength[attrNum];
      tempValue->v.stringV = (char *) malloc(len + 1);
      strncpy(tempValue->v.stringV, string, len);
      tempValue->v.stringV[len] = '\0';
      //printf("STRING : %s \n\n", tempValue->v.stringV);


    }
      break;
    case DT_FLOAT: // get attribute value from record of type Float
    {
      tempValue->dt = DT_FLOAT;
      float val;
      memcpy(&val, string, sizeof(float));
      tempValue->v.floatV = val;
    }
      break;
    case DT_BOOL: // get attribute value from record of type Boolean
    {
      tempValue->dt = DT_BOOL;
      bool val;
      memcpy(&val, string, sizeof(bool));
      tempValue->v.boolV = val;
    }
      break;
    default:
      printf("NO SERIALIZER FOR DATATYPE \n\n\n\n");
  }


  *value = tempValue;
  return RC_OK;
}

// Set the Attribute value into the Record
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
  int offset = 0;
  attrOffset(schema, attrNum, &offset); // retrieve the attribute offset
  char *data = record->data;
  data += offset;

  switch (schema->dataTypes[attrNum]) {
    case DT_INT: // set the attribute value of type Integer
      *(int *) data = value->v.intV;
      data += sizeof(int);
      break;
    case DT_STRING: // set the attribute value of type String
    {
      char *buf;
      int len = schema->typeLength[attrNum]; // length of string
      buf = (char *) malloc(len + 1); // allocate memory to buffer
      strncpy(buf, value->v.stringV, len); // copy string to buffer
      buf[len] = '\0';
      strncpy(data, buf, len); // copy data to buffer
      free(buf); // free memory of buffer
      data += schema->typeLength[attrNum];
    }
      break;
    case DT_FLOAT: // set the attribute value of type Float
    {
      *(float *) data = value->v.floatV;    // set value of attribute
      data += sizeof(float); // increment  the data pointer
    }
      break;
    case DT_BOOL: // set the attribute value of type Boolean
    {
      *(bool *) data = value->v.boolV;    // copy the boolean value
      data += sizeof(bool);
    }
      break;
    default:
      printf("NO SERIALIZER FOR DATATYPE");
  }

  return RC_OK;
}

