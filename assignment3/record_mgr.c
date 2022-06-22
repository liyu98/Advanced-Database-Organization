#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <unistd.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "data_structures.h"
#include <math.h>

int size = 35;
char *curTableName;
// scan is Start with this method
int flag;

Table_Data *tableData;

void initPage(char *nmtp, Schema *schema);

RC initReserverSchemaPage(SM_FileHandle *fHandle, char *data);

Schema *initSchema(SM_PageHandle pageHandle, int numAttrs);
//writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)

void fillRecordByZero(Record *record, int size);

int getFreeSlot(char *data, int recordSize);


// 初始化 Record Manager setp 2
RC initRecordManager(void *mgmtData) {
  initStorageManager();
  return RC_OK;
}

RC shutdownRecordManager() {
//  RC rc = shutdownBufferPool(&tableData->bPool);
//  if (rc != RC_OK) {
//    return rc;
//  }
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

void fillRecordByZero(Record *record, int size) {

  record->data = (char *) malloc(size);
  // set char pointer to data of record
  char *temp = record->data;
  // set tombstone '0' because record is still empty todo
  *temp = '0';
  temp += sizeof(char);
  *temp = '\0'; // set null value to record after tombstone

}


// give First Free Slot of Particular Page
int getFreeSlot(char *data, int recordSize) {

  int Slots = floor(PAGE_SIZE / recordSize);
  int i;
  for (i = 0; i < Slots; i++) {
    if (data[i * recordSize] != '$') {
      return i;
    }
  }
  return -1;
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
  initBufferPool(&tableData->bPool, name, size, RS_LFU, NULL);

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

//  printf(name);


  //判断rel是否为空
  if (rel == NULL) {
    return RC_NULL_ERROR;
  }
  //
  curTableName = name;
  SM_PageHandle smph;
  int numAttrs, i;

  rel->mgmtData = tableData;        // Set mgmtData
  rel->name = strdup(name);        // Set mgmtData

  //todo pingPage
  pinPage(&tableData->bPool, &tableData->pHandle, (PageNumber) 0); // pinning the 0th page

  //读取字段结构
  smph = (char *) tableData->pHandle.data;    // set char to pointer to 0th page data

  tableData->num_of_tuples = *(int *) smph;    // retrieve the total number of tuples from file
  //printf("Num of Tuples: %d \n", tableMgmt->num_of_tuples );
  smph = smph + sizeof(int);

  tableData->free_page = *(int *) smph;    // retrieve the free page
  //printf("First free page: %d \n", tableMgmt->free_page );
  smph = smph + sizeof(int);

  numAttrs = *(int *) smph;        // retrieve the number of Attributes
  smph = smph + sizeof(int);

  // Set all values to Schema object

  // 初始化表结构到RM_TableData的schema
  rel->schema = initSchema(smph, numAttrs);
  //schema; // set schema object to rel object
  // Unpin after reading
  unpinPage(&tableData->bPool, &tableData->pHandle);
  forcePage(&tableData->bPool, &tableData->pHandle);
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
  shutdownBufferPool(&table->bPool);    //  Shutdown Buffer Pool

  if (rel->schema != NULL) {
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
  if (name == NULL) {
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

  if (rel == NULL) {
    return RC_NULL_ERROR;
  }
  if (rel->mgmtData == NULL) {
    return RC_NULL_ERROR;
  }

  Table_Data *tmt;
  tmt = rel->mgmtData;

  return tmt->num_of_tuples;
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
  fillRecordByZero(r, recordSize);
// page number is not fixed for empty record which is in memory
// slot number is not fixed for empty record which is in memory
  r->id.page = -1;
  r->id.slot = -1;

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
  if (record == NULL) {
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
 * todo 注释
 */
RC insertRecord(RM_TableData *rel, Record *record) {
//  printf("insert record\n");

  RC resultCode;
  Table_Data *tableData;
  tableData = rel->mgmtData;


  int recordSize = getRecordSize(rel->schema);    // record size of particular Record
  if (recordSize < 0) {
    return recordSize;
  }

  // set rid from current Record
  RID *rid = &record->id;
  rid->page = tableData->free_page; // set First Free page to current page


  resultCode = pinPage(&tableData->bPool, &tableData->pHandle, rid->page);    // pinning first free page
  if (resultCode != RC_OK) {
    return resultCode;
  }

  char *curpage_data;
  curpage_data = tableData->pHandle.data;    // set character pointer to current page's data

  rid->slot = getFreeSlot(curpage_data, recordSize);    // retrieve first free slot of current pinned page

  while (rid->slot == -1) {
    unpinPage(&tableData->bPool, &tableData->pHandle);    // if pinned page doesn't have free slot the unpin that page

    rid->page++;    // increment page number
    pinPage(&tableData->bPool, &tableData->pHandle, rid->page);
    curpage_data = tableData->pHandle.data;
    rid->slot = getFreeSlot(curpage_data, recordSize);
  }

  char *slot;
  slot = curpage_data;
  //write record
  resultCode = markDirty(&tableData->bPool, &tableData->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  slot += rid->slot * recordSize;
  *slot = '$';
  slot++;

  memcpy(slot, record->data + 1, recordSize - 1);

  resultCode = unpinPage(&tableData->bPool, &tableData->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  tableData->num_of_tuples++;

  //todo to be removed

  resultCode = pinPage(&tableData->bPool, &tableData->pHandle, 0);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  curpage_data = tableData->pHandle.data;

  return RC_OK;
}


// Delete Record From Table having particular RID

/**
 * todo 注释
 * @param rel
 * @param id
 * @return
 */
RC deleteRecord(RM_TableData *rel, RID id) {
  RC resultCode;
  Table_Data *table = rel->mgmtData;
// get Record Size
  int rSize = getRecordSize(rel->schema);
  // pinning page which have record that needs to be deleted
  resultCode = pinPage(&table->bPool, &table->pHandle, id.page);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  //update number of tuples
  //update first free page
  table->num_of_tuples--;
  table->free_page = id.page;

  // set character pointer to pinned page data
  // retrieve number of tuples
  char *tmpdata = table->pHandle.data;
  *(int *) tmpdata = table->num_of_tuples;

  // set pointer to perticular slot of record
  // set tombstone '0' for deleted record
  tmpdata += id.slot * rSize;
  *tmpdata = '0';

  // mark page as Dirty because of deleted record
  resultCode = markDirty(&table->bPool, &table->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  // unpin the page after deletion
  resultCode = unpinPage(&table->bPool, &table->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  //move to slot
  // need to reduce the number of tuples in 0th page

  return RC_OK;
}

// Update particular Record of Table
/**
 *
 * @param rel
 * @param record
 * @return
 */
RC updateRecord(RM_TableData *rel, Record *record) {

  RC resultCode;
  Table_Data *table = rel->mgmtData;
  resultCode = pinPage(&table->bPool, &table->pHandle, record->id.page);
  // pinning the age have record which we need to Update
  if (resultCode != RC_OK) {
    return resultCode;
  }

  char *tmpData;
  RID id = record->id;

  tmpData = table->pHandle.data;
  int rSize = getRecordSize(rel->schema); // get Record size
  tmpData = tmpData + id.slot * rSize; // set pointer to desire slot

  *tmpData = '$'; // set tombstone as '$' because it will become non-empty record

  tmpData++; // increment data pointer by 1

  memcpy(tmpData, record->data + 1,
         rSize - 1); // write new record to old record means update the record with new record

  resultCode = markDirty(&table->bPool, &table->pHandle); // mark page as dirty because record is updated
  if (resultCode != RC_OK) {
    return resultCode;
  }
  resultCode = unpinPage(&table->bPool, &table->pHandle); // unpinning the page after updation
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}


char *getTempStr(char *slot) {
  char *temp1 = slot + 1; // increment pointer to next attribute
  temp1 += sizeof(int);
  char *string = (char *) malloc(5);
  strncpy(string, temp1, 4);
  string[4] = '\0';
  temp1 += 4;
  free(string);
  return temp1;
}


// get particular record from Table
/**
 *
 * @param rel
 * @param id
 * @param record
 * @return
 */

RC getRecord(RM_TableData *rel, RID id, Record *record) {
  RC result;
  if (rel->mgmtData == NULL) {
    return RC_NULL_ERROR;
  }
  int rSize = getRecordSize(rel->schema); // get Record Size

  Table_Data *table = rel->mgmtData;
  result = pinPage(&table->bPool, &table->pHandle, id.page); // pinning the age have record which we need to GET
  if (result != RC_OK) {
    return result;
  }

  char *slot = table->pHandle.data;
  slot += id.slot * rSize;
  if (*slot != '$') {
    return RC_RM_NO_TUPLE_FIND; // return code for not matching record in the table
  } else {
    char *target = record->data; // set pointer to data of records
    *target = '0';
    target++;
    memcpy(target, slot + 1, rSize - 1); // het record data
    record->id = id; // set Record ID

    char *slot1 = record->data;

    getTempStr(slot1);

//    free(string);

  }
  result = unpinPage(&table->bPool, &table->pHandle); // Unpin the page after getting record
  if (result != RC_OK) {
    return result;
  }
  return RC_OK;
}

/**
 * Start Scan
 * @param rel
 * @param scan
 * @param cond
 * @return
 * todo 注释
 */

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

  //每次需要释放下
//  closeTable(rel);

  Table_Data *table;
  // set rel->mgmtData value to tableMgmt before Closing
  table = rel->mgmtData;
  shutdownBufferPool(&table->bPool);    //  Shutdown Buffer Pool

  if (rel->schema != NULL) {
    freeSchema(rel->schema);
  }

  openTable(rel, curTableName);

  Scan_Condition *scanCond;
  scanCond = (Scan_Condition *) malloc(sizeof(Scan_Condition)); //allocate memory for scanManagement Data

  scan->mgmtData = scanCond;

  scanCond->rid.page = 1;      // start scan from 1st page
  scanCond->rid.slot = 0;      // start scan from 1st slot
  scanCond->count = 0;         // count of n number of scan
  scanCond->condition = cond; // set the condition of scan
  Table_Data *tmt;
  tmt = rel->mgmtData;
  tmt->num_of_tuples = 10;    // set the number of tuples
  scan->rel = rel;
  flag = 0;

  return RC_OK;
}


/// Close Scan after finishing it
/**
 *
 * @param scan
 * @return
 */
RC closeScan(RM_ScanHandle *scan) {
  Scan_Condition *sc = (Scan_Condition *) scan->mgmtData;
  Table_Data *tableMgmt = (Table_Data *) scan->rel->mgmtData;

  //if incomplete scan
  if (sc->count > 0) {

    RC result = unpinPage(&tableMgmt->bPool, &sc->ph); // unpin the page
    if (result != RC_OK) {
      return result;
    }

    sc->rid.page = 1; // reset scanMgmt to 1st page
    sc->rid.slot = 0; // reset scanMgmt to 1st slot
    sc->count = 0; // reset count to 0
  }

  // Free mgmtData

  free(scan->mgmtData);

  return RC_OK;
}


RC next(RM_ScanHandle *scan, Record *record) {

  RC result;
  Scan_Condition *sc;
  sc = (Scan_Condition *) scan->mgmtData;
  Table_Data *t;
  t = (Table_Data *) scan->rel->mgmtData;    //tableMgmt;
  if (t->num_of_tuples == 0) {
    return RC_RM_NO_MORE_TUPLES;
  }
  int recordSize = getRecordSize(scan->rel->schema);
  int totalSlots = floor(PAGE_SIZE / recordSize);


  Value *value = (Value *) malloc(sizeof(Value));

  char *data;
  while (sc->count <= t->num_of_tuples) {  //scan until all tuples are scanned

    if (sc->count <= 0) {
      //    printf("Inside scanMgmt->count <= 0 \n");
      sc->rid.page = 1;
      sc->rid.slot = 0;

      result = pinPage(&t->bPool, &sc->ph, sc->rid.page);
      if (result != RC_OK) {
        return result;
      }
      data = sc->ph.data;
    } else {
      sc->rid.slot++;
      if (sc->rid.slot >= totalSlots) {
        sc->rid.slot = 0;
        sc->rid.page++;
      }

      result = pinPage(&t->bPool, &sc->ph, sc->rid.page);
      if (result != RC_OK) {
        return result;
      }
      data = sc->ph.data;
    }

    data = data + sc->rid.slot * recordSize;

    record->id.page = sc->rid.page;
    record->id.slot = sc->rid.slot;

    sc->count++;

    char *target = record->data;
    *target = '0';
    target++;

    memcpy(target, data + 1, recordSize - 1);


    if (sc->condition != NULL) {
      evalExpr(record, (scan->rel)->schema, sc->condition, &value);
    }

    if (value->v.boolV == TRUE) {  //v.BoolV is true when the condition checks out
      result = unpinPage(&t->bPool, &sc->ph);
      if (result != RC_OK) {
        return result;
      }
      flag = 1;
      return RC_OK;  //return RC_Ok
    }
  }

  result = unpinPage(&t->bPool, &sc->ph);
  if (result != RC_OK) {
    return result;
  }

  sc->rid.page = 1;
  sc->rid.slot = 0;
  sc->count = 0;

  return RC_RM_NO_MORE_TUPLES;

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
/**
 *
 * @param schema
 * @param attrNum
 * @param result
 * @return
 */
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
/**
 * todo 注释
 * @param record
 * @param schema
 * @param attrNum
 * @param value
 * @return
 */
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

  int offset = 0;

  attrOffset(schema, attrNum, &offset); // to offset to the givent attribute number

  Value *attrValue = (Value *) malloc(sizeof(Value)); // allocate memory to value object

  char *string = record->data;

  string += offset;

  if (attrNum == 1) {
    schema->dataTypes[attrNum] = 1;
  }

  switch (schema->dataTypes[attrNum]) {
    case DT_INT: // get attribute value from record of type Integer
    {
      int val = 0;
      memcpy(&val, string, sizeof(int));
      attrValue->v.intV = val;
      attrValue->dt = DT_INT;
    }
      break;
    case DT_STRING: // get attribute value from record of type String
    {
      attrValue->dt = DT_STRING;

      int len = schema->typeLength[attrNum];
      attrValue->v.stringV = (char *) malloc(len + 1);
      strncpy(attrValue->v.stringV, string, len);
      attrValue->v.stringV[len] = '\0';
      //printf("STRING : %s \n\n", tempValue->v.stringV);


    }
      break;
    case DT_FLOAT: // get attribute value from record of type Float
    {
      attrValue->dt = DT_FLOAT;
      float val;
      memcpy(&val, string, sizeof(float));
      attrValue->v.floatV = val;
    }
      break;
    case DT_BOOL: // get attribute value from record of type Boolean
    {
      attrValue->dt = DT_BOOL;
      bool val;
      memcpy(&val, string, sizeof(bool));
      attrValue->v.boolV = val;
    }
      break;
    default:
      printf("NO DATATYPE \n");
  }

  *value = attrValue;
  return RC_OK;
}

// Set the Attribute value into the Record
/**
 * todo 注释
 * @param record
 * @param schema
 * @param attrNum
 * @param value
 * @return
 */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
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
      printf("NO DATATYPE");
  }

  return RC_OK;
}

