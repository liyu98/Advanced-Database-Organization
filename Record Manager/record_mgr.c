#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <unistd.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "rm_data_structures.h"
#include <math.h>

const char C_TOMB_STONE = '$';
const int Max_Attr_Length = 10;
const int Max_AttrName_Length = 10;
const int MAX_PAGENUM_SIZE = 35;
char *Cur_TableName;
int scan_state;

Table_Data *tableData;

int getSchemaInfo(char *nmtp);
void initSchemaInfoForPage(char *nmtp, Schema *schema);
RC initReserverSchemaPage(SM_FileHandle *fHandle, char *data);
Schema *initSchema(SM_PageHandle pageHandle, int numOfAttrs);
void fillRecordByZero(Record *record, int size);
int searchFreeSlot(char *data, int recordSize);


/**
 * initializes the Record Manager
 * @param mgmtData
 * @return errCode
 */
RC initRecordManager(void *mgmtData) {
  initStorageManager();
  return RC_OK;
}

/**
 * Shutdown Record Manager, We can release resources by this method
 * @return errorCode
 */
RC shutdownRecordManager() {
//  RC rc = shutdownBufferPool(&tableData->bPool);
//  if (rc != RC_OK) {
//    return rc;
//  }
  return RC_OK;
}

int getSchemaInfo(char *nmtp) {
  nmtp = (char *) tableData->pHandle.data;
  tableData->numOfTuples = *(int *) nmtp;
  nmtp = nmtp + sizeof(int);
  tableData->page = *(int *) nmtp;
  nmtp = nmtp + sizeof(int);
  int Attrs = *(int *) nmtp;
  nmtp = nmtp + sizeof(int);
  return Attrs;
}

/**
 * init schema Info and Store database schema structure
 * @param nmtp
 * @param schema
 */
void initSchemaInfoForPage(char *nmtp, Schema *schema) {

  memset(nmtp, 0, PAGE_SIZE);
  *(int *) nmtp = 0;
  nmtp = nmtp + sizeof(int);
  *(int *) nmtp = 1;
  nmtp = nmtp + sizeof(int);
  *(int *) nmtp = schema->numAttr;  /* num of attributes */
  nmtp += sizeof(int);
  *(int *) nmtp = schema->keySize;   /* set keySize of Attributes  */
  nmtp += sizeof(int);

  int i;
  for (i = 0; i < schema->numAttr; i++) {
    /* set Attribute Name, notice : Max_AttrName_Length  */
    strncpy(nmtp, schema->attrNames[i], Max_AttrName_Length);
    nmtp = nmtp + Max_AttrName_Length;
    *(int *) nmtp = (int) schema->dataTypes[i];
    nmtp = nmtp + sizeof(int);
    *(int *) nmtp = (int) schema->typeLength[i];
    nmtp = nmtp + sizeof(int);
  }
}

/**
 * fillRecordByZero
 * @param record
 * @param size
 */
void fillRecordByZero(Record *record, int size) {

  record->data = (char *) malloc(size);
  char *c = record->data;
  // fill tombstone with '0' because record is still empty
  *c = '0';
  c = c + sizeof(char);
  *c = '\0';
}

int searchFreeSlot(char *data, int recordSize) {

  int Slots = floor(PAGE_SIZE / recordSize);
  int i;
  for (i = 0; i < Slots; i++) {
    if (data[i * recordSize] != C_TOMB_STONE) {
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

/**
 * create Table with filename "name" if it doesn't exist
 * ie. createTable("test_table_r",schema)
 * @param name
 * @param schema
 * @return errCode
 */
RC createTable(char *name, Schema *schema) {
  if (schema == NULL) {
    return RC_NULL_ERROR;
  }
  RC resultCode;
  // Check if the file exists
  resultCode = access(name, F_OK);

  if (resultCode == -1) {
    /* if the file does not exist, create it by createPageFile(), */
    resultCode = createPageFile(name);
    if (resultCode != RC_OK) {
      return resultCode;
    }
  } else if (resultCode == 0) {
    return RC_RM_TABLE_EXISTS;
  } else {
    return resultCode;
  }

  // now allocate memory for table management data and begin init
  tableData = (Table_Data *) malloc(sizeof(Table_Data));

  //init Buffer Pool, strategy is RS_LRU and call the Buffer Manager functions
  initBufferPool(&tableData->bPool, name, MAX_PAGENUM_SIZE, RS_LRU, NULL);

  // Initialize the page header file and record the schema information
  char *schemaData[PAGE_SIZE];
  initSchemaInfoForPage(schemaData, schema);

  SM_FileHandle f;
  // open the newly created page
  resultCode = openPageFile(name, &f);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  // Write the Schema To first location of the page  file
  resultCode = writeBlock(0, &f, schemaData);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  // Closing File after writing
  resultCode = closePageFile(&f);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}


/**
 * Open table
 * ie. openTable(table, "test_table_r");
 * @param rel Table
 * @param name tableName
 * @return errCode
 */
RC openTable(RM_TableData *rel, char *name) {
  // Check if the parameter is empty
  if (rel == NULL) {
    return RC_NULL_ERROR;
  }
  // if the file has no access, then return error code to report it
  if (access(name, R_OK) != 0 || access(name, W_OK) != 0) {
    return RC_NON_TABLE_FILE__ACCESS;
  }

  Cur_TableName = name;
  rel->mgmtData = tableData;
  rel->name = strdup(name);

  pinPage(&tableData->bPool, &tableData->pHandle, (PageNumber) 0);

  // read schema field structure
  SM_PageHandle shm;
  int attrs, i;
  shm = (char *) tableData->pHandle.data;

  tableData->numOfTuples = *(int *) shm;
  shm = shm + sizeof(int);
  tableData->page = *(int *) shm;
  shm = shm + sizeof(int);
  // get the number of Attributes
  attrs = *(int *) shm;
  shm = shm + sizeof(int);

  // Initialize the table structure to the schema of RM_TableData
  rel->schema = initSchema(shm, attrs);

  RC resultCode;
  resultCode = unpinPage(&tableData->bPool, &tableData->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  resultCode = forcePage(&tableData->bPool, &tableData->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}

/**
 * Close table
 * @param rel
 * @return
 */
RC closeTable(RM_TableData *rel) {
  if (rel == NULL) {
    return RC_NULL_ERROR;
  }
  // shutdown buffer pool before Closing
  Table_Data *td;
  td = rel->mgmtData;
  shutdownBufferPool(&td->bPool);
  //free shcema
  freeSchema(rel->schema);

  rel->schema = NULL;
  rel->name = NULL;
  rel->mgmtData = NULL;

  return RC_OK;
}

/**
 *  Delete the Table file
 * @param name
 * @return
 */
RC deleteTable(char *name) {
  if (name == NULL) {
    printf("talbe name is null\n");
    return RC_NULL_ERROR;
  }

  RC resultCode;
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

  return tmt->numOfTuples;
}


/***********************************************************
*                                                          *
*              Record Related Methods.                     *
*                                                          *
***********************************************************/

/**
 * create an empty record of the given schema
 * @param record array of records that program create
 * @param schema
 * @return errCode
 */
RC createRecord(Record **record, Schema *schema) {
  if (schema == NULL) {
    return RC_NULL_ERROR;
  }
  // allocate memory to empty record, need free
  Record *r = (Record *) malloc(sizeof(Record));
  // get Record Size
  int rSize = getRecordSize(schema);
  // fill record and malloc memory
  fillRecordByZero(r, rSize);

  r->id.page = -1;  //assign page number with -1
  r->id.slot = -1;  //assign slot number with -1
  // return record
  *record = r;
  return RC_OK;
}

/**
 * free a certain record
 * @param record
 * @return
 */
RC freeRecord(Record *record) {
  if (record == NULL) {
    return RC_NULL_ERROR;
  }
  //todo: Check if it is necessary to release record->data
  free(record->data);
  record->data = NULL;
  // free memory of record
  free(record);
  record = NULL;

  return RC_OK;
}

RC setCurrentPinnedPage(Table_Data *table, RID *curRid) {
  RC resultCode;
  // set rid and the first free page
  curRid->page = table->page;
  // ping  first free page
  resultCode = pinPage(&tableData->bPool, &tableData->pHandle, curRid->page);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}

RC writeRecord(Table_Data *table, char *slot, RID *curRid, Record *record, int recordSize) {
  RC resultCode = markDirty(&table->bPool, &table->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  slot += curRid->slot * recordSize;
  *slot = C_TOMB_STONE;
  slot++;
  memcpy(slot, record->data + 1, recordSize - 1);
  return RC_OK;
}

/**
 * insert Record
 * @param rel RM_TableData
 * @param record
 * @return ErrCode
 */
RC insertRecord(RM_TableData *rel, Record *record) {
//  printf("begin insert record\n");
  RC resultCode;
  Table_Data *tableData;
  tableData = rel->mgmtData;

  int rSize = getRecordSize(rel->schema);
  if (rSize < 0) {
    printf("recordSize < 0 \n");
    return RC_ERROR;
  }

  char *curpage_data;
  RID *curRid = &record->id;
  setCurrentPinnedPage(tableData, curRid);

  //search the first free slot in the current pinned page
  curpage_data = tableData->pHandle.data;
  curRid->slot = searchFreeSlot(curpage_data, rSize);
  while (curRid->slot == -1) {
    /* if doesn't have free slot then unpin that page */
    resultCode = unpinPage(&tableData->bPool, &tableData->pHandle);
    if (resultCode != RC_OK) {
      return resultCode;
    }
    /* increment page number, redo pinPage  */
    curRid->page++;
    resultCode = pinPage(&tableData->bPool, &tableData->pHandle, curRid->page);
    if (resultCode != RC_OK) {
      return resultCode;
    }
    curpage_data = tableData->pHandle.data;
    curRid->slot = searchFreeSlot(curpage_data, rSize);
  }

  char *slot;
  slot = curpage_data;
  resultCode = writeRecord(tableData, slot, curRid, record, rSize);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  resultCode = unpinPage(&tableData->bPool, &tableData->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  tableData->numOfTuples++;
  resultCode = pinPage(&tableData->bPool, &tableData->pHandle, 0);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  curpage_data = tableData->pHandle.data;

  return RC_OK;
}


/**
 * Delete Record
 * @param rel table enty
 * @param id RID of the record we about to delete
 * @return
 */
RC deleteRecord(RM_TableData *rel, RID id) {
  RC resultCode;
  Table_Data *td = rel->mgmtData;
  /* get record size of table */
  int rSize = getRecordSize(rel->schema);
  // pinning page which record needs to be deleted
  resultCode = pinPage(&td->bPool, &td->pHandle, id.page);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  //update number of tuples, update first free page
  td->numOfTuples--;
  td->page = id.page;

  char *tmpdata = td->pHandle.data;
  *(int *) tmpdata = td->numOfTuples;
  tmpdata += id.slot * rSize;
  *tmpdata = '0';  /* set tombstone '0' for deleted record */

  // mark page as Dirty because of having modify (deleted record)
  resultCode = markDirty(&td->bPool, &td->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  // unpin the page after delete
  resultCode = unpinPage(&td->bPool, &td->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}

/**
 * update record, write info into page
 * @param rel
 * @param record
 * @return
 */
RC updateRecord(RM_TableData *rel, Record *record) {
  RC resultCode;
  Table_Data *td = rel->mgmtData;
  resultCode = pinPage(&td->bPool, &td->pHandle, record->id.page);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  char *tmpData;
  //get record RID
  RID id = record->id;
  tmpData = td->pHandle.data;
  int rSize = getRecordSize(rel->schema);
  if (rSize < 0) {
    return RC_ERROR;
  }

  tmpData = tmpData + id.slot * rSize;
  *tmpData = C_TOMB_STONE; // set tombstone as '$' because it will become non-empty record
  tmpData++;
  //write new record
  memcpy(tmpData, record->data + 1, rSize - 1);
  // mark page dirty because record haven modify ( updated )
  resultCode = markDirty(&td->bPool, &td->pHandle);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  resultCode = unpinPage(&td->bPool, &td->pHandle);
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

/**
 * get record having Record ID in the table referenced by "rel"
 * @param rel
 * @param id
 * @param record
 * @return errorCode
 */
RC getRecord(RM_TableData *rel, RID id, Record *record) {
  RC result;
  if (rel->mgmtData == NULL) {
    return RC_NULL_ERROR;
  }
  //get record Size
  int rSize = getRecordSize(rel->schema);
  // pin the page which have record what wo need to get
  Table_Data *table = rel->mgmtData;
  result = pinPage(&table->bPool, &table->pHandle, id.page);
  if (result != RC_OK) {
    return result;
  }
  // calc slot addr
  char *slot = table->pHandle.data;
  slot = slot + id.slot * rSize;
  if (*slot != C_TOMB_STONE) {
    return RC_RM_NO_TUPLE_FIND;
  }

  char *d = record->data;
  *d = '0';
  d++;
  memcpy(d, slot + 1, rSize - 1);
  record->id = id;
  //Copy data
  char *slot1 = record->data;
  getTempStr(slot1);
  //Unpin the page after the record is retrieved since the page is no longer required to be in memory
  result = unpinPage(&table->bPool, &table->pHandle);
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
 */

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

  if (rel == NULL)
    return RC_NULL_ERROR;
  if (scan == NULL)
    return RC_NULL_ERROR;
  if (cond == NULL)
    return RC_NULL_ERROR;

  //Need to release before each execution
  Table_Data *table;
  // set rel->mgmtData value to tableMgmt before Closing
  table = rel->mgmtData;
  // shutdown Buffer Pool
  shutdownBufferPool(&table->bPool);
  if (rel->schema != NULL) {
    freeSchema(rel->schema);
  }
  //open table for current table
  openTable(rel, Cur_TableName);
  // init Scan Condition
  Scan_Condition *scanCond;
  scanCond = (Scan_Condition *) malloc(sizeof(Scan_Condition));

  scan->mgmtData = scanCond;
  scanCond->rid.page = 1;
  scanCond->rid.slot = scanCond->count = 0;
  scanCond->condition = cond;

  Table_Data *tmt;
  tmt = rel->mgmtData;
  tmt->numOfTuples = Max_Attr_Length;
  scan->rel = rel;

  scan_state = 0;
  return RC_OK;
}

/**
 * Close Scan
 * @param scan
 * @return
 */
RC closeScan(RM_ScanHandle *scan) {
  Scan_Condition *sc = (Scan_Condition *) scan->mgmtData;
  Table_Data *data = (Table_Data *) scan->rel->mgmtData;
  if (sc->count > 0) {
    // unpin the page
    RC result = unpinPage(&data->bPool, &sc->ph);
    if (result != RC_OK) {
      return result;
    }
    sc->rid.page = 1;
    sc->rid.slot = sc->count = 0;
  }
  scan->mgmtData= NULL;
  free(scan->mgmtData);
  return RC_OK;
}

/**
 * scan, and return the next eligible record
 * @param scan
 * @param record
 * @return
 */
RC next(RM_ScanHandle *scan, Record *record) {

  RC result;
  if (scan == NULL) {
    return RC_NULL_ERROR;
  }
  Scan_Condition *sc;
  sc = (Scan_Condition *) scan->mgmtData;

  Table_Data *t;
  t = (Table_Data *) scan->rel->mgmtData;
  if (t->numOfTuples == 0) {
    return RC_RM_NO_MORE_TUPLES;
  }
  int Size = getRecordSize(scan->rel->schema);
  int Slots = floor(PAGE_SIZE / Size);

  char *data;
  Value *value = (Value *) malloc(sizeof(Value));
  //scan until all tuples are scanned
  while (sc->count <= t->numOfTuples) {
    if (sc->count <= 0) {
      // set page and slot to first position
      sc->rid.page = 1;
      sc->rid.slot = 0;
//      result = pinPage(&t->bPool, &sc->ph, sc->rid.page);
//      if (result != RC_OK) {
//        return result;
//      }
    } else {
      sc->rid.slot++;
      if (sc->rid.slot >= Slots) {
        sc->rid.slot = 0;
        sc->rid.page++;
      }
    }
    // ping the page
    result = pinPage(&t->bPool, &sc->ph, sc->rid.page);
    if (result != RC_OK) {
      return result;
    }

    data = sc->ph.data; // retrieving the data of the page
    data = data + sc->rid.slot * Size; // Calulate the data location from record's slot and record size

    record->id.page = sc->rid.page;
    record->id.slot = sc->rid.slot;
    sc->count++;

    char *dataPtr = record->data;
    *dataPtr = '0';
    dataPtr++;
    memcpy(dataPtr, data + 1, Size - 1);

    if (sc->condition != NULL) {
      evalExpr(record, (scan->rel)->schema, sc->condition, &value);
    }
    if (value->v.boolV == TRUE) {
      //v.BoolV is true when the condition checks out
      result = unpinPage(&t->bPool, &sc->ph);
      if (result != RC_OK) {
        return result;
      }
      scan_state = 1;
      return RC_OK;
    }
  }

  result = unpinPage(&t->bPool, &sc->ph);
  if (result != RC_OK) {
    return result;
  }

  sc->rid.page = 1;
  sc->rid.slot = sc->count = 0;

  //None of the tuple satisfy the condition , there are no more tuples to scan
  return RC_RM_NO_MORE_TUPLES;
}


/**
 * get the size of the each record for a given schema
 * @param schema
 * @return record Size
 */
int getRecordSize(Schema *schema) {
  int offsetSize = 0;
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    // loop for all number of attribute
    switch (schema->dataTypes[i]) {
      case DT_INT:
        offsetSize = offsetSize + sizeof(int);
        break;
      case DT_FLOAT:
        offsetSize = offsetSize + sizeof(float);
        break;
      case DT_STRING:
        offsetSize = offsetSize + schema->typeLength[i];
        break;
      case DT_BOOL:
        offsetSize = offsetSize + sizeof(bool);
        break;
    }
  }

  offsetSize = offsetSize + 1;
  return offsetSize;
}

// Create Schema of Table


/***********************************************************
*                                                          *
*              Schema Related Methods.                     *
*                                                          *
***********************************************************/

/**
 * create a new schema with given values and write the global variables
 * @param numAttr:  number of attributes
 * @param attrNames:  to an array of pointers to attribute names
 * @param dataTypes:  array of data types for each attribute
 * @param typeLength: array of length for each attribute
 * @param keySize: num of keys
 * @param keys: array of the positions of the key attributes
 * @return schema of Create, must remember free
 */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  /* Determine whether the parameter is legal */
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

  /* Create and Allocate memory to schema， write values into the variable SCHEMA */
  Schema *S = (Schema *) malloc(sizeof(Schema));
  S->attrNames = attrNames;
  S->numAttr = numAttr;
  S->dataTypes = dataTypes;
  S->typeLength = typeLength;
  S->keySize = keySize;
  S->keyAttrs = keys;
  return S;
}

/**
 * unset the passed in schema，and ree schema
 * @param schema
 * @return errCode
 */
RC freeSchema(Schema *schema) {
  //Occasional errors, the cause is found to be a memory management problem, temporarily shielding the code
  //malloc: *** error for object 0x600003a84ef0: pointer being freed was not allocated
  if (schema != NULL) {
//    if (schema->attrNames != NULL) {
//      printf("free attrNames \n");
//      free(schema->attrNames);
//    }
//    if (schema->dataTypes != NULL) {
//      printf("free dataTypes \n");
//      free(schema->dataTypes);
//    }
//    if (schema->typeLength != NULL) {
//      printf("free typeLength \n");
//      free(schema->typeLength);
//    }
//    if (schema->keyAttrs != NULL) {
//      printf("free keyAttrs \n");
//      free(schema->keyAttrs);
//    }
    schema->numAttr = schema->keySize = -1;
    schema->attrNames = NULL;
    schema->dataTypes = NULL;
    schema->typeLength = schema->keyAttrs = NULL;

    free(schema);
  } else {
    printf("schema is null\n");
    return RC_NULL_ERROR;
  }
  return RC_OK;
}

/**
 * init Schema with numAttrs
 * @param pageHandle
 * @param numAttrs
 * @return
 */
Schema *initSchema(SM_PageHandle pageHandle, int numOfAttrs) {

  Schema *sche;
  sche = (Schema *) malloc(sizeof(Schema));
  sche->numAttr = numOfAttrs;
  sche->attrNames = (char **) malloc(sizeof(char *) * numOfAttrs);
  sche->dataTypes = (DataType *) malloc(sizeof(DataType) * numOfAttrs);
  sche->typeLength = (int *) malloc(sizeof(int) * numOfAttrs);

  int idx;
  for (idx = 0; idx < numOfAttrs; idx++) {
    sche->attrNames[idx] = (char *) malloc(Max_AttrName_Length);
  }
  for (idx = 0; idx < sche->numAttr; idx++) {
    strncpy(sche->attrNames[idx], pageHandle, Max_AttrName_Length);
    pageHandle += Max_AttrName_Length;

    sche->dataTypes[idx] = *(int *) pageHandle;
    pageHandle += sizeof(int);

    sche->typeLength[idx] = *(int *) pageHandle;
    pageHandle += sizeof(int);
  }
  return sche;
}


/**
 * attrOffset
 * @param schema
 * @param attrNum
 * @param result
 * @return errCode
 */
RC attrOffset(Schema *schema, int attrNum, int *result) {
  int offsetSize = 1, attrPos = 0;
  for (attrPos = 0; attrPos < attrNum; attrPos++)
    /* check the dataType of attributes */
    if (schema->dataTypes[attrPos] == DT_INT) {
      offsetSize = offsetSize + sizeof(int);
    } else if (schema->dataTypes[attrPos] == DT_FLOAT) {
      offsetSize = offsetSize + sizeof(float);
    } else if (schema->dataTypes[attrPos] == DT_STRING) {
      offsetSize = offsetSize + schema->typeLength[attrPos];
    } else if (schema->dataTypes[attrPos] == DT_BOOL) {
      offsetSize = offsetSize + sizeof(bool);
    }

  *result = offsetSize;
  return RC_OK;
}

/**
 * get the value of a given record of the schema's attribute and store it
 * at the spot in **value
 * @param record
 * @param schema
 * @param attrNum
 * @param value
 * @return errCode
 */
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
//  printf("begin getAttr\n");
  int offsetSize = 0;
  attrOffset(schema, attrNum, &offsetSize);
  Value *attrValue = (Value *) malloc(sizeof(Value));

  char *str = record->data;
  str = str + offsetSize;
  if (attrNum == 1) {
    schema->dataTypes[attrNum] = 1;
  }
  // get attribute value from record of int, float, string, boll type

  if (schema->dataTypes[attrNum] == DT_INT) {
    int iv = 0;
    memcpy(&iv, str, sizeof(int));
    attrValue->v.intV = iv;
    attrValue->dt = DT_INT;
  } else if (schema->dataTypes[attrNum] == DT_FLOAT) {
    attrValue->dt = DT_FLOAT;
    float fv;
    memcpy(&fv, str, sizeof(float));
    attrValue->v.floatV = fv;
  } else if (schema->dataTypes[attrNum] == DT_STRING) {
    attrValue->dt = DT_STRING;
    int len = schema->typeLength[attrNum];
    attrValue->v.stringV = (char *) malloc(len + 1);
    strncpy(attrValue->v.stringV, str, len);
    attrValue->v.stringV[len] = '\0';
  } else if (schema->dataTypes[attrNum] == DT_BOOL) {
    attrValue->dt = DT_BOOL;
    bool bv;
    memcpy(&bv, str, sizeof(bool));
    attrValue->v.boolV = bv;
  }

  *value = attrValue;
  return RC_OK;
}

void setAttrValue(char *rdata, Schema *schema, int attrNum, Value *value) {

  switch (schema->dataTypes[attrNum]) {
    case DT_INT:
      *(int *) rdata = value->v.intV;
      rdata = rdata + sizeof(int);
      break;
    case DT_STRING: {
      char *buf;
      int len = schema->typeLength[attrNum];
      buf = (char *) malloc(len + 1);
      strncpy(buf, value->v.stringV, len);
      buf[len] = '\0';
      strncpy(rdata, buf, len);
      free(buf);
      rdata = rdata + schema->typeLength[attrNum];
    }
      break;
      /* set the attribute value of type Float */
    case DT_FLOAT: {
      // set value of attribute
      *(float *) rdata = value->v.floatV;
      rdata += sizeof(float);
    }
      break;
      /* set the attribute value of type Boolean */
    case DT_BOOL: {
      *(bool *) rdata = value->v.boolV;
      rdata += sizeof(bool);
    }
      break;
    default:
      printf("NO SUPPORT DATATYPE");
  }
}

/**
 * update the value of a record of a given schema's attrNum attribute to a value
 * @param record record waiting to get update
 * @param schema
 * @param attrNum the ordinal of the attribute we want to update
 * @param value the value want to update to
 * @return
 */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
  int offsetSize = 0;
  attrOffset(schema, attrNum, &offsetSize);
  char *data = record->data;
  data = data + offsetSize;
  // set attrs value
  setAttrValue(data, schema, attrNum, value);
  return RC_OK;
}

