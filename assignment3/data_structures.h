
#ifndef ASSIGNMENT3_RECORDMGRDST_H
#define ASSIGNMENT3_RECORDMGRDST_H

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "tables.h"
#include "expr.h"


typedef struct TableData {
    // total number of tuples in the table
    int num_of_tuples;
    // first free page which has empty slots in table
    int free_page;
    // Buffer Manager PageHandle
    BM_PageHandle pHandle;
    // Buffer Manager Buffer Pool
    BM_BufferPool bPool;
} Table_Data;

typedef struct ScanCondition {
    BM_PageHandle ph;
    // number of tuples scanned till now
    int count;
    // current row that is being scanned
    RID rid;
    // expression to be checked
    Expr *condition;

} Scan_Condition;


#endif //ASSIGNMENT3_RECORDMGRDST_H


