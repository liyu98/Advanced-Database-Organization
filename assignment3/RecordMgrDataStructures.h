//
// Created by 李宇 on 2022/6/21.
// #include "bufferMgrDataStructures.h"


#ifndef ASSIGNMENT3_RECORDMGRDST_H
#define ASSIGNMENT3_RECORDMGRDST_H

#include "buffer_mgr.h"
#include "storage_mgr.h"

//DataStructures
typedef struct RM_TableMgmtData {
    int num_of_tuples;    // total number of tuples in the table
    int free_page;        // first free page which has empty slots in table
    BM_PageHandle ph;    // Buffer Manager PageHandle
    BM_BufferPool bm;    // Buffer Manager Buffer Pool

} RM_TableMgmtData;

// Structure for Scanning tuples in Table
typedef struct RM_ScanMgmtData {
    BM_PageHandle ph;
    RID rid; // current row that is being scanned
    int count; // no. of tuples scanned till now
    Expr *condition; // expression to be checked

} RM_ScanMgmtData;
//int count = 0;
//int size = 35;
RM_TableMgmtData *tableMgmt;
RM_ScanMgmtData *scanMgmt;

#endif //ASSIGNMENT3_RECORDMGRDST_H


