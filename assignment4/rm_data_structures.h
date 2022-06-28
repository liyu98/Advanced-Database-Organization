
#ifndef ASSIGNMENT3_RECORDMGRDST_H
#define ASSIGNMENT3_RECORDMGRDST_H

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "expr.h"

/**
 * Page Frame Structure
 * this structure represents one page frame in buffer pool (memory).
 *
 */
typedef struct FrameData {
    SM_PageHandle data;
    /* An identification integer number given to each page */
    PageNumber pageNum;
    /* When a page is modified (content updated or deleted), the dirtyFlag is set to TRUE */
    int dirtyFlag;
    /* Used to indicate the number of clients using that page at a given instance.When a page is in using, the fixCount is 0 */
    int fixCount;
    /* to get the least recently used page (Used by LRU) */
    int hitNum;
    /* to get the least frequently used page  */
    int rfNum;
} Frame_Data;


/**
 * Table Structure
 *
 *
 */
typedef struct TableData {
    // total number of tuples in the table
    int numOfTuples;
    // first free page which has empty slots in table
    int page;
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


/**
 * B+ Tree Node Structure
 */
typedef struct NodeData
{
    bool isLeaf;
    int *keys;
    int countOfKeys;
    struct Node *parent;
    struct Node *next;
    int pageNum;
    void **pointers;
} Node;


/**
 *  B+ Tree Structure
 */

typedef struct BTreeData {
    int nodes;
    int keys;
    int page;
    int element;
    Node * root;
    DataType keyType;
    BM_BufferPool *bPool;
    BM_PageHandle *pHandle;
    struct Node *nodePtr;
} BTree_Data;
typedef struct ScanManager
{
    int KeysScanned;
    int numKeys;
    Node *node;
    RID *records;
} ScanManager;

#endif //ASSIGNMENT3_RECORDMGRDST_H


