#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
	RM_TableData *rel;
	void *mgmtData;
} RM_ScanHandle;

// table and manager

// 初始化record manager
extern RC initRecordManager (void *mgmtData);
// 关闭record manager
extern RC shutdownRecordManager ();

// 创建table -11
extern RC createTable (char *name, Schema *schema);
// 打开表 -12
extern RC openTable (RM_TableData *rel, char *name);
// 关闭表 -13
extern RC closeTable (RM_TableData *rel);
// 删除表 -14
extern RC deleteTable (char *name);
// 获取tuple的序号（数量？） -15
extern int getNumTuples (RM_TableData *rel);

// handling records in a table

// 插入记录 33
extern RC insertRecord (RM_TableData *rel, Record *record);
// 删除记录 34
extern RC deleteRecord (RM_TableData *rel, RID id);
// 更新记录 35
extern RC updateRecord (RM_TableData *rel, Record *record);
// 获取记录 36
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans

// 开始扫描
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
// 扫描下一个
extern RC next (RM_ScanHandle *scan, Record *record);
// 关闭扫描
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);

// 创建schema - step 1
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
// -2
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
// 创建记录 -31
extern RC createRecord (Record **record, Schema *schema);
// 释放记录 -32
extern RC freeRecord (Record *record);
// 获取属性
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
// 设置属性
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

#endif // RECORD_MGR_H
