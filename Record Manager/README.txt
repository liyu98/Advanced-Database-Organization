# assignment3 CS525

### Introduction:
    The goal of this assignment is to implement a simple record manager, and we add some necessary functions for
    convenience. This file offers the operation on tables, records and schemas in a database. We have Record Manager
    all function with the Tombstone.

### Directory Structure:
　　  　 buffer_mgr_stat.c
　　  　 dberror.c
　　  　 dberror.h
　　  　 buffer_mgr.h
　　  　 buffer_mgr.c
　　  　 dt.h
　　  　 buffer_mgr_stat.h
　　  　 expr.c
　　  　 expr.h
　　  　 test_helper.h
　　  　 Makefile
　　  　 record_mgr.h
　　  　 record_mgr.c
　　  　 rm_serializer.c
　　  　 tables.h
　　  　 storage_mgr.c
　　  　 storage_mgr.h
　　  　 test_expr.c
        rm_data_structures.h
###Requirements
gnu gcc compiler（ https://gcc.gnu.org/ )

### Instructions to Run the Code:
- Go to Project root (Assignment3) using Terminal:
1. with default test case:
Compile : make test_assign3_1
Run : ./test_assign3_1

2. With additional test cases:
Compile : make test_expr
Run : ./test_expr

3. revert:
make clean

4. Compile All:
Run : make all

###　Brief ideas:
1) Functions use two Structures named RM_TableMgmtData and RM_ScanMgmtData. 
	RM_TableMgmtData : maintain all pointer of Buffer Pool and information of total number of tuples, First free page
	RM_ScanMgmtData  : maintain pointer of buffer pool page handle and scan information like scan count, condition expression.

2) Tombstone : Tombstone is an efficient implementation for lazy deletion of tuples. We used one byte character for it. Its the first byte of every tuple.
	'$' :- it represents that slot has a record meaning its a non-empty slot.
	'0' :- (zero) it represents that slot is empty or that a previous value has been deleted; ready for insertion.
3) Page0 in the Page File contains only information, no records. 
	It contains: Number of tuples in the table, First page with empty slots in the file, the Schema of the Table.
4) We used LRU as the Page replacement Algorithm, Since Page0 will be frequently used we felt its good if it remains in memory


### notes：
1  the poolContents Demo expected results referenced case：const int orderRequests[]= {3,2,0,8,4,2,5,0,9,8,3,2} （test_assign2_2）;

2  the next research work：Check if it is necessary to release record->data