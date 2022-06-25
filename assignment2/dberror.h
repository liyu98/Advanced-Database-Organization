#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

#define RC_FILE_NAME_EMPTY 7
#define RC_FILE_EXIST 8
#define RC_FILE_CREATE_FAILED 9
#define RC_FAILED_DEL_FAIL 8

#define RC_ERROR 5

#define RC_PINNED_PAGES_STILL_IN_BUFFER 6

#define RC_FILE_NAME_EMPTY 7
#define RC_FILE_EXIST 8
#define RC_FILE_CREATE_FAILED 9
#define RC_FAILED_DEL_FAIL 8

#define RC_NULL_ERROR 10

#define RC_FILE_SEEK_ERROR 11
#define RC_FILE_CLOSE_FAILED 12
#define RC_FILE_DESTROY_FAILED 13

#define RC_PINNED_PAGES_IN_BUFFER 20
#define RC_REPLACE_WHILE_PINNED_PAGES 21
#define RC_NON_EXIST_BUFFERPOOL 22


/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
