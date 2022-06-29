CC=gcc
CFLAGS=-lpthread
DEPS=dberror.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dt.h test_helper.h rm_data_structures.h

OBJ1=dberror.o storage_mgr.o

OBJ2=dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $<

all: test_assign2_1 test_assign2_2


# assign 2
# test_assign2_1
test_assign2_1: $(OBJ2)	test_assign2_1.o
	$(CC) -o $@ $^ $(CFLAGS)

buffer_mgr_stat.o: buffer_mgr_stat.c
	$(CC) -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c
	$(CC) -c buffer_mgr.c

test_assign2_1.o: test_assign2_1.c
	$(CC) -c test_assign2_1.c

# test_assign2_2
test_assign2_2: $(OBJ2)	test_assign2_2.o
	$(CC) -o $@ $^ $(CFLAGS)

test_assign2_2.o: test_assign2_2.c
	$(CC) -c test_assign2_2.c

.PHONY: clean
clean:
	-rm -f test_assign2_1 test_assign2_2 *.o *.log

