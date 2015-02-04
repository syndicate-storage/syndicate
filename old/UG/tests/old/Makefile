CPP=g++ -Wall -g 
COMMON=../../common
LIBINC=-L$(COMMON)

# For home
LIB=-lfuse -lcurl -lcrypto -lpthread -lrt -lsyndicate `xml2-config --libs` -lboost_filesystem -lboost_system

# For planetlab
#LIB=-lfuse -lcurl -lcrypto -lpthread -lrt -lsyndicate `xml2-config --libs` -lboost_filesystem

INC=-I/usr/include -I/usr/include/syndicate `xml2-config --cflags` -I$(COMMON) -I../
OBJS=../syndicatefs.o ../metadata.o ../download.o ../log.o ../cache.o ../stats.o ../fs_entry.o
TESTOBJS=test_fs_entry.o test_download.o
DEFS=-D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=28 -D_REENTRANT -D_THREAD_SAFE
TESTS=test_fs_entry.test test_download.test

all: $(TESTS)

test_fs_entry.test: $(OBJS) $(TESTOBJS)
	$(CPP) -o test_fs_entry.test $(LIB) $(LIBINC) $(OBJS) test_fs_entry.o
	
test_download.test: $(OBJS) $(TESTOBJS)
	$(CPP) -o test_download.test $(LIB) $(LIBINC) $(OBJS) test_download.o

%.o : %.c
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cpp
	$(CPP) -o $@ $(INC) -c $< $(DEFS)


.PHONY : clean
clean:
	/bin/rm -f $(TESTS) $(TESTOBJS)
	/bin/rm -f test_fs_entry/local.html.*
	/bin/rm -rf test_fs_entry/localdir*
