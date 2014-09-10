CPP    := g++ -Wall -g -fPIC 
LIB   := -lpthread -lrt -lcrypto
INC   := -I/usr/include -I. -I../../include
C_SRCS:= $(wildcard *.c)
CXSRCS:= $(wildcard *.cpp)
COMMON_SRC := $(wildcard iFuseLib.*.c)
OBJ   := $(patsubst %.c,%.o,$(C_SRCS)) $(patsubst %.cpp,%.o,$(CXSRCS))
DEFS  := -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_TEST_TRACE -D_FILE_OFFSET_BITS=64
TESTS := log
COMMON_OBJ := $(patsubst %.c,%.o,$(COMMON_SRC))

.PHONY: debug
	/bin/echo $(COMMON_SRC)
	/bin/echo $(COMMON_OBJ)

all: $(TESTS)

log: $(OBJ)
	$(CPP) -o log log.o $(COMMON) $(LIBINC) $(LIB)

%.o : %.c
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cpp
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY: clean
clean:
	/bin/rm -f $(OBJ) $(TESTS)
