CPP=g++ -Wall -g
COMMON=../../common
MICROHTTPD=$(COMMON)/libmicrohttpd-syndicate/out
LIBINC=-L$(COMMON)
INC=-I/usr/include -I$(COMMON) -I../ -I. -I$(MICROHTTPD)/include
SRC=$(wildcard *.cpp)
OBJS=$(SRC:%.cpp=%.o)
DEFS=-D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE

all: $(OBJS)

%.o : %.cpp 
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY : clean
clean:
	/bin/rm -f $(OUT) *.o
