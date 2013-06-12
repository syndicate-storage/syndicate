CPP=g++ -Wall -g -Wno-format 
COMMON=../common
SERVERS=RG-disk

# For home
LIB= -lcurl -lcrypto -lpthread -lrt -lprotobuf -luriparser -lmicrohttpd

INC=-I/usr/include -I/usr/include/syndicate -I$(COMMON) 
DEFS=-D_REENTRANT -D_THREAD_SAFE
OBJS_COMMON=$(patsubst %.cpp,%.o,$(wildcard $(COMMON)/*.cpp)) $(patsubst %.cc,%.o,$(wildcard $(COMMON)/*.cc)) $(patsubst %.c,%.o,$(wildcard $(COMMON)/*.c))

all: $(SERVERS)

RG-disk: common
	$(CPP) -o RG-disk $(LIB) $(OBJS_COMMON) $(INC) RG-disk.cpp

%.o : %.c
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cpp
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

common:
	$(MAKE) -C $(COMMON)

.PHONY : clean
clean:
	/bin/rm -f *.o $(SERVERS)

