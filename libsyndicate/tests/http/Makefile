CPP			:= g++ -Wall -fPIC -g -Wno-format
INC			:= -I../../ -I/usr/local/include

LIB			:= -lpthread -lcurl -lssl -lmicrohttpd -lprotobuf -lrt -lm -ldl -lsyndicate 
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D_DISTRO_DEBIAN -D__STDC_FORMAT_MACROS -fstack-protector -fstack-protector-all -funwind-tables

TARGETS	   := server

all: $(TARGETS)

server: server.o
	$(CPP) -o server server.o $(COMMON) $(LIB) $(LIBINC)

%.o: %.c
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cpp
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cc
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

.PHONY : clean
clean: oclean
	/bin/rm $(TARGETS)

.PHONY : oclean
oclean:
	/bin/rm -f *.o 
