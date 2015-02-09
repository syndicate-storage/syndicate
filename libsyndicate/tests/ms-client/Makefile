CPP			:= g++ -Wall -fPIC -g -Wno-format
LIBINC		:= -L../
INC			:= -I/usr/include -I/usr/local/include

LIB			:= -lpthread -lcurl -lcrypto -lmicrohttpd -luriparser -lprotobuf -lrt -lsyndicate
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE


all: ms-client-test

ms-client-test: ms-client-test.o
	$(CPP) -o ms-client-test *.o $(LIB) $(LIBINC)

%.o: %.c
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cpp
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cc
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

.PHONY : clean
clean: oclean
	/bin/rm ms-client-test

.PHONY : oclean
oclean:
	/bin/rm -f *.o 
