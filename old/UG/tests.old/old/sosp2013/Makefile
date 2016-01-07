CPP=g++ -Wall -g -Wno-format -Wno-unused-function
PROTOC=protoc -I=. --cpp_out=.
OUT_WRITE=write_file
OUT_READ=read_file
OUT_SERVE=serve_file
OUT_WRITE_REMOTE=write_file_remote
OUT=$(OUT_WRITE) $(OUT_READ) $(OUT_SERVE) $(OUT_WRITE_REMOTE)
COMMON=../../../common
CLIENT=../../
FS=../../fs
LIBINC=-L$(COMMON)

LIB=-lfuse -lcurl -lcrypto -lpthread -lrt -lprotobuf -lmicrohttpd -luriparser -lrt

OBJS_CLIENT=$(CLIENT)/log.o $(CLIENT)/stats.o $(CLIENT)/replication.o $(CLIENT)/collator.o $(CLIENT)/syndicate.o $(CLIENT)/http-common.o $(CLIENT)/fs/*.o
OBJS_COMMON=$(COMMON)/*.o
INC=-I/usr/include -I/usr/include/syndicate -I$(COMMON) -I$(FS) -I$(CLIENT)
DEFS=-D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE

all: write_file read_file serve_file write_file_remote

write_file: client common write_file.o
	$(CPP) -o $(OUT_WRITE) $(OBJS_CLIENT) $(OBJS_COMMON) $(LIB) $(INC) write_file.o

write_file_remote: client common write_file_remote.o
	$(CPP) -o $(OUT_WRITE_REMOTE) $(OBJS_CLIENT) $(OBJS_COMMON) $(LIB) $(INC) write_file_remote.o

read_file: client common read_file.o
	$(CPP) -o $(OUT_READ) $(OBJS_CLIENT) $(OBJS_COMMON) $(LIB) $(INC) read_file.o

serve_file: client common serve_file.o
	$(CPP) -o $(OUT_SERVE) $(OBJS_CLIENT) $(OBJS_COMMON) $(LIB) $(INC) serve_file.o

.PHONY : client
client: common
	$(MAKE) -C $(CLIENT)

.PHONY : common
common:
	$(MAKE) -C $(COMMON)

%.o : %.c 
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cpp 
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cc 
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY : clean
clean:
	/bin/rm -f $(OUT) *.o

