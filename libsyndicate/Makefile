CPP			:= g++ -Wall -g
PYTHON_OUT  := ../ms/protobufs
PROTOC		:= protoc -I. --cpp_out=. --python_out=$(PYTHON_OUT)
LIBINC		:= 
INC			:= -I/usr/include -Ilibmicrohttpd-syndicate/out/include
AR				:= ar

LIB			:= -lpthread -lcurl -lcrypto -luriparser -lprotobuf -lrt
OBJ			:= util.o ms.pb.o serialization.pb.o ms-client.o libgateway.o
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE
PROTOBUFS	:= ms.proto serialization.proto

MICROHTTPD  := libmicrohttpd-syndicate
MICROHTTPD_OBJ := $(MICROHTTPD)/out/lib/*.o

all: static
	
static: $(OBJ)
	$(CPP) -o libsyndicate.o -c libsyndicate.c $(INC) $(DEFS)
	$(AR) rcs libsyndicate.a $(OBJ) libsyndicate.o $(MICROHTTPD_OBJ)

%.o: %.c protobufs libmicrohttpd
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cpp protobufs libmicrohttpd
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cc protobufs
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

serialization.pb.o: protobufs
	$(CPP) -o $@ $(INC) $(DEFS) -c serialization.pb.cc

ms.pb.o: protobufs
	$(CPP) -o $@ $(INC) $(DEFS) -c ms.pb.cc

protobufs:
	$(PROTOC) $(PROTOBUFS)
	$(SHELL) -c "for python_pb in \$$(ls ../ms/protobufs/*.py); do echo \"Fixing up \$$python_pb\"; cat \$$python_pb | sed 's/google\./googlepb\./g' > /tmp/\$$(basename \$$python_pb); mv /tmp/\$$(basename \$$python_pb) \$$python_pb; done"

libmicrohttpd:
	cd $(MICROHTTPD) && ./build-syndicate.sh

.PHONY : clean
clean:
	/bin/rm -f *.o *.pb.* *.a

.PHONY : clean-all
clean-all:
	$(MAKE) clean
	$(MAKE) -C $(MICROHTTPD) clean
	cd $(MICROHTTPD) && ./build-syndicate.sh clean
