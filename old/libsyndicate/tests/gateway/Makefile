CPP			:= g++ -Wall -fPIC -g -Wno-format
INC			:= -I../../../ -I/usr/local/include -I/usr/local/include/libsyndicate

LIB			:= -lpthread -lcurl -lssl -lmicrohttpd -lprotobuf -lrt -lm -ldl -lsyndicate 
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D_DISTRO_DEBIAN -D__STDC_FORMAT_MACROS -fstack-protector -fstack-protector-all -funwind-tables

TARGETS	   := gateway-echo client-getmanifest client-getblock client-patchmanifest client-detach client-rename client-truncate
COMMON	   := common.o

all: $(TARGETS)

client-getmanifest: client-getmanifest.o $(COMMON)
	$(CPP) -o client-getmanifest client-getmanifest.o $(LIB) $(LIBINC) $(COMMON)

client-getblock: client-getblock.o $(COMMON)
	$(CPP) -o client-getblock client-getblock.o $(LIB) $(LIBINC) $(COMMON)

client-patchmanifest: client-patchmanifest.o $(COMMON)
	$(CPP) -o client-patchmanifest client-patchmanifest.o $(LIB) $(LIBINC) $(COMMON)

gateway-echo: gateway-echo.o
	$(CPP) -o gateway-echo gateway-echo.o $(LIB) $(LIBINC)

client-detach: client-detach.o
	$(CPP) -o client-detach client-detach.o $(LIB) $(LIBINC) $(COMMON)

client-rename: client-rename.o
	$(CPP) -o client-rename client-rename.o $(LIB) $(LIBINC) $(COMMON)

client-truncate: client-truncate.o
	$(CPP) -o client-truncate client-truncate.o $(LIB) $(LIBINC) $(COMMON)

%.o: %.c
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cpp
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

%.o: %.cc
	$(CPP) -o $@ $(INC) $(DEFS) -c $<

.PHONY : clean
clean: oclean
	/bin/rm -f $(TARGETS)

.PHONY : oclean
oclean:
	/bin/rm -f *.o 
