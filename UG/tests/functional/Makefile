CPP			:= g++ -Wall -fPIC -g -Wno-format
INC			:= -I/usr/local/include -I/usr/local/include/libsyndicateUG -I/usr/local/include/libsyndicateUG/fs/ -I/usr/local/include/libsyndicate

LIB			:= -lpthread -lcurl -lssl -lmicrohttpd -lprotobuf -lrt -lm -ldl -lsyndicate -lsyndicateUG -lprofiler
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D_DISTRO_DEBIAN -D__STDC_FORMAT_MACROS -fstack-protector -fstack-protector-all -funwind-tables

TARGETS	   := creat read write open-close mkdir readdir rmdir unlink getxattr setxattr listxattr removexattr chownxattr chmodxattr
COMMON		:= common.o

all: $(TARGETS)

creat: creat.o $(COMMON)
	$(CPP) -o creat creat.o $(COMMON) $(LIB) $(LIBINC)

read: read.o $(COMMON)
	$(CPP) -o read read.o $(COMMON) $(LIB) $(LIBINC)

write: write.o $(COMMON)
	$(CPP) -o write write.o $(COMMON) $(LIB) $(LIBINC)

open-close: open-close.o $(COMMON)
	$(CPP) -o open-close open-close.o $(COMMON) $(LIB) $(LIBINC)

mkdir: mkdir.o $(COMMON)
	$(CPP) -o mkdir mkdir.o $(COMMON) $(LIB) $(LIBINC)

readdir: readdir.o $(COMMON)
	$(CPP) -o readdir readdir.o $(COMMON) $(LIB) $(LIBINC)

rmdir: rmdir.o $(COMMON)
	$(CPP) -o rmdir rmdir.o $(COMMON) $(LIB) $(LIBINC)

unlink: unlink.o $(COMMON)
	$(CPP) -o unlink unlink.o $(COMMON) $(LIB) $(LIBINC)

getxattr: getxattr.o $(COMMON)
	$(CPP) -o getxattr getxattr.o $(COMMON) $(LIB) $(LIBINC)

setxattr: setxattr.o $(COMMON)
	$(CPP) -o setxattr setxattr.o $(COMMON) $(LIB) $(LIBINC)

listxattr: listxattr.o $(COMMON)
	$(CPP) -o listxattr listxattr.o $(COMMON) $(LIB) $(LIBINC)

removexattr: removexattr.o $(COMMON)
	$(CPP) -o removexattr removexattr.o $(COMMON) $(LIB) $(LIBINC)

chownxattr: chownxattr.o $(COMMON)
	$(CPP) -o chownxattr chownxattr.o $(COMMON) $(LIB) $(LIBINC)

chmodxattr: chmodxattr.o $(COMMON)
	$(CPP) -o chmodxattr chmodxattr.o $(COMMON) $(LIB) $(LIBINC)

%.o:	%.c
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
