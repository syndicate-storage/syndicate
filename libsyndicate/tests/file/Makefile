CPP			:= g++ -Wall -fPIC -g -Wno-format
INC			:= -I/usr/local/include -I/usr/local/include/libsyndicateUG -I/usr/local/include/libsyndicateUG/fs -I../
LIB			:= -lpthread -lcurl -lssl -lmicrohttpd -lprotobuf -lrt -lm -ldl -lsyndicate -lsyndicateUG
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D_DISTRO_DEBIAN -D__STDC_FORMAT_MACROS -fstack-protector -fstack-protector-all -funwind-tables

OBJ			:= $(patsubst %.c,%.o,$(wildcard *.c)) $(patsubst %.cpp,%.o,$(wildcard *.cpp))
COMMON		:= common.o
TARGETS	   := getattr getchild listdir create create_async delete delete_async update update_async chcoord rename

all: $(TARGETS)

getattr: $(OBJ) 
	$(CPP) -o getattr getattr.o $(COMMON) $(LIB) $(LIBINC)

getchild: $(OBJ) 
	$(CPP) -o getchild getchild.o $(COMMON) $(LIB) $(LIBINC)

listdir: $(OBJ)
	$(CPP) -o listdir listdir.o $(COMMON) $(LIB) $(LIBINC) 

create: $(OBJ)
	$(CPP) -o create create.o $(COMMON) $(LIB) $(LIBINC)

create_async: $(OBJ)
	$(CPP) -o create_async create_async.o $(COMMON) $(LIB) $(LIBINC)

update: $(OBJ)
	$(CPP) -o update update.o $(COMMON) $(LIB) $(LIBINC)

update_async: $(OBJ)
	$(CPP) -o update_async update_async.o $(COMMON) $(LIB) $(LIBINC)

delete: $(OBJ)
	$(CPP) -o delete delete.o $(COMMON) $(LIB) $(LIBINC)

delete_async: $(OBJ)
	$(CPP) -o delete_async delete_async.o $(COMMON) $(LIB) $(LIBINC)

chcoord: $(OBJ)
	$(CPP) -o chcoord chcoord.o $(COMMON) $(LIB) $(LIBINC)

rename: $(OBJ)
	$(CPP) -o rename rename.o $(COMMON) $(LIB) $(LIBINC)

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
