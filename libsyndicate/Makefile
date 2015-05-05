include ../buildconf.mk

# NOTE: we build the protocol buffers into libsyndicate...
LIB   	:= -lpthread -lcurl -lm -lrt -ldl -lmicrohttpd -lprotobuf -lcrypto -lssl -lz -l$(LIBJSON)

C_SRCS	:= $(wildcard *.c) \
				$(wildcard scrypt/*.c) \
				$(wildcard ms/*.c)

PROTOBUF_SRCS  := $(wildcard $(BUILD_PROTOBUFS_CPP)/*.cc)

CXSRCS	:= $(wildcard *.cpp) \
				$(wildcard scrypt/*.cpp) \
				$(wildcard ms/*.cpp)

HEADERS	:= $(patsubst %.c,%.h,$(C_SRCS)) \
				$(patsubst %.cc,%.h,$(PROTOBUF_SRCS)) \
				$(patsubst %cpp,%.h,$(CXSRCS))

OBJ   := $(patsubst %.c,$(BUILD_LIBSYNDICATE)/%.o,$(C_SRCS)) \
			$(patsubst %.cpp,$(BUILD_LIBSYNDICATE)/%.o,$(CXSRCS)) \
			$(patsubst %.cc,%.o,$(PROTOBUF_SRCS))

VERSION_MAJOR := 1
VERSION_MINOR := 0
VERSION_PATCH := 1

LIBSYNDICATE 		:= $(BUILD_LIBSYNDICATE)/libsyndicate.so
LIBSYNDICATE_SONAME := libsyndicate.so.$(VERSION_MAJOR)
LIBSYNDICATE_SO 	:= $(BUILD_LIBSYNDICATE)/$(LIBSYNDICATE_SONAME)
LIBSYNDICATE_LIB 	:= $(BUILD_LIBSYNDICATE)/libsyndicate.so.$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

all: libsyndicate

libsyndicate: $(OBJ)
	$(CPP) -shared -Wl,-soname,$(LIBSYNDICATE_SONAME) -o $(LIBSYNDICATE_LIB) $(OBJ) $(LIBINC) $(LIB)
	$(SHELL) -c "if ! test -L $(LIBSYNDICATE_SO); then /bin/ln -s $(LIBSYNDICATE_LIB) $(LIBSYNDICATE_SO); fi"
	$(SHELL) -c "if ! test -L $(LIBSYNDICATE); then /bin/ln -s $(LIBSYNDICATE_SO) $(LIBSYNDICATE); fi"

libsyndicate-install: libsyndicate
	mkdir -p $(LIBDIR)
	cp -a $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB) $(LIBDIR)

libsyndicate-install-headers: libsyndicate
	mkdir -p $(INCLUDEDIR) $(INCLUDEDIR)/ms $(INCLUDEDIR)/scrypt
	cp -a $(wildcard *.h) 								$(INCLUDEDIR)
	cp -a $(wildcard ms/*.h) 							$(INCLUDEDIR)
	cp -a $(wildcard scrypt/*.h) 						$(INCLUDEDIR)
	cp -a $(wildcard $(BUILD_PROTOBUFS_CPP)/*.h) $(INCLUDEDIR)

install: libsyndicate-install libsyndicate-install-headers
	cp -a $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB) $(LIBDIR)/

install-headers: libsyndicate-install-headers

$(BUILD_LIBSYNDICATE)/%.o : %.c $(BUILD_LIBSYNDICATE_DIRS)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

$(BUILD_LIBSYNDICATE)/%.o : %.cpp $(BUILD_LIBSYNDICATE_DIRS)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

$(BUILD_PROTOBUFS_CPP)/%.o: $(BUILD_PROTOBUFS_CPP)/%.cc $(BUILD_PROTOBUFS_DIRS)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY: clean
clean:
	/bin/rm -rf $(BUILD_LIBSYNDICATE)/*

print-%: ; @echo $*=$($*)
