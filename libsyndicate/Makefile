include ../buildconf.mk

# NOTE: we build the protocol buffers into libsyndicate...
LIB   	:= -lpthread -lcurl -lm -lrt -ldl -lmicrohttpd -lprotobuf -lcrypto -lssl -lz -l$(LIBJSON)

C_SRCS	:= $(wildcard *.c) \
            $(wildcard */*.c)

PROTOBUF_OBJ  := $(wildcard $(BUILD_PROTOBUFS_CPP)/*.o)

CXSRCS	:= $(wildcard *.cpp) \
	         $(wildcard */*.cpp)

HEADERS := $(wildcard *.h) \
           $(wildcard */*.h)

LIBSYNDICATE_OBJ := $(patsubst %.c,$(BUILD_LIBSYNDICATE)/%.o,$(C_SRCS)) \
                    $(patsubst %.cpp,$(BUILD_LIBSYNDICATE)/%.o,$(CXSRCS))

OBJ := $(LIBSYNDICATE_OBJ) $(PROTOBUF_OBJ)

INC	:= $(INC) -I$(BUILD_LIBSYNDICATE_INCLUDEDIR)

VERSION_MAJOR := 1
VERSION_MINOR := 0
VERSION_PATCH := 1

LIBSYNDICATE 		:= $(BUILD_LIBSYNDICATE)/libsyndicate.so
LIBSYNDICATE_SONAME := libsyndicate.so.$(VERSION_MAJOR)
LIBSYNDICATE_SO 	:= $(BUILD_LIBSYNDICATE)/$(LIBSYNDICATE_SONAME)
LIBSYNDICATE_LIB 	:= $(BUILD_LIBSYNDICATE)/libsyndicate.so.$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

LIBSYNDICATE_HEADERS := $(patsubst %.h,$(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h,$(HEADERS))

LIBSYNDICATE_FILES := $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB)

LIBSYNDICATE_FILES_INSTALL := $(patsubst $(BUILD_LIBSYNDICATE)/%,$(LIBDIR)/%,$(LIBSYNDICATE_FILES))
LIBSYNDICATE_HEADERS_INSTALL := $(patsubst $(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h,$(INCLUDEDIR)/libsyndicate/%.h,$(LIBSYNDICATE_HEADERS))

LIBSYNDICATE_INSTALL := $(LIBSYNDICATE_FILES_INSTALL) $(LIBSYNDICATE_HEADERS_INSTALL)

all: $(LIBSYNDICATE_HEADERS) $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB)

$(LIBSYNDICATE_LIB): $(OBJ)
	@mkdir -p $(shell dirname $@)
	$(CPP) -shared -Wl,-soname,$(LIBSYNDICATE_SONAME) -o $@ $(OBJ) $(LIBINC) $(LIB)

$(LIBSYNDICATE_SO): $(LIBSYNDICATE_LIB)
	@ln -sf $(shell basename $(LIBSYNDICATE_LIB)) $@

$(LIBSYNDICATE): $(LIBSYNDICATE_SO)
	@ln -sf $(shell basename $(LIBSYNDICATE_SO)) $@

$(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h: %.h $(LIBSYNDICATE) 
	@mkdir -p $(shell dirname $@)
	@cat $< > $@

$(BUILD_LIBSYNDICATE)/%.o : %.c $(PROTOBUF_OBJ)
	@mkdir -p $(shell dirname $@)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

$(BUILD_LIBSYNDICATE)/%.o : %.cpp $(PROTOBUF_OBJ)
	@mkdir -p $(shell dirname $@)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY: install
install: $(LIBSYNDICATE_INSTALL)
	ldconfig

$(LIBDIR)/%: $(BUILD_LIBSYNDICATE)/%
	@mkdir -p $(shell dirname $@)
	cp -a $< $@

$(INCLUDEDIR)/libsyndicate/%: $(BUILD_LIBSYNDICATE_INCLUDEDIR)/%
	@mkdir -p $(shell dirname $@)
	cp -a $< $@

.PHONY: clean
clean:
	/bin/rm -rf $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB) $(LIBSYNDICATE_HEADERS) $(LIBSYNDICATE_OBJ)

.PHONY: uninstall
uninstall:
	/bin/rm -f $(LIBSYNDICATE_FILES_INSTALL) $(LIBSYNDICATE_HEADERS_INSTALL)

print-%: ; @echo $*=$($*)
