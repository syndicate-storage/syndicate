include ../buildconf.mk

# NOTE: we build the protocol buffers into libsyndicate...
LIB   	:= -lpthread -lcurl -lm -lrt -ldl -lmicrohttpd -lprotobuf -lcrypto -lssl -lz -l$(LIBJSON)

C_SRCS	:= $(wildcard *.c) \
            $(wildcard */*.c)

PROTOBUF_OBJ  := $(wildcard $(BUILD_PROTOBUFS_OBJ)/*.o)

CXSRCS	:= $(wildcard *.cpp) \
	         $(wildcard */*.cpp)

HEADERS := $(wildcard *.h) \
           $(wildcard */*.h)

HEADERS_PRIVATE := $(wildcard private/*.h)

OBJDIR := obj/libsyndicate
LIBSYNDICATE_OBJ := $(patsubst %.c,$(BUILD_LIBSYNDICATE)/$(OBJDIR)/%.o,$(C_SRCS)) \
                    $(patsubst %.cpp,$(BUILD_LIBSYNDICATE)/$(OBJDIR)/%.o,$(CXSRCS))

OBJ := $(LIBSYNDICATE_OBJ) $(PROTOBUF_OBJ)

INC	:= $(INC) -I$(BUILD_LIBSYNDICATE_INCLUDEDIR)

VERSION_MAJOR := 1
VERSION_MINOR := 0
VERSION_PATCH := 1

LIBSYNDICATE 		:= $(BUILD_LIBDIR)/libsyndicate.so
LIBSYNDICATE_SONAME := libsyndicate.so.$(VERSION_MAJOR)
LIBSYNDICATE_SO 	:= $(BUILD_LIBDIR)/$(LIBSYNDICATE_SONAME)
LIBSYNDICATE_LIB 	:= $(BUILD_LIBDIR)/libsyndicate.so.$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

LIBSYNDICATE_HEADERS := $(patsubst %.h,$(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h,$(HEADERS))
LIBSYNDICATE_HEADERS_PRIVATE := $(patsubst %.h,$(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h,$(HEADERS_PRIVATE))

LIBSYNDICATE_FILES := $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB)

LIBSYNDICATE_FILES_INSTALL := $(patsubst $(BUILD_LIBSYNDICATE)/%,$(LIBDIR)/%,$(LIBSYNDICATE_FILES))
LIBSYNDICATE_HEADERS_INSTALL := $(patsubst $(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h,$(INCLUDEDIR)/libsyndicate/%.h,$(filter-out $(LIBSYNDICATE_HEADERS_PRIVATE),$(LIBSYNDICATE_HEADERS)))

LIBSYNDICATE_INSTALL := $(LIBSYNDICATE_FILES_INSTALL) $(LIBSYNDICATE_HEADERS_INSTALL)

all: $(LIBSYNDICATE_HEADERS) $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB)

$(LIBSYNDICATE_LIB): $(OBJ)
	@mkdir -p "$(shell dirname "$@")"
	@rm -f "$@"
	$(CPP) -shared -Wl,-soname,$(LIBSYNDICATE_SONAME) -o "$@" $(OBJ) $(LIBINC) $(LIB)

$(LIBSYNDICATE_SO): $(LIBSYNDICATE_LIB)
	@rm -f "$@"
	@ln -sf "$(shell basename $(LIBSYNDICATE_LIB))" "$@"

$(LIBSYNDICATE): $(LIBSYNDICATE_SO)
	@rm -f "$@"
	@ln -sf "$(shell basename $(LIBSYNDICATE_SO))" "$@"

$(BUILD_LIBSYNDICATE_INCLUDEDIR)/%.h: %.h 
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(BUILD_LIBSYNDICATE)/$(OBJDIR)/%.o : %.c $(PROTOBUF_OBJ) $(LIBSYNDICATE_HEADERS)
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

$(BUILD_LIBSYNDICATE)/$(OBJDIR)/%.o : %.cpp $(PROTOBUF_OBJ) $(LIBSYNDICATE_HEADERS)
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

.PHONY: install
install: $(LIBSYNDICATE_INSTALL)

$(LIBDIR)/%: $(BUILD_LIBSYNDICATE)/%
	@mkdir -p "$(shell dirname "$@")"
	@rm -f "$@"
	cp -a "$<" "$@"

$(INCLUDEDIR)/libsyndicate/%: $(BUILD_LIBSYNDICATE_INCLUDEDIR)/%
	@mkdir -p "$(shell dirname "$@")"
	@rm -f "$@"
	cp -a "$<" "$@"

.PHONY: clean
clean:
	/bin/rm -rf $(LIBSYNDICATE) $(LIBSYNDICATE_SO) $(LIBSYNDICATE_LIB) $(LIBSYNDICATE_HEADERS) $(LIBSYNDICATE_OBJ)

.PHONY: uninstall
uninstall:
	/bin/rm -f $(LIBSYNDICATE_FILES_INSTALL) $(LIBSYNDICATE_HEADERS_INSTALL)

print-%: ; @echo $*=$($*)
