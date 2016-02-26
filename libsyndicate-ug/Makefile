include ../buildconf.mk

LIB   	:= -lsyndicate -lprotobuf -lfskit
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)
HEADERS := $(wildcard *.h)
OBJDIR  := obj/libsyndicate-ug
OBJ      := $(patsubst %.c,$(BUILD_LIBSYNDICATE_UG)/$(OBJDIR)/%.o,$(C_SRCS)) \
            $(patsubst %.cpp,$(BUILD_LIBSYNDICATE_UG)/$(OBJDIR)/%.o,$(CXSRCS))

INC		:= $(INC) -I$(BUILD_LIBSYNDICATE_INCLUDEDIR)

VERSION_MAJOR := 1
VERSION_MINOR := 0
VERSION_PATCH := 1

LIBSYNDICATE_UG := $(BUILD_LIBDIR)/libsyndicate-ug.so
LIBSYNDICATE_UG_SONAME := libsyndicate-ug.so.$(VERSION_MAJOR)
LIBSYNDICATE_UG_SO := $(BUILD_LIBDIR)/$(LIBSYNDICATE_UG_SONAME)
LIBSYNDICATE_UG_LIB := $(BUILD_LIBDIR)/$(LIBSYNDICATE_UG_SONAME).$(VERSION_MINOR).$(VERSION_PATCH)
LIBSYNDICATE_UG_HEADERS := $(patsubst %.h,$(BUILD_LIBSYNDICATE_UG_INCLUDEDIR)/%.h,$(HEADERS))

LIBSYNDICATE_UG_FILES := $(LIBSYNDICATE_UG) $(LIBSYNDICATE_UG_SO) $(LIBSYNDICATE_UG_LIB)
LIBSYNDICATE_UG_INSTALL := $(patsubst $(BUILD_LIBSYNDICATE_UG)/%,$(LIBDIR)/%,$(LIBSYNDICATE_UG_FILES))
LIBSYNDICATE_UG_HEADERS_INSTALL := $(patsubst $(BUILD_LIBSYNDICATE_UG_INCLUDEDIR)/%.h,$(INCLUDEDIR)/libsyndicate-filesystem/%.h,$(LIBSYNDICATE_UG_HEADERS))

all: $(LIBSYNDICATE_UG) $(LIBSYNDICATE_UG_HEADERS)

$(LIBSYNDICATE_UG_LIB): $(OBJ)
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -shared -Wl,-soname,$(LIBSYNDICATE_UG_SONAME) -o "$@" $(OBJ) $(LIBINC) $(DEFS)

$(LIBSYNDICATE_UG_SO): $(LIBSYNDICATE_UG_LIB)
	@mkdir -p "$(shell dirname "$@")"
	@ln -sf "$(shell basename "$(LIBSYNDICATE_UG_LIB)")" "$@"

$(LIBSYNDICATE_UG): $(LIBSYNDICATE_UG_SO)
	@mkdir -p "$(shell dirname "$@")"
	@ln -sf "$(shell basename "$(LIBSYNDICATE_UG_SO)")" "$@"

$(BUILD_LIBSYNDICATE_UG_INCLUDEDIR)/%.h: %.h
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(BUILD_LIBSYNDICATE_UG)/$(OBJDIR)/%.o : %.c 
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

$(BUILD_LIBSYNDICATE_UG)/$(OBJDIR)/%.o : %.cpp 
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

.PHONY: install 
install: $(LIBSYNDICATE_UG_INSTALL) $(LIBSYNDICATE_UG_HEADERS_INSTALL)
	ldconfig

$(LIBDIR)/%: $(BUILD_LIBSYNDICATE_UG)/%
	@mkdir -p "$(shell dirname "$@")"
	cp -a "$<" "$@"

$(INCLUDEDIR)/libsyndicate-filesystem/%.h: $(BUILD_LIBSYNDICATE_UG_INCLUDEDIR)/%.h
	@mkdir -p "$(shell dirname "$@")"
	cat "$<" > "$@"

.PHONY: clean
clean:
	@rm -rf $(LIBSYNDICATE_UG) $(LIBSYNDICATE_UG_SO) $(LIBSYNDICATE_UG_LIB) $(LIBSYNDICATE_UG_HEADERS) $(OBJ)

.PHONY: uninstall 
uninstall:
	@rm -rf $(LIBSYNDICATE_UG_INSTALL) $(LIBSYNDICATE_UG_HEADERS_INSTALL)

print-%: ; @echo $*=$($*)

