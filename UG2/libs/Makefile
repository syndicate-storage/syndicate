include ../../buildconf.mk

INC	   := $(INC) -I ../
LIB   	:= -lsyndicate -lfskit
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)

UG_COMMON 		:= ../
UG_COMMON_HEADERS := $(wildcard $(UG_COMMON)/*.h)

OBJ     := $(wildcard $(BUILD_UG)/*.o)
HEADERS := $(patsubst $(UG_COMMON)/%.h,$(BUILD_UG_INCLUDEDIR)/%.h,$(UG_COMMON_HEADERS))

VERSION_MAJOR := 1
VERSION_MINOR := 0
VERSION_PATCH := 1

LIBSYNDICATE_UG := $(BUILD_UG_LIBS)/libsyndicate-ug.so
LIBSYNDICATE_UG_SONAME := libsyndicate-ug.so.$(VERSION_MAJOR)
LIBSYNDICATE_UG_SO := $(BUILD_UG_LIBS)/$(LIBSYNDICATE_UG_SONAME)
LIBSYNDICATE_UG_LIB := $(BUILD_UG_LIBS)/libsyndicate-ug.so.$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

LIBSYNDICATE_UG_FILES := $(LIBSYNDICATE_UG) $(LIBSYNDICATE_UG_SO) $(LIBSYNDICATE_UG_LIB)
LIBSYNDICATE_UG_INSTALL := $(patsubst $(BUILD_UG_LIBS)/%,$(LIBDIR)/%,$(LIBSYNDICATE_UG_FILES))
LIBSYNDICATE_UG_HEADERS_INSTALL := $(patsubst $(UG_COMMON)/%.h,$(INCLUDEDIR)/syndicate-ug/%.h,$(UG_COMMON_HEADERS))

all: $(LIBSYNDICATE_UG) $(HEADERS)

$(LIBSYNDICATE_UG_LIB): $(OBJ)
	@mkdir -p $(shell dirname $@)
	$(CPP) -shared -Wl,-soname,$(LIBSYNDICATE_UG_SONAME) -o $@ $(OBJ) $(LIBINC) $(DEFS)

$(LIBSYNDICATE_UG_SO): $(LIBSYNDICATE_UG_LIB)
	@mkdir -p $(shell dirname $@)
	@ln -sf $(shell basename $(LIBSYNDICATE_UG_LIB)) $@

$(LIBSYNDICATE_UG): $(LIBSYNDICATE_UG_SO)
	@mkdir -p $(shell dirname $@)
	@ln -sf $(shell basename $(LIBSYNDICATE_UG_SO)) $@

$(BUILD_UG_INCLUDEDIR)/%.h: $(UG_COMMON)/%.h
	@mkdir -p $(shell dirname $@)
	@cat $< > $@

.PHONY: install 
install: $(LIBSYNDICATE_UG_INSTALL) $(LIBSYNDICATE_UG_HEADERS_INSTALL)
	ldconfig

$(LIBDIR)/%: $(BUILD_UG_LIBS)/%
	@mkdir -p $(shell dirname $@)
	cp -a $< $@

$(INCLUDEDIR)/syndicate-ug/%.h: $(BUILD_UG_INCLUDEDIR)/%.h
	@mkdir -p $(shell dirname $@)
	cat $< > $@

.PHONY: clean
clean:
	@rm -rf $(LIBSYNDICATE_UG) $(LIBSYNDICATE_UG_SO) $(LIBSYNDICATE_UG_LIB) $(HEADERS)

.PHONY: uninstall 
uninstall:
	@rm -rf $(LIBSYNDICATE_UG_INSTALL) $(LIBSYNDICATE_UG_HEADERS_INSTALL)

print-%: ; @echo $*=$($*)
