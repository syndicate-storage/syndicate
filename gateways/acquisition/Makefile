include ../../buildconf.mk

LIB   	:= -lpthread -lsyndicate -lsyndicate-ug -lprotobuf -lfskit
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)
OBJDIR  := obj/gateway/acquisition
OBJ      := $(patsubst %.c,$(BUILD_AG)/$(OBJDIR)/%.o,$(C_SRCS)) \
            $(patsubst %.cpp,$(BUILD_AG)/$(OBJDIR)/%.o,$(CXSRCS))

INC		:= $(INC) -I$(BUILD_LIBSYNDICATE_INCLUDEDIR)

AG_BUILD := $(BUILD_AG)/syndicate-ag $(BUILD_LIBEXEC_DIR)/ag-driver
AG_INSTALL := $(BINDIR)/syndicate-ag $(LIBEXECDIR)/ag-driver

all: $(AG_BUILD)

$(BUILD_AG)/syndicate-ag: $(OBJ)
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(OBJ) $(LIB) $(LIBINC)

$(BUILD_LIBEXEC_DIR)/%: libexec/%
	@mkdir -p "$(shell dirname "$@")"
	cp -a "$<" "$@"

$(BUILD_AG)/$(OBJDIR)/%.o : %.c 
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

$(BUILD_AG)/$(OBJDIR)/%.o : %.cpp 
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

.PHONY: clean 
clean:
	rm -f $(OBJ)

.PHONY: install
install: $(AG_INSTALL)

$(BINDIR)/%: $(BUILD_AG)/%
	@mkdir -p "$(shell dirname "$@")"
	cp -a "$<" "$@"

$(LIBEXECDIR)/%: $(BUILD_LIBEXEC_DIR)/%
	@mkdir -p "$(shell dirname "$@")"
	cp -a "$<" "$@"

.PHONY: uninstall
uninstall:
	rm -f "$(AG_INSTALL)"

print-%: ; @echo $*=$($*)
