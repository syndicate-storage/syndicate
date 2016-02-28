include ../../../buildconf.mk

LIB   	:= -lsyndicate -lsyndicate-ug -lfskit -lfskit_fuse -lprotobuf -lcurl
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)

OBJDIR  := obj/gateway/user/syndicatefs
OBJ		:= $(patsubst %.c,$(BUILD_UG_GATEWAYS)/$(OBJDIR)/%.o,$(C_SRCS)) \
	       $(patsubst %.cpp,$(BUILD_UG_GATEWAYS)/$(OBJDIR)/%.o,$(CXSRCS))

UG_FUSE := syndicatefs
GATEWAYS := $(patsubst %,$(BUILD_UG_GATEWAYS)/%,$(UG_FUSE))

DEFS := $(DEFS) -D_FILE_OFFSET_BITS=64

all: $(GATEWAYS)

$(BUILD_UG_GATEWAYS)/$(UG_FUSE) : $(OBJ)
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) "$<" $(LIBINC) $(LIB)

$(BUILD_UG_GATEWAYS)/$(OBJDIR)/%.o : %.c
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

$(BUILD_UG_GATEWAYS)/$(OBJDIR)/%.o : %.cpp
	@mkdir -p "$(shell dirname "$@")"
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

.PHONY: clean
clean:
	rm -f $(OBJ) $(GATEWAYS)

.PHONY: install 
install:
	@echo "install target for FUSE gateway is undefined"

.PHONY: uninstall
uninstall:
	@echo "uninstall target for FUSE gateway is undefined"

print-%: ; @echo $*=$($*)
