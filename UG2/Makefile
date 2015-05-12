include ../buildconf.mk

LIB   	:= -lsyndicate
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)
OBJ      := $(patsubst %.c,$(BUILD_UG)/%.o,$(C_SRCS)) $(patsubst %.cpp,$(BUILD_UG)/%.o,$(CXSRCS))

INC		:= $(INC) -I$(BUILD_LIBSYNDICATE_INCLUDEDIR)

TOOLS	:= tools/
GATEWAYS := gateways/
LIBS := libs/

all: $(TOOLS) $(GATEWAYS) $(LIBS) $(OBJ)

tools: $(OBJ)
	$(MAKE) -C $(TOOLS)

gateways: $(OBJ)
	$(MAKE) -C $(GATEWAYS)

libs: $(OBJ)
	$(MAKE) -C $(LIBS)

$(BUILD_UG)/%.o : %.c 
	@mkdir -p $(shell dirname $@)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

$(BUILD_UG)/%.o : %.cpp 
	@mkdir -p $(shell dirname $@)
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY: clean
clean:
	/bin/rm -rf $(BUILD_UG)/*

print-%: ; @echo $*=$($*)
