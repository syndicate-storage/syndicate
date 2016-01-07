include ../../buildconf.mk

LIB   	:= -lsyndicate -lfskit -lprotobuf -lcurl
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)

UG_COMMON 		:= ../
UG_COMMON_OBJ  := $(patsubst $(UG_COMMON)/%.c,$(BUILD_UG)/%.o,$(wildcard $(UG_COMMON)/*.c)) \
						$(patsubst $(UG_COMMON)/%.cpp,$(BUILD_UG)/%.o,$(wildcard $(UG_COMMON)/*.cpp))

OBJ				:= $(patsubst %.c,$(BUILD_UG_TOOLS)/%.o,$(C_SRCS)) \
						$(patsubst %.cpp,$(BUILD_UG_TOOLS)/%.o,$(CXSRCS))

TOOL_NAMES := syndicate-ls syndicate-mkdir syndicate-rmdir syndicate-touch \
  			  syndicate-put syndicate-cat syndicate-vacuum syndicate-unlink \
			  syndicate-trunc syndicate-write syndicate-read

TOOLS := $(patsubst %,$(BUILD_UG_TOOLS)/%,$(TOOL_NAMES))
COMMON_SRC := common.cpp
COMMON_OBJ := $(patsubst %.cpp,$(BUILD_UG_TOOLS)/%.o,$(COMMON_SRC))

all: $(TOOLS)

$(BUILD_UG_TOOLS)/syndicate-% : $(BUILD_UG_TOOLS)/syndicate-%.o $(UG_COMMON_OBJ) $(COMMON_OBJ)
	@mkdir -p $(shell dirname "$@")
	$(CPP) -o "$@" $(INC) "$<" $(UG_COMMON_OBJ) $(COMMON_OBJ) $(LIBINC) $(LIB)

$(BUILD_UG_TOOLS)/%.o : %.c
	@mkdir -p $(shell dirname "$@")
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

$(BUILD_UG_TOOLS)/%.o : %.cpp
	@mkdir -p $(shell dirname "$@")
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

.PHONY: clean
clean:
	rm -f $(OBJ) $(TOOLS)

print-%: ; @echo $*=$($*)
