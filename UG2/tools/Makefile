include ../../buildconf.mk

INC	   := $(INC) -I ../
LIB   	:= -lsyndicate -lfskit
C_SRCS	:= $(wildcard *.c)
CXSRCS	:= $(wildcard *.cpp)

UG_COMMON 		:= ../
UG_COMMON_OBJ  := $(patsubst $(UG_COMMON)/%.c,$(BUILD_UG)/%.o,$(wildcard $(UG_COMMON)/*.c)) \
						$(patsubst $(UG_COMMON)/%.cpp,$(BUILD_UG)/%.o,$(wildcard $(UG_COMMON)/*.cpp))

OBJ				:= $(patsubst %.c,$(BUILD_UG_TOOLS)/%.o,$(C_SRCS)) \
						$(patsubst %.cpp,$(BUILD_UG_TOOLS)/%.o,$(CXSRCS))

TOOLS		:= $(BUILD_UG_TOOLS)/syndicate-ls

all: $(TOOLS)

$(BUILD_UG_TOOLS)/syndicate-% : $(BUILD_UG_TOOLS)/syndicate-%.o
	$(CPP) -o $@ $(INC) $(LIBINC) $(LIB) $(OBJ) $(UG_COMMON_OBJ)

install: $(TOOLS)
	cp -a $(TOOLS) $(BINDIR)

$(BUILD_UG_TOOLS)/%.o : %.c
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

$(BUILD_UG_TOOLS)/%.o : %.cpp
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY: clean
clean:
	/bin/rm -rf $(BUILD_UG)/*

print-%: ; @echo $*=$($*)
