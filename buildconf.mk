# build environment
ROOT_DIR	:= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUILD		?= $(ROOT_DIR)/build/out
DISTRO	?= DEBIAN

# install environment
PREFIX			?= /
BINDIR			?= $(PREFIX)/bin
LIBDIR			?= $(PREFIX)/lib
INCLUDEDIR		?= $(PREFIX)/include
PKGCONFIGDIR	?= $(PREFIX)/lib/pkgconfig

# protobufs 
BUILD_PROTOBUFS_CPP		:= $(BUILD)/protobufs/cpp/
BUILD_PROTOBUFS_CPP_H	:= $(wildcard $(BUILD)/protobufs/cpp/%.h)
BUILD_PROTOBUFS_PYTHON	:= $(BUILD)/protobufs/python/
BUILD_PROTOBUFS_DIRS		:= $(BUILD_PROTOBUFS_CPP) $(BUILD_PROTOBUFS_PYTHON)

# libsyndicate
BUILD_LIBSYNDICATE		:= $(BUILD)/lib/libsyndicate
BUILD_LIBSYNDICATE_DIRS	:= $(BUILD_LIBSYNDICATE)/ms $(BUILD_LIBSYNDICATE)/scrypt $(BUILD_LIBSYNDICATE)/drivers

# user gateway 
BUILD_UG			:= $(BUILD)/UG
BUILD_UG_TOOLS := $(BUILD_UG)/tools
BUILD_UG_GATEWAYS := $(BUILD_UG)/gateways
BUILD_UG_DIRS	:= $(BUILD_UG_TOOLS) $(BUILD_UG_GATEWAYS)

# compiler
CPP		:= g++ -Wall -g -fPIC -fstack-protector -fstack-protector-all -pthread
INC		:= -I. -I$(ROOT_DIR) -I$(BUILD_PROTOBUFS_CPP) -I/usr/include
DEFS		:= -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_DISTRO_$(DISTRO)
LIBINC	:= -L$(BUILD_LIBSYNDICATE)

# build setup
BUILD_DIRS	:= $(BUILD_PROTOBUFS_DIRS) \
					$(BUILD_LIBSYNDICATE_DIRS) \
					$(BUILD_UG_DIRS)

# misc
ifeq ($(DISTRO),DEBIAN)
	LIBJSON ?= json-c
else
	LIBJSON ?= json
endif

all:

build_setup: $(BUILD_DIRS)

$(BUILD_DIRS):
	mkdir -p $@

# debugging...
print-%: ; @echo $*=$($*)
