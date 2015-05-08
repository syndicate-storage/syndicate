# build environment
ROOT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUILD    ?= $(ROOT_DIR)/build/out
DISTRO   ?= DEBIAN
BUILD_BINDIR := $(BUILD)/bin
BUILD_LIBDIR := $(BUILD)/lib
BUILD_INCLUDEDIR := $(BUILD)/include/

# install environment
PREFIX         ?= /usr/local
BINDIR         ?= $(PREFIX)/bin
LIBDIR         ?= $(PREFIX)/lib
INCLUDEDIR     ?= $(PREFIX)/include
PKGCONFIGDIR   ?= $(PREFIX)/lib/pkgconfig

# protobufs 
BUILD_PROTOBUFS_CPP     := $(BUILD)/protobufs/cpp/
BUILD_PROTOBUFS_PYTHON  := $(BUILD)/protobufs/python/
BUILD_PROTOBUFS_INCLUDEDIR := $(BUILD_INCLUDEDIR)/libsyndicate
BUILD_PROTOBUFS_DIRS    := $(BUILD_PROTOBUFS_CPP) $(BUILD_PROTOBUFS_PYTHON)

# metadata service 
BUILD_MS                := $(BUILD)/ms
BUILD_MS_DIRS           := $(BUILD_MS)/common \
                           $(BUILD_MS)/protobufs \
                           $(BUILD_MS)/storage \
                           $(BUILD_MS)/storage/backends \
                           $(BUILD_MS)/MS \
                           $(BUILD_MS)/MS/methods \
                           $(BUILD_MS)/google \
                           $(BUILD_MS)/google/protobuf \
                           $(BUILD_MS)/google/protobuf/internal \
                           $(BUILD_MS)/google/protobuf/compiler \
                           $(BUILD_MS)/openid \
                           $(BUILD_MS)/openid/server \
                           $(BUILD_MS)/openid/extensions \
                           $(BUILD_MS)/openid/extensions/draft \
                           $(BUILD_MS)/openid/store \
                           $(BUILD_MS)/openid/consumer \
                           $(BUILD_MS)/openid/yadis

# libsyndicate
BUILD_LIBSYNDICATE       := $(BUILD_LIBDIR)
BUILD_LIBSYNDICATE_INCLUDEDIR := $(BUILD_INCLUDEDIR)/libsyndicate
BUILD_LIBSYNDICATE_DIRS  := $(BUILD_LIBSYNDICATE)/ms \
	                         $(BUILD_LIBSYNDICATE)/scrypt \
									 $(BUILD_LIBSYNDICATE)/drivers \
                            $(BUILD_LIBSYNDICATE_INCLUDEDIR) \
									 $(BUILD_LIBSYNDICATE_INCLUDEDIR)/ms \
									 $(BUILD_LIBSYNDICATE_INCLUDEDIR)/scrypt \
									 $(BUILD_LIBSYNDICATE_INCLUDEDIR)/drivers

# user gateway 
BUILD_UG          := $(BUILD)/UG
BUILD_UG_TOOLS    := $(BUILD_UG)/tools
BUILD_UG_GATEWAYS := $(BUILD_UG)/gateways
BUILD_UG_INCLUDEDIR := $(BUILD_INCLUDEDIR)/syndicate-ug
BUILD_UG_DIRS     := $(BUILD_UG_TOOLS) $(BUILD_UG_GATEWAYS) $(BUILD_UG_INCLUDEDIR)

# compiler
CPPFLAGS := -Wall -g -fPIC -fstack-protector -fstack-protector-all -pthread
CPP      := g++ $(CPPFLAGS)
INC      := -I. -I$(ROOT_DIR) -I$(BUILD_INCLUDEDIR)
DEFS     := -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_DISTRO_$(DISTRO)
LIBINC   := -L$(BUILD_LIBSYNDICATE)

# build setup
BUILD_DIRS   := $(BUILD_PROTOBUFS_DIRS) \
                $(BUILD_MS_DIRS) \
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
	@mkdir -p $@

# debugging...
print-%: ; @echo $*=$($*)
