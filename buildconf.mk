# build environment
ROOT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUILD    ?= $(ROOT_DIR)/build/out
DISTRO   ?= DEBIAN
BUILD_BINDIR := $(BUILD)/bin
BUILD_LIBDIR := $(BUILD)/lib
BUILD_LIBEXEC_DIR := $(BUILD)/lib/syndicate
BUILD_INCLUDEDIR := $(BUILD)/include/

# install environment
PREFIX         ?= /usr/local
DESTDIR			?= /
BINDIR         ?= $(PREFIX)/bin
LIBDIR         ?= $(PREFIX)/lib
LIBEXECDIR     ?= $(PREFIX)/lib/syndicate
INCLUDEDIR     ?= $(PREFIX)/include
PKGCONFIGDIR   ?= $(PREFIX)/lib/pkgconfig

# protobufs 
BUILD_PROTOBUFS_CPP     := $(BUILD)/protobufs/cpp/
BUILD_PROTOBUFS_OBJ     := $(BUILD)/protobufs/obj
BUILD_PROTOBUFS_PYTHON  := $(BUILD)/protobufs/python/
BUILD_PROTOBUFS_INCLUDEDIR := $(BUILD_INCLUDEDIR)/libsyndicate
BUILD_PROTOBUFS_DIRS    := $(BUILD_PROTOBUFS_CPP) $(BUILD_PROTOBUFS_PYTHON)

# metadata service 
BUILD_MS                := $(BUILD)/ms
BUILD_MS_TOOLS			:= $(BUILD_BINDIR)
BUILD_MS_DIRS           := $(BUILD_MS)/common \
                           $(BUILD_MS)/protobufs \
                           $(BUILD_MS)/storage \
                           $(BUILD_MS)/storage/backends \
                           $(BUILD_MS)/MS \
                           $(BUILD_MS)/MS/methods \
                           $(BUILD_MS)/google \
                           $(BUILD_MS)/google/protobuf \
                           $(BUILD_MS)/google/protobuf/internal \
                           $(BUILD_MS)/google/protobuf/compiler

# libsyndicate
BUILD_LIBSYNDICATE       := $(BUILD_LIBDIR)
BUILD_LIBSYNDICATE_INCLUDEDIR := $(BUILD_INCLUDEDIR)/libsyndicate
BUILD_LIBSYNDICATE_DIRS  := $(BUILD_LIBSYNDICATE)/ms \
                            $(BUILD_LIBSYNDICATE)/drivers \
                            $(BUILD_LIBSYNDICATE_INCLUDEDIR) \
                            $(BUILD_LIBSYNDICATE_INCLUDEDIR)/ms \
                            $(BUILD_LIBSYNDICATE_INCLUDEDIR)/drivers

# libsyndicate-ug
BUILD_LIBSYNDICATE_UG          := $(BUILD_LIBDIR)
BUILD_LIBSYNDICATE_UG_INCLUDEDIR := $(BUILD_INCLUDEDIR)/libsyndicate-ug
BUILD_LIBSYNDICATE_UG_DIRS     := $(BUILD_LIBSYNDICATE_UG_INCLUDEDIR)

# user gateway 
BUILD_UG          := $(BUILD_BINDIR)
BUILD_UG_TOOLS    := $(BUILD_BINDIR)
BUILD_UG_GATEWAYS := $(BUILD_BINDIR)
BUILD_UG_DIRS     := $(BUILD_UG_TOOLS) \
                     $(BUILD_UG_GATEWAYS)

# replica gateway 
BUILD_RG          := $(BUILD_BINDIR)
BUILD_RG_DIRS     := $(BUILD_RG)

# acquisition gateway
BUILD_AG          := $(BUILD_BINDIR)
BUILD_AG_DIRS     := $(BUILD_AG)

# automount daemon 
BUILD_AMD		  := $(BUILD_BINDIR)
BUILD_AMD_DIRS    := $(BUILD_AMD)

# python extension
BUILD_PYTHON_SYNDICATE := $(BUILD)/python/
BUILD_PYTHON_SYNDICATE_DIRS := $(BUILD_PYTHON_SYNDICATE)
INSTALL_PYTHON_BIN := $(DESTDIR)$(PREFIX)/bin
INSTALL_PYTHON_LIBEXEC := $(DESTDIR)$(PREFIX)/lib/syndicate

# python tools 
BUILD_PYTHON_BIN := $(BUILD_BINDIR)
BUILD_PYTHON_LIBEXEC := $(BUILD_LIBEXEC_DIR)

# compiler
CPPFLAGS := -std=c++11 -Wall -g -fPIC -fstack-protector -fstack-protector-all -pthread
CPP      := c++ $(CPPFLAGS)
INC      := -I. -I$(BUILD_INCLUDEDIR) -I$(ROOT_DIR)
DEFS     := -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_DISTRO_$(DISTRO)
LIBINC   := -L$(BUILD_LIBDIR)

# build setup
BUILD_DIRS   := $(sort $(BUILD_PROTOBUFS_DIRS) \
                $(BUILD_MS_DIRS) \
                $(BUILD_LIBSYNDICATE_DIRS) \
                $(BUILD_UG_DIRS) \
					 $(BUILD_RG_DIRS) \
                $(BUILD_PYTHON_SYNDICATE_DIRS))

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
