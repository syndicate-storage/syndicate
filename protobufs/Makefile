include ../buildconf.mk

PROTOC		:= protoc --cpp_out=$(BUILD_PROTOBUFS_CPP) --python_out=$(BUILD_PROTOBUFS_PYTHON)
PROTO_DEFS	:= $(wildcard *.proto)

all:
	$(PROTOC) $(PROTO_DEFS)

.PHONY: clean
clean:
	rm -f $(BUILD_PROTOBUFS_CPP)/* $(BUILD_PROTOBUFS_PYTHON)/*

