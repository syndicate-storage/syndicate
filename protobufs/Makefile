include ../buildconf.mk

PROTOC     := protoc 
PROTO_DEFS := $(wildcard *.proto)

PROTO_PY   := $(patsubst %.proto,$(BUILD_PROTOBUFS_PYTHON)/%_pb2.py,$(PROTO_DEFS))
PROTO_CPP  := $(patsubst %.proto,$(BUILD_PROTOBUFS_CPP)/%.pb.cc,$(PROTO_DEFS))
PROTO_CPP_H := $(patsubst %.cc,%.h,$(PROTO_CPP))
PROTO_OBJ  := $(patsubst %.cc,%.o,$(PROTO_CPP))

PROTO_HEADERS := $(patsubst %.proto,$(BUILD_PROTOBUFS_INCLUDEDIR)/%.pb.h,$(PROTO_DEFS))

PROTO_BUILD_DIRS := $(BUILD_PROTOBUFS_DIRS) $(BUILD_LIBSYNDICATE_INCLUDEDIR)

PROTO_HEADERS_INSTALL := $(patsubst $(BUILD_PROTOBUFS_INCLUDEDIR)/%.h,$(INCLUDEDIR)/libsyndicate/%.h,$(PROTO_HEADERS))

all: $(PROTO_PY) $(PROTO_OBJ) $(PROTO_HEADERS)

$(BUILD_PROTOBUFS_CPP)/%.o: $(BUILD_PROTOBUFS_CPP)/%.cc $(PROTO_CPP_H)
	$(CPP) -o "$@" $(INC) -c "$<" $(DEFS)

$(BUILD_PROTOBUFS_CPP)/%.pb.cc $(BUILD_PROTOBUFS_CPP)/%.pb.h: %.proto
	@mkdir -p "$(shell dirname "$@")"
	$(PROTOC) --cpp_out=$(BUILD_PROTOBUFS_CPP) "$<"

$(BUILD_PROTOBUFS_PYTHON)/__init__.py:
	@mkdir -p "$(shell dirname "$@")"
	@touch "$@"

$(BUILD_PROTOBUFS_PYTHON)/%_pb2.py: %.proto $(BUILD_PROTOBUFS_PYTHON)/__init__.py
	@mkdir -p "$(shell dirname "$@")"
	$(PROTOC) --python_out=$(BUILD_PROTOBUFS_PYTHON) "$<"

$(BUILD_PROTOBUFS_INCLUDEDIR)/%.pb.h: $(BUILD_PROTOBUFS_CPP)/%.pb.h
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

.PHONY: install
install: $(PROTO_HEADERS_INSTALL)
$(INCLUDEDIR)/libsyndicate/%.h: $(BUILD_PROTOBUFS_INCLUDEDIR)/%.h
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

.PHONY: clean
clean:
	rm -rf $(BUILD_PROTOBUFS_DIRS) $(PROTO_HEADERS)

.PHONY: uninstall
uninstall:
	rm -rf $(PROTO_HEADERS_INSTALL)

print-%: ; @echo $*=$($*)
