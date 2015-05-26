include ../buildconf.mk

SETUP := ./setup.py --distro=$(DISTRO) --build-dir=$(BUILD_PYTHON_SYNDICATE) --source-root=$(ROOT_DIR) 

PYTHON_SYNDICATE_ROOT := syndicate
PYTHON_SYNDICATE_FILES := $(wildcard $(PYTHON_SYNDICATE_ROOT)/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*/*.py)  

PYTHON_SYNDICATE_CYTHON_FILES := $(wildcard *.pyx) $(wildcard *.pxd)

PYTHON_SYNDICATE_PROTOBUF_FILES := $(notdir $(wildcard $(BUILD_PROTOBUFS_PYTHON)/*.py))

PYTHON_SYNDICATE_PACKAGE := $(BUILD_PYTHON_SYNDICATE)/syndicate

PYTHON_BUILD := $(patsubst $(PYTHON_SYNDICATE_ROOT)/%,$(PYTHON_SYNDICATE_PACKAGE)/%,$(PYTHON_SYNDICATE_FILES)) \
                $(patsubst %,$(BUILD_PYTHON_SYNDICATE)/%,$(PYTHON_SYNDICATE_CYTHON_FILES)) \
                $(patsubst %.py,$(PYTHON_SYNDICATE_PACKAGE)/protobufs/%.py,$(PYTHON_SYNDICATE_PROTOBUF_FILES))

CYTHON_BUILD := $(patsubst %,$(BUILD_PYTHON_SYNDICATE)/%,$(PYTHON_SYNDICATE_CYTHON_FILES))

# don't include admin_info
MS_FILES := $(filter-out $(BUILD_MS)/common/admin_info.py,$(wildcard $(BUILD_MS)/common/*.py))
MS_FILES_BUILD := $(patsubst $(BUILD_MS)/common/%.py,$(PYTHON_SYNDICATE_PACKAGE)/ms/%.py,$(MS_FILES))

PYTHON_BIN := $(wildcard $(PYTHON_SYNDICATE_ROOT)/ms/syntool.py)
PYTHON_BIN_BUILD := $(patsubst %,$(BUILD_BINDIR)/%,$(notdir $(PYTHON_BIN)))
PYTHON_BIN_INSTALL := $(patsubst %,$(BINDIR)/%,$(notdir $(PYTHON_BIN_BUILD)))

.PHONY: all
all: $(PYTHON_BUILD) $(BUILD_PYTHON_SYNDICATE)/setup.py $(CYTHON_BUILD) $(MS_FILES_BUILD) $(PYTHON_BIN_BUILD)
	cd $(BUILD_PYTHON_SYNDICATE) && $(SETUP) build 

$(BUILD_PYTHON_SYNDICATE)/setup.py: setup.py 
	@cp "$<" "$@"

$(PYTHON_SYNDICATE_PACKAGE)/%.py: $(PYTHON_SYNDICATE_ROOT)/%.py
	@mkdir -p $(shell dirname "$@")
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/ms/%.py: $(BUILD_MS)/common/%.py
	@mkdir -p $(shell dirname "$@")
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/protobufs/%.py: $(BUILD_PROTOBUFS_PYTHON)/%.py
	@mkdir -p $(shell dirname "$@")
	@cat "$<" > "$@"

$(BUILD_PYTHON_SYNDICATE)/%.pyx: %.pyx
	@mkdir -p $(shell dirname "$@")
	@cat "$<" > "$@"

$(BUILD_PYTHON_SYNDICATE)/%.pxd: %.pxd
	@mkdir -p $(shell dirname "$@")
	@cat "$<" > "$@"

$(BUILD_BINDIR)/%.py: $(PYTHON_SYNDICATE_ROOT)/ms/%.py
	@mkdir -p $(shell dirname "$@")
	@cp -a "$<" "$@"

$(BINDIR)/%.py: $(BUILD_BINDIR)/%.py
	cp -a "$<" "$@"

.PHONY: install 
install: $(BUILD_PYTHON_SYNDICATE)/setup.py $(PYTHON_BUILD) $(CYTHON_BUILD) $(PYTHON_BIN_INSTALL)
	cd $(BUILD_PYTHON_SYNDICATE) && $(SETUP) install --prefix=$(DESTDIR)

.PHONY: clean 
clean:
	rm -f $(PYTHON_BUILD) $(CYTHON_BUILD)
	rm -rf $(BUILD_PYTHON_SYNDICATE)/build $(BUILD_PYTHON_SYNDICATE)/*.cpp $(BUILD_PYTHON_SYNDICATE)/setup.py

print-%: ; @echo $*=$($*)
