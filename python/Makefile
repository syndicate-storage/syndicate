include ../buildconf.mk

SETUP := ./setup.py --distro=$(DISTRO) --build-dir=$(BUILD_PYTHON_SYNDICATE) --source-root=$(ROOT_DIR) 

PYTHON_SYNDICATE_ROOT := $(ROOT_DIR)/python/syndicate
PYTHON_BIN := $(ROOT_DIR)/python/bin
PYTHON_LIBEXEC := $(ROOT_DIR)/python/libexec
PYTHON_SYNDICATE_FILES := $(wildcard $(PYTHON_SYNDICATE_ROOT)/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*/*/*.py) \
						  $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*/config) \
						  $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*/secrets) \
						  $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*/driver) \
						  $(PYTHON_SYNDICATE_ROOT)/util/paths.py

PYTHON_BIN_FILES := $(wildcard $(PYTHON_BIN)/*)

PYTHON_LIBEXEC_FILES := $(wildcard $(PYTHON_LIBEXEC)/*)

PYTHON_SYNDICATE_CYTHON_FILES := $(wildcard *.pyx) $(wildcard *.pxd)

PYTHON_SYNDICATE_PROTOBUF_FILES := $(notdir $(wildcard $(BUILD_PROTOBUFS_PYTHON)/*.py))

PYTHON_SYNDICATE_PACKAGE := $(BUILD_PYTHON_SYNDICATE)/syndicate

PYTHON_BIN_BUILD := $(patsubst $(PYTHON_BIN)/%,$(BUILD_PYTHON_BIN)/%,$(PYTHON_BIN_FILES))
PYTHON_LIBEXEC_BUILD := $(patsubst $(PYTHON_LIBEXEC)/%,$(BUILD_PYTHON_LIBEXEC)/%,$(PYTHON_LIBEXEC_FILES))
PYTHON_BUILD := $(patsubst $(PYTHON_SYNDICATE_ROOT)/%,$(PYTHON_SYNDICATE_PACKAGE)/%,$(PYTHON_SYNDICATE_FILES)) \
                $(patsubst %,$(BUILD_PYTHON_SYNDICATE)/%,$(PYTHON_SYNDICATE_CYTHON_FILES)) \
                $(patsubst %.py,$(PYTHON_SYNDICATE_PACKAGE)/protobufs/%.py,$(PYTHON_SYNDICATE_PROTOBUF_FILES)) \
					 $(PYTHON_BIN_BUILD) \
					 $(PYTHON_LIBEXEC_BUILD)

CYTHON_BUILD := $(patsubst %,$(BUILD_PYTHON_SYNDICATE)/%,$(PYTHON_SYNDICATE_CYTHON_FILES))

# don't include admin_info
MS_FILES_EXCLUDE := $(BUILD_MS)/common/admin_info.py $(BUILD_MS)/common/log.py
MS_FILES := $(filter-out $(MS_FILES_EXCLUDE),$(wildcard $(BUILD_MS)/common/*.py))
MS_FILES_BUILD := $(patsubst $(BUILD_MS)/common/%.py,$(PYTHON_SYNDICATE_PACKAGE)/ms/%.py,$(MS_FILES))

PYTHON_BIN_INSTALL := $(patsubst %,$(INSTALL_PYTHON_BIN)/%,$(notdir $(PYTHON_BIN_BUILD)))
PYTHON_LIBEXEC_INSTALL := $(patsubst %,$(INSTALL_PYTHON_LIBEXEC)/%,$(notdir $(PYTHON_LIBEXEC_BUILD)))

.PHONY: all
all: $(PYTHON_BUILD) $(BUILD_PYTHON_SYNDICATE)/setup.py $(CYTHON_BUILD) $(MS_FILES_BUILD)
	cd $(BUILD_PYTHON_SYNDICATE) && $(SETUP) build && cp -a build/lib.*/syndicate/*.so syndicate/

$(BUILD_PYTHON_SYNDICATE)/setup.py: setup.py 
	@cp "$<" "$@"

$(PYTHON_SYNDICATE_PACKAGE)/util/paths.py: $(PYTHON_SYNDICATE_ROOT)/util/paths.py.in
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" | sed -e 's~@PREFIX@~$(PREFIX)~g' > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/%.py: $(PYTHON_SYNDICATE_ROOT)/%.py $(PYTHON_SYNDICATE_PACKAGE)/util/paths.py
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/%/config: $(PYTHON_SYNDICATE_ROOT)/%/config
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/%/secrets: $(PYTHON_SYNDICATE_ROOT)/%/secrets
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/%/driver: $(PYTHON_SYNDICATE_ROOT)/%/driver
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/ms/%.py: $(BUILD_MS)/common/%.py
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(PYTHON_SYNDICATE_PACKAGE)/protobufs/%.py: $(BUILD_PROTOBUFS_PYTHON)/%.py
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(BUILD_PYTHON_SYNDICATE)/%.pyx: %.pyx
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(BUILD_PYTHON_SYNDICATE)/%.pxd: %.pxd
	@mkdir -p "$(shell dirname "$@")"
	@cat "$<" > "$@"

$(BUILD_PYTHON_BIN)/%: $(PYTHON_BIN)/%
	@mkdir -p "$(shell dirname "$@")"
	@cp "$<" "$@"

$(BUILD_PYTHON_LIBEXEC)/%: $(PYTHON_LIBEXEC)/%
	@mkdir -p "$(shell dirname "$@")"
	@cp -a "$<" "$@"

$(INSTALL_PYTHON_BIN)/%: $(BUILD_PYTHON_BIN)/%
	@mkdir -p "$(shell dirname "$@")"
	@cp -a "$<" "$@"

$(INSTALL_PYTHON_LIBEXEC)/%: $(BUILD_PYTHON_LIBEXEC)/%
	@mkdir -p "$(shell dirname "$@")"
	@cp -a "$<" "$@"

.PHONY: install 
install: $(BUILD_PYTHON_SYNDICATE)/setup.py $(PYTHON_BUILD) $(CYTHON_BUILD) $(PYTHON_BIN_INSTALL) $(PYTHON_LIBEXEC_INSTALL)
	cd $(BUILD_PYTHON_SYNDICATE) && $(SETUP) install --prefix=$(PREFIX)

.PHONY: clean 
clean:
	rm -f $(PYTHON_BUILD) $(CYTHON_BUILD) $(MS_FILES_BUILD)
	rm -rf $(BUILD_PYTHON_SYNDICATE)/build $(BUILD_PYTHON_SYNDICATE)/*.cpp $(BUILD_PYTHON_SYNDICATE)/setup.py

print-%: ; @echo $*=\'$($*)\'
