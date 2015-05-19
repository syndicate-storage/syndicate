include ../buildconf.mk

SETUP := ./setup.py --distro=$(DISTRO) --build-dir=$(BUILD_PYTHON_SYNDICATE) --source-root=$(ROOT_DIR) 

PYTHON_SYNDICATE_ROOT := syndicate
PYTHON_SYNDICATE_FILES := $(wildcard $(PYTHON_SYNDICATE_ROOT)/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*.py) \
                          $(wildcard $(PYTHON_SYNDICATE_ROOT)/*/*/*.py) 

PYTHON_SYNDICATE_CYTHON_FILES := $(wildcard *.pyx) $(wildcard *.pxd)

PYTHON_SYNDICATE_PACKAGE := $(BUILD_PYTHON_SYNDICATE)/syndicate

PYTHON_BUILD := $(patsubst $(PYTHON_SYNDICATE_ROOT)/%,$(PYTHON_SYNDICATE_PACKAGE)/%,$(PYTHON_SYNDICATE_FILES)) \
                $(patsubst %,$(PYTHON_SYNDICATE_PACKAGE)/%,$(PYTHON_SYNDICATE_CYTHIN_FILES))

CYTHON_BUILD := $(patsubst %,$(BUILD_PYTHON_SYNDICATE)/%,$(PYTHON_SYNDICATE_CYTHON_FILES))

.PHONY: all
all: $(PYTHON_BUILD) $(BUILD_PYTHON_SYNDICATE)/setup.py $(CYTHON_BUILD)
	cd $(BUILD_PYTHON_SYNDICATE) && $(SETUP) build 

$(BUILD_PYTHON_SYNDICATE)/setup.py: setup.py 
	@cp $< $@

$(PYTHON_SYNDICATE_PACKAGE)/%.py: $(PYTHON_SYNDICATE_ROOT)/%.py
	@mkdir -p $(shell dirname $@)
	@cat $< > $@

$(BUILD_PYTHON_SYNDICATE)/%.pyx: %.pyx
	@mkdir -p $(shell dirname $@)
	@cat $< > $@

$(BUILD_PYTHON_SYNDICATE)/%.pxd: %.pxd
	@mkdir -p $(shell dirname $@)
	@cat $< > $@

.PHONY: install 
install: $(BUILD_PYTHON_SYNDICATE)/setup.py $(PYTHON_BUILD) $(CYTHON_BUILD)
	cd $(BUILD_PYTHON_SYNDICATE) && $(SETUP) install -n --prefix=$(PREFIX)

.PHONY: clean 
clean:
	rm -f $(PYTHON_BUILD) $(CYTHON_BUILD)
	rm -rf $(BUILD_PYTHON_SYNDICATE)/build $(BUILD_PYTHON_SYNDICATE)/*.cpp $(BUILD_PYTHON_SYNDICATE)/setup.py

print-%: ; @echo $*=$($*)
