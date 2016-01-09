include ../../buildconf.mk

UG_DRIVER := ug-driver

$(BUILD_LIBEXEC_DIR)/%: libexec/%
	@mkdir -p "$(shell dirname "$@")"
	cp -a "$<" "$@"

$(LIBEXECDIR)/%: $(BUILD_LIBEXEC_DIR)/%
	@mkdir -p "$(shell dirname "$@")"
	cp -a "$<" "$@"

all: $(BUILD_LIBEXEC_DIR)/$(UG_DRIVER)
	$(MAKE) -C tools 

.PHONY: install
install: $(LIBEXECDIR)/$(UG_DRIVER)
	$(MAKE) -C tools install

.PHONY: clean 
clean:
	$(MAKE) -C tools clean 
	rm -f $(BUILD_LIBEXEC_DIR)/$(UG_DRIVER)

PHONY: uninstall
uninstall:
	$(MAKE) -C tools uninstall 
	rm -f $(LIBEXECDIR)/$(UG_DRIVER)

print-%: ; @echo $*=$($*)
