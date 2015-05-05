include buildconf.mk

protobufs: build_setup
	$(MAKE) -C protobufs

libsyndicate: build_setup
	$(MAKE) -C libsyndicate

UG: build_setup
	$(MAKE) -C UG2

MS: build_setup
	$(MAKE) -C ms 

.PHONY: clean
clean:
	rm -rf $(BUILD)
