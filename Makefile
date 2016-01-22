include buildconf.mk

# BROKEN; do not use 

all:
	$(MAKE) -C protobufs 

protobufs: build_setup
	$(MAKE) -C protobufs

libsyndicate: build_setup
	$(MAKE) -C libsyndicate

.PHONY: libsyndicate-install
libsyndicate-install: libsyndicate
	$(MAKE) -C libsyndicate install

UG: build_setup
	$(MAKE) -C UG2

RG: build_setup
	$(MAKE) -C RG2

MS: build_setup
	$(MAKE) -C ms 

.PHONY: clean
clean:
	rm -rf $(BUILD)
