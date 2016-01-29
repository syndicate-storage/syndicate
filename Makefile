include buildconf.mk

all: syndicate

syndicate: protobufs libsyndicate libsyndicate-ug gateways ms syndicate-python

.PHONY: protobufs
protobufs:
	$(MAKE) -C protobufs

.PHONY: libsyndicate
libsyndicate: protobufs
	$(MAKE) -C libsyndicate

.PHONY: libsyndicate-ug
libsyndicate-ug: libsyndicate
	$(MAKE) -C libsyndicate-ug

.PHONY: gateways
gateways: libsyndicate libsyndicate-ug
	$(MAKE) -C gateways

.PHONY: ms
ms: protobufs
	$(MAKE) -C ms

.PHONY: syndicate-python
syndicate-python: protobufs ms libsyndicate-ug
	$(MAKE) -C python

.PHONY: clean
clean:
	$(MAKE) -C libsyndicate clean
	$(MAKE) -C protobufs clean
	$(MAKE) -C libsyndicate-ug clean
	$(MAKE) -C gateways clean
	$(MAKE) -C ms clean
	$(MAKE) -C python clean

