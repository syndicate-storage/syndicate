include ../buildconf.mk

all: gateways

.PHONY: gateways
gateways:
	$(MAKE) -C user
	$(MAKE) -C replica
	$(MAKE) -C acquisition

.PHONY: clean
clean:
	$(MAKE) -C user clean
	$(MAKE) -C replica clean
	$(MAKE) -C acquisition clean
