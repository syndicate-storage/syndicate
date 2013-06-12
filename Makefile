UG=./UG
COMMON=./common

all: syndicate 

.PHONY : syndicate
syndicate: UG common

.PHONY : UG
UG: common
	$(MAKE) -C $(UG) all

.PHONY : common
common:
	$(MAKE) -C $(COMMON) all

.PHONY : install
install: syndicate
	$(MAKE) -C $(UG) install

.PHONY : package
package: syndicate
	$(MAKE) -C $(UG) package

.PHONY : uninstall
uninstall:
	$(MAKE) -C $(UG) uninstall

.PHONY : clean
clean:
	$(MAKE) -C $(UG) clean
	$(MAKE) -C $(COMMON) clean-all
