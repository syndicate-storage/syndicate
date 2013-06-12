CLIENT_DIR := ../client
MDSERVER_DIR := ../md-server
CONTENT_SERVER_DIR := ../content-server
GATEWAY_DIR := ../content-server/gateway
LIBSYNDICATE_DIR := ../common
LIBMICROHTTPD_DIR := ../libmicrohttpd-syndicate

LIBGATEWAY_SRC 		:= rpm/SOURCES/libgateway
CLIENT_SRC 				:= rpm/SOURCES/syndicatefs
MDSERVER_SRC 			:= rpm/SOURCES/syndicate-md
CONTENT_SERVER_SRC 	:= rpm/SOURCES/syndicate-content
GATEWAY_SRC 			:= rpm/SOURCES/libgateway
LIBMICROHTTPD_SRC		:= rpm/SOURCES/libmicrohttpd-syndicate

ALL_SRPMS	:= rpm/SRPMS/*.rpm
ALL_SOURCES := rpm/SOURCES/*.tar.gz $(LIBGATEWAY_SRC) $(CLIENT_SRC) $(MDSERVER_SRC) $(CONTENT_SERVER_SRC) $(GATEWAY_SRC) $(LIBMICROHTTPD_SRC)

.PHONY : all
all:
	$(MAKE) -C $(LIBMICROHTTPD_DIR) -f Makefile.syndicate rpm
	$(MAKE) -C $(CLIENT_DIR) rpm
	$(MAKE) -C $(MDSERVER_DIR) rpm
	$(MAKE) -C $(CONTENT_SERVER_DIR) rpm
	$(MAKE) -C $(GATEWAY_DIR) rpm
	$(MAKE) -C $(LIBSYNDICATE_DIR) rpm

.PHONY : clean
clean:
	rm -f $(ALL_SRPMS)
	rm -rf $(ALL_SOURCES)

.PHONY : rpm-macros
rpm-macros:
	if ! [ -f ~/.rpmmacros ] then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; done
