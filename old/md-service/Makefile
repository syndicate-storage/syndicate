DESTDIR=/
CONFIGS=default-md-server.conf default-md-server.template syndicate-metadata-service.conf
SERVICE_INSTALLABLES=*.py *.sql SMDS test tools
SLAVE_INSTALLABLES=SMDS

all:


install-basic:
	/bin/mkdir -p $(DESTDIR)/etc/syndicate
	/bin/mkdir -p $(DESTDIR)/usr/share/SMDS
	/bin/cp -a etc/syndicate/* $(DESTDIR)/etc/syndicate

install: install-basic
	/bin/cp -a $(SERVICE_INSTALLABLES) $(DESTDIR)/usr/share/SMDS
	/bin/ln -s $(DESTDIR)/usr/share/SMDS/SMDS/mdctl.py /usr/bin/mdctl.py

slave-install: install-basic
	/bin/cp -a $(SLAVE_INSTALLABLES) $(DESTDIR)/usr/share/SMDS

uninstall:
	/bin/rm -rf $(DESTDIR)/usr/share/SMDS
	/bin/rm -f $(DESTDIR)/usr/bin/mdctl.py

clean:


