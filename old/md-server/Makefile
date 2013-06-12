CPP			:= g++ -Wall -g -Wno-format 
COMMON			:= ../common 
CLIENT       := ../client
LIBINC		:= -L$(COMMON) 
INC			:= -I/usr/include  -I/usr/include/syndicate -I$(COMMON) -I$(CLIENT)

MDTOOL		:= mdtool
MDSERVERD	:= mdserverd

DESTDIR			:= /

BIN_DIR			:= usr//bin
CONF_DIR			:= etc/syndicate
INIT_DIR       := etc/init.d

MD_CONF			:= etc/syndicate/syndicate-metadata-server.conf
MD_INIT        := etc/init.d/syndicate-md
MD_SAMPLE_PKEY := etc/syndicate/mdctl.key
MD_SAMPLE_CERT := etc/syndicate/mdctl.cert

OUT			:= $(MDTOOL) $(MDSERVERD)
LIB			:= -lpthread -lcurl -lcrypto -lsyndicate -lmicrohttpd -lprotobuf -lrt
DEFS			:= -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE

MD_BUILD		:= ../pkg/rpm/SOURCES/syndicate-md
MD_SPEC		:= ../pkg/rpm/SPECS/syndicate-md.spec
ARCH			:= i386

OBJ			:= blacklist.o validator.o http.o mdserverd.o master_copy.o

all: mdtool mdserverd

mdtool:
	$(CPP) -o $(MDTOOL) mdtool.c $(INC) $(LIBINC) $(LIB) $(DEFS)
	
mdserverd: $(OBJ)
	$(CPP) -o $(MDSERVERD) $(INC) $(LIBINC) $(OBJ) $(LIB) $(DEFS)

%.o: %.c
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o: %.cpp
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY : install
install: all
	mkdir -p $(DESTDIR)/$(BIN_DIR)
	mkdir -p $(DESTDIR)/$(CONF_DIR)
	mkdir -p $(DESTDIR)/$(INIT_DIR)
	/bin/cp -a $(MDSERVERD)	$(DESTDIR)/$(BIN_DIR)
	/bin/cp -a $(MDTOOL)			$(DESTDIR)/$(BIN_DIR)
	/bin/cp -a $(MD_CONF)		$(DESTDIR)/$(MD_CONF)
	/bin/cp -a $(MD_INIT)      $(DESTDIR)/$(MD_INIT)
	/bin/cp -a mdserverd.secrets $(DESTDIR)/$(CONF_DIR)
	/bin/cp -a $(MD_SAMPLE_PKEY) $(DESTDIR)/$(MD_SAMPLE_PKEY)
	/bin/cp -a $(MD_SAMPLE_CERT) $(DESTDIR)/$(MD_SAMPLE_CERT)

.PHONY : uninstall
uninstall:
	/bin/rm -f $(DESTDIR)/$(BIN_DIR)/$(MDSERVERD)
	/bin/rm -f $(DESTDIR)/$(BIN_DIR)/$(MDTOOL)
	/bin/rm -f $(DESTDIR)/$(MD_INIT)
	/bin/rm -f $(DESTDIR)/$(CONF_DIR)/mdserverd.secrets
	/bin/rmdir $(DESTDIR)/$(CONF_DIR) || echo "$(DESTDIR)/$(CONF_DIR) not empty, so not removed"

.PHONY : rpm
rpm:
	rm -rf $(MD_BUILD)
	mkdir -p $(MD_BUILD)/syndicate-md/$(BIN_DIR)
	mkdir -p $(MD_BUILD)/syndicate-md/$(CONF_DIR)
	mkdir -p $(MD_BUILD)/syndicate-md/$(INIT_DIR)
	/bin/cp *.c *.cpp *.h $(MD_BUILD)/syndicate-md/
	/bin/cp Makefile.plc $(MD_BUILD)/syndicate-md/Makefile
	/bin/cp $(MD_CONF) $(MD_BUILD)/syndicate-md/$(CONF_DIR)
	/bin/cp $(MD_INIT) $(MD_BUILD)/syndicate-md/$(INIT_DIR)
	tar cf $(MD_BUILD).tar -C $(MD_BUILD) syndicate-md
	gzip -f $(MD_BUILD).tar
	setarch $(ARCH) rpmbuild -bs $(MD_SPEC)


.PHONY : clean
clean:
	/bin/rm -f $(OUT) *.o

.PHONY : crash-clean
crash-clean:
	/bin/rm -f /tmp/mdserverd.soc
