# basic stuff
CPP=g++ -Wall -g 
PROTOC=protoc -I=. --cpp_out=.

# outputs
OUT_FS=syndicatefs
OUT_HTTPD=syndicate-httpd
OUT_TEST=syndicate-test
OUT=$(OUT_FS) $(OUT_HTTPD) $(OUT_TEST)

# useful directory aliases
COMMON=../common
FS=fs
MICROHTTPD=$(COMMON)/libmicrohttpd-syndicate/out/

# library search path
LIBINC=-L$(COMMON)

# common libraries
LIB=-lcurl -lcrypto -lpthread -lrt -lprotobuf -luriparser -lgcrypt -lgnutls

# includes 
INC=-I/usr/include -I$(COMMON) -I$(FS) -I. -I$(MICROHTTPD)/include

# fs operation objects
OBJS_FS=$(patsubst %.cpp,%.o,$(wildcard $(FS)/*.cpp)) $(patsubst %.cc,%.o,$(wildcard $(FS)/*.cc)) $(patsubst %.c,%.o,$(wildcard $(FS)/*.c))

# common objects
OBJS_COMMON=$(COMMON)/libsyndicate.a 

# UG objects
OBJS=collator.o log.o stats.o replication.o syndicate.o http-common.o

# extra #defines
DEFS=-D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE

# install information
DESTDIR = /
BIN_DIR = /usr/local/bin
CONF_DIR = /etc/syndicate/
BIN = syndicatefs syndicate-httpd
CONF = syndicate-client.conf

all: syndicatefs syndicate-httpd

syndicate-test: fs $(OBJS) syndicate-test.o
	$(CPP) -o $(OUT_TEST) syndicate-test.o $(OBJS) $(OBJS_FS) $(OBJS_COMMON) $(LIBINC) $(LIB)

syndicate-httpd: fs $(OBJS) syndicate-httpd.o
	$(CPP) -o $(OUT_HTTPD) syndicate-httpd.o $(OBJS) $(OBJS_FS) $(OBJS_COMMON) $(LIBINC) $(LIB)

syndicatefs: fs $(OBJS) syndicatefs.o
	$(CPP) -o $(OUT_FS) syndicatefs.o $(OBJS) $(OBJS_FS) $(OBJS_COMMON) $(LIBINC) $(LIB) -lfuse 

.PHONY: fs
fs:
	$(MAKE) -C $(FS)

common:
	$(MAKE) -C $(COMMON)

%.o : %.c fs 
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cpp fs
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

%.o : %.cc fs
	$(CPP) -o $@ $(INC) -c $< $(DEFS)

.PHONY : clean
clean:
	/bin/rm -f $(OUT) *.pb.* *.o
	$(MAKE) -C $(FS) clean


.PHONY : test
test:
	/bin/cp  $(DESTDIR)/$(BIN_DIR)

.PHONY : install
install: all
	mkdir -p $(DESTDIR)/$(BIN_DIR)
	mkdir -p $(DESTDIR)/$(CONF_DIR)
	cp  $(BIN) $(DESTDIR)/$(BIN_DIR)
	cp  $(CONF) $(DESTDIR)/$(CONF_DIR)

.PHONY : uninstall
uninstall:
	cd $(DESTDIR)/$(BIN_DIR)/ && rm -f $(BIN)
	cd $(DESTDIR)/$(CONF_DIR)/ && rm -f $(CONF)
	rmdir $(DESTDIR)/$(BIN_DIR) || echo "$(DESTDIR)/$(CONF_DIR) not empty, so not removed"
	rmdir $(DESTDIR)/$(CONF_DIR) || echo "$(DESTDIR)/$(CONF_DIR) not empty, so not removed"

.PHONY : rpm
rpm:
	sh -x rpm.sh
	

# debug: clean up after a crash
.PHONY : crash-clean
crash-clean:
	rm -rf /tmp/syndicate*
