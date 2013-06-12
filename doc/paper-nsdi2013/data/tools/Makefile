CC=gcc -g -Wall -I/usr/include/libev/ -I/usr/include
LD=gcc -g

HTTPRESS=httpress
UDPRESS=udpress

###
# Library dependencies:
#  - libev 4 (http://software.schmorp.de/pkg/libev.html)

LIBS=-lev -lpthread -lgnutls -lrt

CFLAGS_RELEASE=-pthread -Wno-strict-aliasing -O2 -s -DWITH_SSL

LDFLAGS_RELEASE=$(LIBS)

all: httpress udpress

httpress: $(HTTPRESS).o
	$(CC) -o $(HTTPRESS) $(LDFLAGS_RELEASE) $(HTTPRESS).o

udpress: $(UDPRESS).o
	$(CC) -o $(UDPRESS) $(LDFLAGS_RELEASE) $(UDPRESS).o

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS_RELEASE)

clean:
	rm -rf *.o $(HTTPRESS) $(UDPRESS)

.PHONY: all clean Release Debug
