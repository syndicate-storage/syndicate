#!/usr/bin/python

import os
import sys
import lxml 
from lxml import etree
import math 

class StatsCounter(object):

    prefixes = {}
    cur_tag = None

    def start( self, tag, attrib ):
        self.cur_tag = tag

    def end( self, tag ):
        pass
        #self.cur_tag = None 

    def data( self, _data ):
        if self.cur_tag != "File" and self.cur_tag != "Dir":
            return 

        data = _data.rstrip("/")
        if data == "":
            return 

        dir_name = os.path.dirname( data )
        if dir_name == "":
            return 

        if not self.prefixes.has_key( dir_name ):
            self.prefixes[ dir_name ] = 0

        self.prefixes[ dir_name ] += 1

    def close( self ):
        return "closed!"

if __name__ == "__main__":

    counter = StatsCounter()
    parser = etree.XMLParser( target=counter )

    fd = open( sys.argv[1], "r" )
    
    while True:
        buf = fd.read( 32768 )
        if len(buf) == 0:
            break

        parser.feed( buf )

    result = parser.close()

    order = counter.prefixes.keys()
    order.sort()

    size_bins = {}

    for path in order:
        count = counter.prefixes[path]
        print "% 15s %s" % (count, path)

        size_bin = int(math.log(count, 10))
        
        if not size_bins.has_key( size_bin ):
            size_bins[ size_bin ] = 1

        else:
            size_bins[ size_bin ] += 1

    print ""
    print "sizes"
    max_bin = max( size_bins.keys() )

    bin_fmt = r"1e%0" + str( int(math.log(max_bin, 10)) + 1 ) + "s"

    for size in xrange( 0, max_bin + 1 ):
        binsize = 0
        if size_bins.has_key( size ):
            binsize = size_bins[size]

        bin_str = bin_fmt % size
        print "%s %s" % (bin_str, binsize)


    
