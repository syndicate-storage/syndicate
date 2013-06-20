""" 
dnlibs.py: library downloader for SCons

This program parses xml file and downloads via HTTP

Parsing XML requires elementtree python library
"""

__author__ = "Illyoung Choi"

import os
import os.path
import urllib2
import elementtree.ElementTree as ET

def checkFileExist(targetPath):
    return os.path.exists(targetPath)

def downloadDependencies(downlistfile, targetPath):
    tree = ET.parse(downlistfile)
    root = tree.getroot()

    for libElement in root.findall('library'):
        libname = libElement.find('name').text
        urlname = libElement.find('url').text
        filename = libElement.find('filename').text
        savepath = targetPath + filename

        print "Check Library : ", libname
        if checkFileExist(savepath):
            print libname, " exist."
        else:
            print "Download Library : ", libname
            print "==> From : ", urlname

            request = urllib2.Request(urlname)
            response = urllib2.urlopen(request)
    
            output = open(savepath, "wb")
            output.write(response.read())
            output.close()

            print "==> Saved to : ", savepath

    return



