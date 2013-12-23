"""
Author: Justin Samuel <jsamuel@cs.arizona.edu>
Date: 2008-03-19

This is the mod_python index file for this Raven demo.

This script will require:
mysql
mod_python
python-MySQLdb
python-json

Instructions:
Make the gec4demo directory readable/accessible by apache
Copy gec4demo.conf to /etc/httpd/conf.d (changing the directory path in the file)
Create a mysql database with the schema provided.
Set the mysql user/pass/dbname in this file.

Once the above steps are completed, you can perform the following requests:

View the map:
http://quake.cs.arizona.edu/gec4demo/

Add a point:
http://quake.cs.arizona.edu/gec4demo/notify?lat=33.123&long=77.0&site=test_site&version=1

View the data:
http://quake.cs.arizona.edu/gec4demo/data

Reset all data:
http://quake.cs.arizona.edu/gec4demo/reset

View debugging info:
http://quake.cs.arizona.edu/gec4demo/debug
"""

from mod_python import apache, psp
from cgi import escape
from urllib import unquote
import json
import MySQLdb

# DB connection info (fill this in)
dbhost = "localhost"
dbuser = ""
dbpasswd = ""
dbname = ""

# Database connection
conn = None

# Database "cursor"
cursor = None

def index(req):
    req.content_type = "text/html"
    tmpl = psp.PSP(req, filename="templates/index.html")
    tmpl.run()

def notify(req):
    req.content_type = "text/html"
    lat = req.form.getfirst("lat", "")
    long = req.form.getfirst("long", "")
    site = req.form.getfirst("site", "")
    version = req.form.getfirst("version", "")
    dbConnect()
    addData(lat, long, site, version)
    dbClose()
    req.write("OK");

def data(req):
    req.content_type = "text/plain"
    dbConnect()
    req.write(json.write(getData()))
    dbClose()

def debug(req):
    req.content_type = "text/plain"
    dbConnect()
    d = getData()
    req.write("Node count: " + str(len(d)) + "\n\n")
    for val in d:
        req.write(str(val) + "\n")
    dbClose()

def reset(req):
    dbConnect()
    cursor.execute("TRUNCATE nodes")
    dbClose()
    req.write("OK");

def getData():
    global cursor
    cursor.execute("SELECT latitude, longitude, site, version FROM nodes ORDER BY latitude, longitude, site")
    return cursor.fetchall()

def addData(lat, long, site, version):
    global cursor
    cursor.execute("SELECT * FROM nodes WHERE latitude = %s AND longitude = %s AND site = %s",
        (float(lat), float(long), str(site)))
    if cursor.rowcount > 0:
        cursor.execute(
            "UPDATE nodes SET version = %s WHERE latitude = %s AND longitude = %s AND site = %s", 
            (int(version), float(lat), float(long), str(site)))
    else: 
        cursor.execute(
            "INSERT INTO nodes (latitude, longitude, site, version) VALUES (%s, %s, %s, %s)", 
            (float(lat), float(long), str(site), int(version)))

def dbConnect():
    global conn, cursor, dbhost, dbuser, dbpasswd, dbname
    try:
        conn = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpasswd, db=dbname)
        cursor = conn.cursor()
    except MySQLdb.Error, e:
        #print "Error %d: %s" % (e.args[0], e.args[1])
	raise

def dbClose():
    global conn, cursor
    try:
        cursor.close()
        conn.close()
    except MySQLdb.Error, e:
        #print "Error %d: %s" % (e.args[0], e.args[1])
	raise

