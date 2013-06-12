"""
Borrowed from PlanetLab's PLCAPI.  Modifications by Jude Nelson
"""

#
# PostgreSQL database interface. Sort of like DBI(3) (Database
# independent interface for Perl).
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2006 The Trustees of Princeton University
#
# $Id: PostgreSQL.py 18344 2010-06-22 18:56:38Z caglar $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/trunk/PLC/PostgreSQL.py $
#

import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
# UNICODEARRAY not exported yet
psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)

import pgdb
from types import StringTypes, NoneType
import traceback
import commands
import re
from pprint import pformat

import SMDS.logger as log
from SMDS.faults import *

class PostgreSQL:
    def __init__(self, api):
        self.api = api
        self.debug = False
#        self.debug = True
        self.connection = None

    def cursor(self):
        if self.connection is None:
            # (Re)initialize database connection
            try:
                # Try UNIX socket first
                self.connection = psycopg2.connect(user = self.api.config.MD_DB_USER,
                                                   password = self.api.config.MD_DB_PASSWORD,
                                                   database = self.api.config.MD_DB_NAME)
            except psycopg2.OperationalError:
                # Fall back on TCP
                self.connection = psycopg2.connect(user = self.api.config.MD_DB_USER,
                                                   password = self.api.config.MD_DB_PASSWORD,
                                                   database = self.api.config.MD_DB_NAME,
                                                   host = self.api.config.MD_DB_HOST,
                                                   port = self.api.config.MD_DB_PORT)
            self.connection.set_client_encoding("UNICODE")

        (self.rowcount, self.description, self.lastrowid) = \
                        (None, None, None)

        return self.connection.cursor()

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    # join insists on getting strings
    @classmethod
    def quote_string(self, value):
        return str(PostgreSQL.quote(value))

    @classmethod
    def quote(self, value):
        """
        Returns quoted version of the specified value.
        """

        # The pgdb._quote function is good enough for general SQL
        # quoting, except for array types.
        if isinstance(value, (list, tuple, set)):
            return "ARRAY[%s]" % ", ".join(map (PostgreSQL.quote_string, value))
        else:
            ret = None
            if isinstance(value, str):
               ret = "'" + pgdb.escape_string( value ) + "'"
            else:
               ret = pgdb.escape_string( str(value) )
            return ret

    @classmethod
    def param(self, name, value):
        # None is converted to the unquoted string NULL
        if isinstance(value, NoneType):
            conversion = "s"
        # True and False are also converted to unquoted strings
        elif isinstance(value, bool):
            conversion = "s"
        elif isinstance(value, float):
            conversion = "f"
        elif not isinstance(value, StringTypes):
            conversion = "d"
        else:
            conversion = "s"

        return '%(' + name + ')' + conversion

    def begin_work(self):
        # Implicit in pgdb.connect()
        pass

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def do(self, query, params = None):
        cursor = self.execute(query, params)
        cursor.close()
        return self.rowcount

    def next_id(self, table_name, primary_key):
        sequence = "%(table_name)s_%(primary_key)s_seq" % locals()
        sql = "SELECT nextval('%(sequence)s')" % locals()
        rows = self.selectall(sql, hashref = False)
        if rows:
            return rows[0][0]

        return None

    def last_insert_id(self, table_name, primary_key):
        if isinstance(self.lastrowid, int):
            sql = "SELECT %s FROM %s WHERE oid = %d" % \
                  (primary_key, table_name, self.lastrowid)
            rows = self.selectall(sql, hashref = False)
            if rows:
                return rows[0][0]

        return None

    # modified for psycopg2-2.0.7
    # executemany is undefined for SELECT's
    # see http://www.python.org/dev/peps/pep-0249/
    # accepts either None, a single dict, a tuple of single dict - in which case it execute's
    # or a tuple of several dicts, in which case it executemany's
    def execute(self, query, params = None):

        cursor = self.cursor()
        try:

            # psycopg2 requires %()s format for all parameters,
            # regardless of type.
            # this needs to be done carefully though as with pattern-based filters
            # we might have percents embedded in the query
            # so e.g. GetPersons({'email':'*fake*'}) was resulting in .. LIKE '%sake%'
            if psycopg2:
                query = re.sub(r'(%\([^)]*\)|%)[df]', r'\1s', query)
            # rewrite wildcards set by Filter.py as '***' into '%'
            query = query.replace ('***','%')

            if not params:
                if self.debug:
                    print >> log,'execute0',query
                cursor.execute(query)
            elif isinstance(params,dict):
                if self.debug:
                    print >> log,'execute-dict: params',params,'query',query%params
                cursor.execute(query,params)
            elif isinstance(params,tuple) and len(params)==1:
                if self.debug:
                    print >> log,'execute-tuple',query%params[0]
                cursor.execute(query,params[0])
            else:
                param_seq=(params,)
                if self.debug:
                    for params in param_seq:
                        print >> log,'executemany',query%params
                cursor.executemany(query, param_seq)
            (self.rowcount, self.description, self.lastrowid) = \
                            (cursor.rowcount, cursor.description, cursor.lastrowid)
        except Exception, e:
            try:
                self.rollback()
            except:
                pass
            uuid = commands.getoutput("uuidgen")
            log.exception( e, "Database error %s:" % uuid )
            log.error( "Failed Query:" )
            log.error( query )
            log.error( "Failed Params:" )
            log.error( pformat(params) )
            raise MDDBError("Please contact " + \
                             self.api.config.MD_NAME + " Support " + \
                             "<" + self.api.config.MD_MAIL_SUPPORT_ADDRESS + ">" + \
                             " and reference " + uuid)

        return cursor

    def selectall(self, query, params = None, hashref = True, key_field = None):
        """
        Return each row as a dictionary keyed on field name (like DBI
        selectrow_hashref()). If key_field is specified, return rows
        as a dictionary keyed on the specified field (like DBI
        selectall_hashref()).

        If params is specified, the specified parameters will be bound
        to the query.
        """

        cursor = self.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        self.commit()
        if hashref or key_field is not None:
            # Return each row as a dictionary keyed on field name
            # (like DBI selectrow_hashref()).
            labels = [column[0] for column in self.description]
            rows = [dict(zip(labels, row)) for row in rows]

        if key_field is not None and key_field in labels:
            # Return rows as a dictionary keyed on the specified field
            # (like DBI selectall_hashref()).
            return dict([(row[key_field], row) for row in rows])
        else:
            return rows

    def fields(self, table, notnull = None, hasdef = None):
        """
        Return the names of the fields of the specified table.
        """

        if hasattr(self, 'fields_cache'):
            if self.fields_cache.has_key((table, notnull, hasdef)):
                return self.fields_cache[(table, notnull, hasdef)]
        else:
            self.fields_cache = {}

        sql = "SELECT attname FROM pg_attribute, pg_class" \
              " WHERE pg_class.oid = attrelid" \
              " AND attnum > 0 AND relname = %(table)s"

        if notnull is not None:
            sql += " AND attnotnull is %(notnull)s"

        if hasdef is not None:
            sql += " AND atthasdef is %(hasdef)s"

        rows = self.selectall(sql, locals(), hashref = False)

        self.fields_cache[(table, notnull, hasdef)] = [row[0] for row in rows]

        return self.fields_cache[(table, notnull, hasdef)]
