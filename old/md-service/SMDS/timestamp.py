#
# Utilities to handle timestamps / durations from/to integers and strings
#
# $Id: Timestamp.py 18344 2010-06-22 18:56:38Z caglar $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/trunk/PLC/Timestamp.py $
#

#
# datetime.{datetime,timedelta} are powerful tools, but these objects are not
# natively marshalled over xmlrpc
#

from types import StringTypes
import time, calendar
import datetime

from SMDS.faults import *
from SMDS.parameter import Parameter, Mixed

# a dummy class mostly used as a namespace
class Timestamp:

    debug=False
#    debug=True

    # this is how we expose times to SQL
    sql_format = "%Y-%m-%d %H:%M:%S"
    sql_format_utc = "%Y-%m-%d %H:%M:%S UTC"
    # this one (datetime.isoformat) would work too but that's less readable - we support this input though
    iso_format = "%Y-%m-%dT%H:%M:%S"
    # sometimes it's convenient to understand more formats
    input_formats = [ sql_format,
                      sql_format_utc,
                      iso_format,
                      "%Y-%m-%d %H:%M",
                      "%Y-%m-%d %H:%M UTC",
                      ]

    # for timestamps we usually accept either an int, or an ISO string,
    # the datetime.datetime stuff can in general be used locally,
    # but not sure it can be marshalled over xmlrpc though

    @staticmethod
    def Parameter (doc):
        return Mixed (Parameter (int, doc + " (unix timestamp)"),
                      Parameter (str, doc + " (formatted as %s)"%Timestamp.sql_format),
                      )

    @staticmethod
    def sql_validate (input, timezone=False, check_future = False):
        """
        Validates the specified GMT timestamp, returns a
        standardized string suitable for SQL input.

        Input may be a number (seconds since UNIX epoch back in 1970,
        or a string (in one of the supported input formats).

        If timezone is True, the resulting string contains
        timezone information, which is hard-wired as 'UTC'

        If check_future is True, raises an exception if timestamp is in
        the past.

        Returns a GMT timestamp string suitable to feed SQL.
        """

        if not timezone: output_format = Timestamp.sql_format
        else:            output_format = Timestamp.sql_format_utc

        if Timestamp.debug: print 'sql_validate, in:',input,
        if isinstance(input, StringTypes):
            sql=''
            # calendar.timegm() is the inverse of time.gmtime()
            for time_format in Timestamp.input_formats:
                try:
                    timestamp = calendar.timegm(time.strptime(input, time_format))
                    sql = time.strftime(output_format, time.gmtime(timestamp))
                    break
                # wrong format: ignore
                except ValueError: pass
            # could not parse it
            if not sql:
                raise MDInvalidArgument, "Cannot parse timestamp %r - not in any of %r formats"%(input,Timestamp.input_formats)
        elif isinstance (input,(int,long,float)):
            try:
                timestamp = long(input)
                sql = time.strftime(output_format, time.gmtime(timestamp))
            except Exception,e:
                raise MDInvalidArgument, "Timestamp %r not recognized -- %r"%(input,e)
        else:
            raise MDInvalidArgument, "Timestamp %r - unsupported type %r"%(input,type(input))

        if check_future and input < time.time():
            raise MDInvalidArgument, "'%s' not in the future" % sql

        if Timestamp.debug: print 'sql_validate, out:',sql
        return sql

    @staticmethod
    def sql_validate_utc (timestamp):
        "For convenience, return sql_validate(intput, timezone=True, check_future=False)"
        return Timestamp.sql_validate (timestamp, timezone=True, check_future=False)


    @staticmethod
    def cast_long (input):
        """
        Translates input timestamp as a unix timestamp.

        Input may be a number (seconds since UNIX epoch, i.e., 1970-01-01
        00:00:00 GMT), a string (in one of the supported input formats above).

        """
        if Timestamp.debug: print 'cast_long, in:',input,
        if isinstance(input, StringTypes):
            timestamp=0
            for time_format in Timestamp.input_formats:
                try:
                    result=calendar.timegm(time.strptime(input, time_format))
                    if Timestamp.debug: print 'out:',result
                    return result
                # wrong format: ignore
                except ValueError: pass
            raise MDInvalidArgument, "Cannot parse timestamp %r - not in any of %r formats"%(input,Timestamp.input_formats)
        elif isinstance (input,(int,long,float)):
            result=long(input)
            if Timestamp.debug: print 'out:',result
            return result
        else:
            raise MDInvalidArgument, "Timestamp %r - unsupported type %r"%(input,type(input))


# utility for displaying durations
# be consistent in avoiding the datetime stuff
class Duration:

    MINUTE = 60
    HOUR = 3600
    DAY = 3600*24

    @staticmethod
    def to_string(duration):
        result=[]
        left=duration
        (days,left) = divmod(left,Duration.DAY)
        if days:    result.append("%d d)"%td.days)
        (hours,left) = divmod (left,Duration.HOUR)
        if hours:   result.append("%d h"%hours)
        (minutes, seconds) = divmod (left, Duration.MINUTE)
        if minutes: result.append("%d m"%minutes)
        if seconds: result.append("%d s"%seconds)
        if not result: result = ['void']
        return "-".join(result)

    @staticmethod
    def validate (duration):
        # support seconds only for now, works for int/long/str
        try:
            return long (duration)
        except:
            raise MDInvalidArgument, "Could not parse duration %r"%duration
