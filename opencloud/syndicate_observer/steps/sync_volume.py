#!/usr/bin/python

import os
import sys
import traceback
import base64

if __name__ == "__main__":
    # for testing 
    if os.getenv("OPENCLOUD_PYTHONPATH"):
        sys.path.append( os.getenv("OPENCLOUD_PYTHONPATH") )
    else:
        print >> sys.stderr, "No OPENCLOUD_PYTHONPATH variable set.  Assuming that OpenCloud is in PYTHONPATH"
 
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")


from django.db.models import F, Q
from planetstack.config import Config
from observer.syncstep import SyncStep
from core.models import Service
from syndicate.models import Volume

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

# point to planetstack
if __name__ != "__main__": 
    if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
        sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
    else:
        logger.warning("No OPENCLOUD_PYTHONPATH set; assuming your PYTHONPATH works")

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib


class SyncVolume(SyncStep):
    provides=[Volume]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        try:
            ret = Volume.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))
            return ret
        except Exception, e:
            traceback.print_exc()
            return None

    def sync_record(self, volume):
        """
        Synchronize a Volume record with Syndicate.
        """
        print "\n\nSync!"
        print "volume = %s\n\n" % volume.name
    
        user_email = volume.owner_id.email
        config = syndicatelib.get_config()

        # get the observer secret 
        try:
            observer_secret = config.SYNDICATE_OPENCLOUD_SECRET
        except Exception, e:
            traceback.print_exc()
            logger.error("config is missing SYNDICATE_OPENCLOUD_SECRET")
            raise e

        # volume owner must exist as a Syndicate user...
        try:
            rc, user = syndicatelib.ensure_user_exists_and_has_credentials( user_email, observer_secret )
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to ensure user '%s' exists" % user_email )
            raise e

        print "\n\nuser for %s: %s\n\n" % (user_email, user)

        # volume must exist 
            
        # create or update the Volume
        try:
            new_volume = syndicatelib.ensure_volume_exists( user_email, volume, user=user )
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to ensure volume '%s' exists" % volume.name )
            raise e

        print "\n\nvolume for %s: %s\n\n" % (volume.name, new_volume)
           
        # did we create the Volume?
        if new_volume is not None:
            # we're good
            pass 
             
        # otherwise, just update it 
        else:
            try:
                rc = syndicatelib.update_volume( volume )
            except Exception, e:
                traceback.print_exc()
                logger.error("Failed to update volume '%s', exception = %s" % (volume.name, e.message))
                raise e
                    
        return True




if __name__ == "__main__":
    sv = SyncVolume()


    # first, set all volumes to not-enacted so we can test 
    for v in Volume.objects.all():
       v.enacted = None
       v.save()
    
    # NOTE: for resetting only 
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
       sys.exit(0)

    recs = sv.fetch_pending()

    for rec in recs:
        rc = sv.sync_record( rec )
        if not rc:
          print "\n\nFailed to sync %s\n\n" % (rec.name)

