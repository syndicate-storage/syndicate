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
from core.models.slice import Slice
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


class SyncSlices(SyncStep):
    provides=[Slice]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        try:
            ret = Slice.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))
            return ret
        except Exception, e:
            traceback.print_exc()
            return None

    def sync_record(self, slice):
        """
        Synchronize a Syndicate Slice record with Syndicate.
        """
        print "Sync - per-slice volume!"
        print "slice = %s" % slice.name

        slice_email = syndicatelib.generate_opencloud_slice_openid( slice.name )
        print "slice email = %s" % slice_email

        logger.info('Create per-slice syndicate volume. Slice = %s, Slice-openid = %s' % (slice.name, slice_email))

        # create the slice-openid
        try:
            #new_user = syndicatelib.ensure_user_exists_and_has_credentials( user_email )
            pass
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to register openid user (slice) '%s' exists" % slice_email )
            return False
 
        """
            
        # create or update the Volume
        try:
            new_volume = syndicatelib.ensure_volume_exists( user_email, volume, user=new_user )
            syndicatelib.ensure_volume_exists( user_email, volume, user=new_user )
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to ensure volume '%s' exists" % volume.name )
            return False

            
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
                return False
                    
            return True
        """
 
        return True



if __name__ == "__main__":
    ss = SyncSlices()

    recs = ss.fetch_pending()

    for rec in recs:
        ss.sync_record( rec )
