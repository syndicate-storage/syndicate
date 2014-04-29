#!/usr/bin/python

import os
import sys
import base64
import traceback

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
from core.models import Service, Slice

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

from syndicate.models import VolumeSlice,VolumeAccessRight,Volume

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class SyncVolumeSlice(SyncStep):
    provides=[VolumeSlice]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        return VolumeSlice.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))

    def sync_record(self, vs):
        print "Mount %s on %s's VMs" % (vs.volume_id.name, vs.slice_id.name)
            
        # extract arguments...
        user_email = vs.slice_id.creator.email
        slice_name = vs.slice_id.name
        volume_name = vs.volume_id.name
        syndicate_caps = syndicatelib.opencloud_caps_to_syndicate_caps( vs.gateway_caps )
        RG_port = vs.replicate_portnum
        UG_port = vs.peer_portnum
            
        config = syndicatelib.get_config()
        try:
           observer_secret = config.SYNDICATE_OPENCLOUD_SECRET
           RG_closure = config.SYNDICATE_RG_CLOSURE
           observer_pkey_path = config.SYNDICATE_PRIVATE_KEY
           syndicate_url = config.SYNDICATE_SMI_URL
        except Exception, e:
           traceback.print_exc()
           logger.error("syndicatelib config is missing one of the following: SYNDICATE_OPENCLOUD_SECRET, SYNDICATE_RG_CLOSURE, SYNDICATE_PRIVATE_KEY, SYNDICATE_SMI_URL")
           raise e
            
        # make sure there's a Syndicate user account for the slice owner
        try:
            rc, user = syndicatelib.ensure_user_exists_and_has_credentials( user_email, observer_secret )
            assert rc is True, "Failed to ensure user %s exists and has credentials (rc = %s,%s)" % (user_email, rc, user)
        except Exception, e:
            traceback.print_exc()
            logger.error('Failed to ensure user %s exists' % user_email)
            raise e
            
        # grant the slice-owning user the ability to provision UGs in this Volume, and also provision for the user the (single) RG the slice will instantiate in each VM.
        try:
            rc = syndicatelib.setup_volume_access( user_email, volume_name, syndicate_caps, RG_port, observer_secret, RG_closure=RG_closure )
            assert rc is True, "Failed to set up Volume access for %s in %s" % (user_email, volume_name)
            
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to set up Volume access for %s in %s" % (user_email, volume_name))
            raise e
            
        # get slice credentials....
        try:
            slice_cred = syndicatelib.generate_slice_credentials( observer_pkey_path, syndicate_url, user_email, volume_name, observer_secret, UG_port, existing_user=user )
            assert slice_cred is not None, "Failed to generate slice credential for %s in %s" % (user_email, volume_name )
                
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to generate slice credential for %s in %s" % (user_email, volume_name))
            raise e
             
        # .... and push them all out.
        try:
            rc = syndicatelib.push_credentials_to_slice( slice_name, slice_cred )
            assert rc is True, "Failed to push credentials to slice %s for volume %s" % (slice_name, volume_name)
               
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to push slice credentials to %s for volume %s" % (slice_name, volume_name))
            raise e

        return True


if __name__ == "__main__":
    sv = SyncVolumeSlice()

    # first, set all VolumeSlice to not-enacted so we can test 
    for v in VolumeSlice.objects.all():
       v.enacted = None
       v.save()

    # NOTE: for resetting only 
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
       sys.exit(0)

    recs = sv.fetch_pending()

    for rec in recs:
        if rec.slice_id.creator is None:
           print "Ignoring slice %s, since it has no creator" % (rec.slice_id)
           continue

        sv.sync_record( rec )

