import os
import sys
import traceback

if __name__ == "__main__":
    # for testing 
    if os.getenv("OPENCLOUD_PYTHONPATH"):
        sys.path.append( os.getenv("OPENCLOUD_PYTHONPATH") )
    else:
        print >> sys.stderr, "No OPENCLOUD_PYTHONPATH variable set.  Assuming that OpenCloud is in PYTHONPATH"

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")


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


from syndicate_service.models import Volume
from observer.deleter import Deleter

from django.forms.models import model_to_dict


# syndicatelib will be in steps/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class VolumeDeleter(Deleter):
        model='Volume'

        def __init__(self, **args):
            Deleter.__init__(self, **args)

        def call(self, pk, model_dict):
            try:
                volume_name = model_dict['name']
                syndicatelib.ensure_volume_absent( volume_name )
                return True
            except Exception, e:
                traceback.print_exc()
                logger.exception("Failed to erase volume '%s'" % volume_name)
                return False
            

if __name__ == "__main__":
   vd = VolumeDeleter()
   
   all_volumes = Volume.objects.all()
   for vol in all_volumes:
      vd( vol.pk, model_to_dict( vol ) )
