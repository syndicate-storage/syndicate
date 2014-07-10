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
        

from syndicate_service.models import VolumeAccessRight
from observer.deleter import Deleter

from django.forms.models import model_to_dict


# find syndicatelib
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class VolumeAccessRightDeleter(Deleter):
   model='VolumeAccessRight'

   def __init__(self, **args):
      Deleter.__init__(self, **args)

   def call(self, pk, model_dict):
      print "XXX delete volume access right", model_dict


if __name__ == "__main__":
   vard = VolumeAccessRightDeleter()

   all_vars = VolumeAccessRight.objects.all()
   for var in all_vars:
      vard( var.pk, model_to_dict( var ) )