#!/usr/bin/python

import os
import sys
import base64
from django.db.models import F, Q
from planetstack.config import Config
from observer.syncstep import SyncStep
from core.models import Service
from syndicate.models import VolumeAccessRight
from util.logger import Logger, logging

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

logger = Logger(level=logging.INFO)

class SyncVolume(SyncStep):
    provides=[VolumeAccessRight]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        return VolumeAccessRight.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))

    def sync_record(self, vac):
        print "sync!"
        # TODO: finish this
