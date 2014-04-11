from core.models import User,Site,Service,SingletonModel,PlCoreBase,Slice
import os
from django.db import models
from django.forms.models import model_to_dict
from bitfield import BitField

# Create your models here.

class SyndicateService(SingletonModel,Service):
    class Meta:
        app_label = "syndicate"
        verbose_name = "Syndicate Service"
        verbose_name_plural = "Syndicate Service"

    # for sealing Volume credentials
    observer_private_key_pem = models.TextField( editable=False )
    
    def __unicode__(self):  return u'Syndicate Service'


class Volume(PlCoreBase):
    class Meta:
        app_label = "syndicate"

    name = models.CharField(max_length=64, unique=True, help_text="Human-readable, searchable name of the Volume")
    
    owner_id = models.ForeignKey(User, verbose_name='Owner')

    slice_id = models.ForeignKey(Slice, verbose_name="Slice", null=True, blank=True)
    
    description = models.TextField(null=True, blank=True,max_length=130, help_text="Human-readable description of what this Volume is used for.")
    blocksize = models.PositiveIntegerField(help_text="Number of bytes per block.")
    private = models.BooleanField(default=True, help_text="Indicates if the Volume is visible to users other than the Volume Owner and Syndicate Administrators.")
    archive = models.BooleanField(default=False, help_text="Indicates if this Volume is read-only, and only an Aquisition Gateway owned by the Volume owner (or Syndicate admin) can write to it.")
    
    credentials_blob = models.TextField(null=True, blank=True, editable=False )
    
    CAP_READ_DATA = 1
    CAP_WRITE_DATA = 2
    CAP_HOST_DATA = 4
    
    # NOTE: preserve order of capabilities here...
    default_gateway_caps = BitField(flags=("read data", "write data", "host files"), verbose_name='Default User Capabilities')

    def __unicode__(self):  return self.name


class VolumeAccessRight(PlCoreBase):
    class Meta:
        app_label = "syndicate"

    owner_id = models.ForeignKey(User, verbose_name='user')
    
    volume = models.ForeignKey(Volume)
    gateway_caps = BitField(flags=("read data", "write data", "host files"), verbose_name="User Capabilities")    

    def __unicode__(self):  return self.owner_id.email


