from core.models import User,Site,Service,SingletonModel,PlCoreBase,Slice
import os
from django.db import models
from django.forms.models import model_to_dict
from bitfield import BitField
from django.core.exceptions import ValidationError

# Create your models here.

class SyndicateService(SingletonModel,Service):
    class Meta:
        app_label = "syndicate"
        verbose_name = "Syndicate Service"
        verbose_name_plural = "Syndicate Service"

    def __unicode__(self):  return u'Syndicate Service'


class SyndicatePrincipal(PlCoreBase):
    class Meta:
        app_label = "syndicate"

    # for now, this is a user email address 
    principal_id = models.TextField()
    public_key_pem = models.TextField()
    sealed_private_key = models.TextField()

    def __unicode__self(self):  return "%s" % self.principal_id


class Volume(PlCoreBase):
    class Meta:
        app_label = "syndicate"

    name = models.CharField(max_length=64, help_text="Human-readable, searchable name of the Volume")
    
    owner_id = models.ForeignKey(User, verbose_name='Owner')

    description = models.TextField(null=True, blank=True,max_length=130, help_text="Human-readable description of what this Volume is used for.")
    blocksize = models.PositiveIntegerField(help_text="Number of bytes per block.")
    private = models.BooleanField(default=True, help_text="Indicates if the Volume is visible to users other than the Volume Owner and Syndicate Administrators.")
    archive = models.BooleanField(default=False, help_text="Indicates if this Volume is read-only, and only an Aquisition Gateway owned by the Volume owner (or Syndicate admin) can write to it.")

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

    def __unicode__(self):  return "%s-%s" % (self.owner_id.email, self.volume.name)


class VolumeSlice(PlCoreBase):
    class Meta:
        app_label = "syndicate"

    volume_id = models.ForeignKey(Volume, verbose_name="Volume")
    slice_id = models.ForeignKey(Slice, verbose_name="Slice")
    gateway_caps = BitField(flags=("read data", "write data", "host files"), verbose_name="Slice Capabilities")
    
    peer_portnum = models.PositiveIntegerField(help_text="User Gateway port", verbose_name="Client peer-to-peer cache port")
    replicate_portnum = models.PositiveIntegerField(help_text="Replica Gateway port", verbose_name="Replication service port")

    credentials_blob = models.TextField(null=True, blank=True, help_text="Encrypted slice credentials")
 
    def __unicode__(self):  return "%s-%s" % (self.volume_id.name, self.slice_id.name)

    def clean(self):
        """
        Verify that our fields are in order:
            * peer_portnum and replicate_portnum have to be valid port numbers between 1025 and 65534
            * peer_portnum and replicate_portnum cannot be changed once set.
        """

        if self.peer_portnum < 1025 or self.peer_portnum > 65534:
            raise ValidationError( "Client peer-to-peer cache port number must be between 1025 and 65534" )

        if self.replicate_portnum < 1025 or self.replicate_portnum > 65534:
            raise ValidationError( "Replication service port number must be between 1025 and 65534" )

