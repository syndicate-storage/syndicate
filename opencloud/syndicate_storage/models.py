from core.models import User,Site,Service,SingletonModel,PlCoreBase,Slice,SlicePrivilege
import os
from django.db import models
from django.db.models import Q
from django.forms.models import model_to_dict
from django.core.exceptions import ValidationError, ObjectDoesNotExist

# Create your models here.

class SyndicateService(SingletonModel,Service):
    class Meta:
        app_label = "syndicate_storage"
        verbose_name = "Syndicate Service"
        verbose_name_plural = "Syndicate Service"

    def __unicode__(self):  return u'Syndicate Service'


class SyndicatePrincipal(PlCoreBase):
    class Meta:
        app_label = "syndicate_storage"

    # for now, this is a user email address 
    principal_id = models.TextField(unique=True)
    public_key_pem = models.TextField()
    sealed_private_key = models.TextField()

    def __unicode__self(self):  return "%s" % self.principal_id


class Volume(PlCoreBase):
    class Meta:
        app_label = "syndicate_storage"

    name = models.CharField(max_length=64, help_text="Human-readable, searchable name of the Volume")
    
    owner_id = models.ForeignKey(User, verbose_name='Owner')

    description = models.TextField(null=True, blank=True,max_length=130, help_text="Human-readable description of what this Volume is used for.")
    blocksize = models.PositiveIntegerField(help_text="Number of bytes per block.")
    private = models.BooleanField(default=True, help_text="Indicates if the Volume is visible to users other than the Volume Owner and Syndicate Administrators.")
    archive = models.BooleanField(default=False, help_text="Indicates if this Volume is read-only, and only an Aquisition Gateway owned by the Volume owner (or Syndicate admin) can write to it.")

    cap_read_data = models.BooleanField(default=True, help_text="VM can read Volume data")
    cap_write_data = models.BooleanField(default=True, help_text="VM can write Volume data")
    cap_host_data = models.BooleanField(default=True, help_text="VM can host Volume data")
    
    slice_id = models.ManyToManyField(Slice, through="VolumeSlice")
    
    def __unicode__(self):  return self.name
 
    
    @staticmethod
    def select_by_user(user):
        """
        Only return Volumes accessible by the user.
        Admin users can see everything.
        """
        if user.is_admin:
            qs = Volume.objects.all()
        else:
            qs = Volume.objects.filter( Q(owner_id=user) | Q(private=False) )
            
        return qs


class VolumeAccessRight(PlCoreBase):
    class Meta:
        app_label = "syndicate_storage"

    owner_id = models.ForeignKey(User, verbose_name='user')
    
    volume = models.ForeignKey(Volume)

    cap_read_data = models.BooleanField(default=True, help_text="VM can read Volume data")
    cap_write_data = models.BooleanField(default=True, help_text="VM can write Volume data")
    cap_host_data = models.BooleanField(default=True, help_text="VM can host Volume data")


    def __unicode__(self):  return "%s-%s" % (self.owner_id.email, self.volume.name)


class ObserverSecretValue( models.TextField ):
    class Meta:
        app_label = "syndicate_storage"
    
    __metaclass__ = models.SubfieldBase
    
    MAGIC_PREFIX = "$SECRET$:"
    
    @classmethod 
    def is_encrypted( cls, secret_str ):
       # all encrypted secrets start with MAGIC_PREFIX, which is NOT base64-encoded
       return secret_str.startswith( cls.MAGIC_PREFIX )
    
    @classmethod 
    def unserialize( cls, serialized_ciphertext ):
       # strip prefix and return ciphertext 
       return serialized_ciphertext[len(cls.MAGIC_PREFIX):]
    
    @classmethod 
    def serialize( cls, ciphertext ):
       # prepend a magic prefix so we know it's encrypted 
       return cls.MAGIC_PREFIX + ciphertext
    
    def to_python( self, secret_str ):
       """
       Decrypt the value with the Observer key
       """
       
       # is this in the clear?
       if not ObserverSecretValue.is_encrypted( secret_str ):
          # nothing to do
          return secret_str
       
       # otherwise, decrypt it
       from syndicate_observer import syndicatelib
       
       # get observer private key
       config = syndicatelib.get_config()
       
       try:
          observer_pkey_path = config.SYNDICATE_PRIVATE_KEY
          observer_pkey_pem = syndicatelib.get_private_key_pem( observer_pkey_path )
       except:
          raise syndicatelib.SyndicateObserverError( "Internal Syndicate Observer error: failed to load Observer private key" )
       
       # deserialize 
       secret_str = ObserverSecretValue.unserialize( secret_str )
       
       # decrypt
       if secret_str is not None and len(secret_str) > 0:
          
          slice_secret = syndicatelib.decrypt_slice_secret( observer_pkey_pem, secret_str )
          
          if slice_secret is not None:
             return slice_secret 
          
          else:
             raise syndicatelib.SyndicateObserverError( "Internal Syndicate Observer error: failed to decrypt slice secret value" )
       else:
          return None
       
       
    def pre_save( self, model_inst, add ):
       """
       Encrypt the value with the Observer key
       """
       
       from syndicate_observer import syndicatelib 
       
       # get observer private key
       config = syndicatelib.get_config()
       
       try:
          observer_pkey_path = config.SYNDICATE_PRIVATE_KEY
          observer_pkey_pem = syndicatelib.get_private_key_pem( observer_pkey_path )
       except:
          raise syndicatelib.SyndicateObserverError( "Internal Syndicate Observer error: failed to load Observer private key" )
       
       slice_secret = getattr(model_inst, self.attname )
       
       if slice_secret is not None:
          
          # encrypt it 
          sealed_slice_secret = syndicatelib.encrypt_slice_secret( observer_pkey_pem, slice_secret )
          
          return ObserverSecretValue.serialize( sealed_slice_secret )
       
       else:
          raise syndicatelib.SyndicateObserverError( "Internal Syndicate Observer error: No slice secret generated" )
                                                    

class SliceSecret(models.Model):        # NOTE: not a PlCoreBase
    class Meta:
       app_label = "syndicate_storage"
    
    slice_id = models.ForeignKey(Slice)
    secret = ObserverSecretValue(blank=True, help_text="Shared secret between OpenCloud and this slice's Syndicate daemons.")
    
    def __unicode__(self):  return self.slice_id.name
 
    @staticmethod
    def select_by_user(user):
        """
        Only return slice secrets for slices where this user has 'admin' role.
        Admin users can see everything.
        """
        if user.is_admin:
            qs = SliceSecret.objects.all()
        else:
            visible_slice_ids = [sp.slice.id for sp in SlicePrivilege.objects.filter(user=user,role__role='admin')]
            qs = SliceSecret.objects.filter(slice_id__id__in=visible_slice_ids)
            
        return qs
 

class VolumeSlice(PlCoreBase):
    class Meta:
        app_label = "syndicate_storage"

    volume_id = models.ForeignKey(Volume, verbose_name="Volume")
    slice_id = models.ForeignKey(Slice, verbose_name="Slice")
    
    cap_read_data = models.BooleanField(default=True, help_text="VM can read Volume data")
    cap_write_data = models.BooleanField(default=True, help_text="VM can write Volume data")
    cap_host_data = models.BooleanField(default=True, help_text="VM can host Volume data")
    
    UG_portnum = models.PositiveIntegerField(help_text="User Gateway port.  Any port above 1024 will work, but it must be available slice-wide.", verbose_name="UG port")
    RG_portnum = models.PositiveIntegerField(help_text="Replica Gateway port.  Any port above 1024 will work, but it must be available slice-wide.", verbose_name="RG port")
    
    credentials_blob = models.TextField(null=True, blank=True, help_text="Encrypted slice credentials, sealed with the slice secret.")
 
    def __unicode__(self):  return "%s-%s" % (self.volume_id.name, self.slice_id.name)

    def clean(self):
        """
        Verify that our fields are in order:
            * UG_portnum and RG_portnum have to be valid port numbers between 1025 and 65534
            * UG_portnum and RG_portnum cannot be changed once set.
            * UG_portnum and RG_portnum are unique
        """

        if self.UG_portnum == self.RG_portnum:
            raise ValidationError( "UG and RG ports must be unique" )
         
        if self.UG_portnum < 1025 or self.UG_portnum > 65534:
            raise ValidationError( "UG port number must be between 1025 and 65534" )

        if self.RG_portnum < 1025 or self.RG_portnum > 65534:
            raise ValidationError( "RG port number must be between 1025 and 65534" )
         
         
    def save(self, *args, **kw):
       """
       Make sure a SliceSecret exists for this slice
       """
       
       from syndicate_observer import syndicatelib
       
       # get observer private key
       config = syndicatelib.get_config()
       
       try:
          observer_pkey_path = config.SYNDICATE_PRIVATE_KEY
          observer_pkey_pem = syndicatelib.get_private_key_pem( observer_pkey_path )
       except:
          raise syndicatelib.SyndicateObserverError( "Internal Syndicate Observer error: failed to load Observer private key" )
       
       # get or create the slice secret 
       slice_secret = syndicatelib.get_or_create_slice_secret( observer_pkey_pem, None, slice_fk=self.slice_id )
       
       if slice_secret is None:
          raise SyndicateObserverError( "Failed to get or create slice secret for %s" % self.slice_id.name )
       
       super(VolumeSlice, self).save(*args, **kw)
       


