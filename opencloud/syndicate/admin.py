from django.contrib import admin

from syndicate.models import *
from django import forms
from django.utils.safestring import mark_safe
from django.contrib.auth.admin import UserAdmin
from django.contrib.admin.widgets import FilteredSelectMultiple
from django.contrib.auth.forms import ReadOnlyPasswordHashField
from django.contrib.auth.signals import user_logged_in
from django.utils import timezone
from django.contrib.contenttypes import generic
from suit.widgets import LinkedSelect
from core.admin import ReadOnlyTabularInline,ReadOnlyAwareAdmin,SingletonAdmin,SliceInline,ServiceAttrAsTabInline,PlanetStackBaseAdmin, PlStackTabularInline,SliceROInline,ServiceAttrAsTabROInline
from suit.widgets import LinkedSelect
from bitfield import BitField
from bitfield.forms import BitFieldCheckboxSelectMultiple
from django.core.exceptions import ValidationError, ObjectDoesNotExist

class SyndicateServiceAdmin(SingletonAdmin,ReadOnlyAwareAdmin):
    model = SyndicateService
    verbose_name = "Syndicate Service"
    verbose_name_plural = "Syndicate Service"
    list_display = ("name","enabled")
    fieldsets = [(None, {'fields': ['name','enabled','versionNumber', 'description',], 'classes':['suit-tab suit-tab-general']})]
    inlines = [SliceInline,ServiceAttrAsTabInline]

    user_readonly_fields = ['name','enabled','versionNumber','description']
    user_readonly_inlines = [SliceROInline, ServiceAttrAsTabROInline]

    suit_form_tabs =(('general', 'Syndicate Service Details'),
        ('slices','Slices'),
        ('serviceattrs','Additional Attributes'),
    )


class VolumeAccessRightForUserROInline(ReadOnlyTabularInline):
    model = VolumeAccessRight
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumeAccessRights'
    fields = ['volume','gateway_caps']

class VolumeAccessRightROInline(ReadOnlyTabularInline):
    model = VolumeAccessRight
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumeAccessRights'
    fields = ['owner_id','gateway_caps']

class VolumeAccessRightInline(PlStackTabularInline):
    model = VolumeAccessRight
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumeAccessRights'
    formfield_overrides = {
        BitField: {'widget': BitFieldCheckboxSelectMultiple}
    }

class VolumeInline(PlStackTabularInline):
    model = Volume
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumes'
    fields = ['name', 'owner_id']

class VolumeROInline(ReadOnlyTabularInline):
    model = Volume
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumes'
    fields = ['name', 'owner_id']


class VolumeSliceFormSet( forms.models.BaseInlineFormSet ):
    # verify that our VolumeSlice is valid

    @classmethod
    def verify_unchanged( cls, volume_pk, slice_pk, field_name, new_value ):
        vs = None
        try:
           vs = VolumeSlice.objects.get( volume_id=volume_pk, slice_id=slice_pk )
        except ObjectDoesNotExist, dne:
           return True, None

        old_value = getattr( vs, field_name )
        if old_value != new_value:
            return False, old_value
        else:
            return True, None


    def clean( self ):
        for form in self.forms:
            # check each inline's cleaned data, if it's valid
            cleaned_data = None
            try:
                if form.cleaned_data:
                    cleaned_data = form.cleaned_data
            except AttributeError:
                continue

            # verify that the ports haven't changed 
            volume_pk = cleaned_data['volume_id'].pk
            slice_pk = cleaned_data['slice_id'].pk
           
            if not cleaned_data.has_key('peer_portnum'):
                raise ValidationError("Missing client peer-to-peer cache port number")

            if not cleaned_data.has_key('replicate_portnum'):
                raise ValidationError("Missing replication service port number")

            rc1, old_peer_port = VolumeSliceFormSet.verify_unchanged( volume_pk, slice_pk, 'peer_portnum', cleaned_data['peer_portnum'] )
            rc2, old_replicate_port = VolumeSliceFormSet.verify_unchanged( volume_pk, slice_pk, 'replicate_portnum', cleaned_data['replicate_portnum'] )

            err1str = ""
            err2str = ""
            if not rc1:
                err1str = "change %s back to %s" % (cleaned_data['peer_portnum'], old_peer_port)
            if not rc2:
                err2str = " and change %s back to %s" % (cleaned_data['replicate_portnum'], old_replicate_port )

            if not rc1 or not rc2:
                raise ValidationError("Port numbers cannot be changed once they are set. Please %s %s" % (err1str, err2str))
            
            

class VolumeSliceInline(PlStackTabularInline):
    model = VolumeSlice
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumeSlices'
    fields = ['volume_id', 'slice_id', 'gateway_caps', 'peer_portnum', 'replicate_portnum']
    formfield_overrides = { BitField: {'widget': BitFieldCheckboxSelectMultiple},}

    formset = VolumeSliceFormSet
    
    readonly_fields = ['credentials_blob']
 

class VolumeSliceROInline(ReadOnlyTabularInline):
    model = VolumeSlice
    extra = 0
    suit_classes = 'suit-tab suit-tab-volumeSlices'
    fields = ['volume_id', 'slice_id', 'gateway_caps', 'peer_portnum', 'replicate_portnum']
    formfield_overrides = { BitField: {'widget': BitFieldCheckboxSelectMultiple},}

    formset = VolumeSliceFormSet

    readonly_fields = ['credentials_blob']


class VolumeAdmin(ReadOnlyAwareAdmin):
    model = Volume
   
    def get_readonly_fields(self, request, obj=None ):
       always_readonly = []
       if obj == None:
          # all fields are editable on add
          return always_readonly

       else:
          # can't change owner, slice id, or block size on update
          return ['blocksize', 'owner_id', 'per_slice_volume', 'per_slice_id', 'per_slice_password'] + always_readonly


    list_display = ['name', 'owner_id']

    formfield_overrides = { BitField: {'widget': BitFieldCheckboxSelectMultiple},}

    #detailsFieldList = ['name', 'owner_id', 'description','file_quota','blocksize', 'private','archive', 'default_gateway_caps' ]
    detailsFieldList = ['name', 'owner_id', 'description','blocksize', 'private','archive', 'default_gateway_caps', 'per_slice_volume', 'per_slice_id', 'per_slice_password' ]
    
    #keyList = ['metadata_private_key']

    fieldsets = [
        (None, {'fields': detailsFieldList, 'classes':['suit-tab suit-tab-general']}),
        #(None, {'fields': keyList, 'classes':['suit-tab suit-tab-volumeKeys']}),
    ]

    inlines = [VolumeAccessRightInline, VolumeSliceInline]

    #user_readonly_fields = ['name','owner_id','description','blocksize','private','metadata_private_key','file_quota','default_gateway_caps']
    user_readonly_fields = ['name','owner_id','description','blocksize','private','default_gateway_caps']
    
    user_readonly_inlines = [VolumeAccessRightROInline, VolumeSliceROInline]

    suit_form_tabs =(('general', 'Volume Details'),
                     #('volumeKeys', 'Access Keys'),
                     ('volumeSlices', 'Slices'),
                     ('volumeAccessRights', 'Volume Access Rights'),
    )
    

# left panel:
admin.site.register(SyndicateService, SyndicateServiceAdmin)
admin.site.register(Volume, VolumeAdmin)
