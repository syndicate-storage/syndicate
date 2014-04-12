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

class VolumeAdmin(ReadOnlyAwareAdmin):
    model = Volume
   
    def get_readonly_fields(self, request, obj=None ):
       if obj == None:
          # all fields are editable on add
          return []

       else:
          # can't change owner, slice id, or block size on update
          return ['blocksize', 'owner_id', 'slice_id']


    list_display = ['name', 'owner_id']

    formfield_overrides = { BitField: {'widget': BitFieldCheckboxSelectMultiple},}

    #detailsFieldList = ['name', 'owner_id', 'description','file_quota','blocksize', 'private','archive', 'default_gateway_caps' ]
    detailsFieldList = ['name', 'owner_id', 'slice_id', 'description','blocksize', 'private','archive', 'default_gateway_caps' ]
    
    #keyList = ['metadata_private_key']

    fieldsets = [
        (None, {'fields': detailsFieldList, 'classes':['suit-tab suit-tab-general']}),
        #(None, {'fields': keyList, 'classes':['suit-tab suit-tab-volumeKeys']}),
    ]

    inlines = [VolumeAccessRightInline]

    #user_readonly_fields = ['name','owner_id','description','blocksize','private','metadata_private_key','file_quota','default_gateway_caps']
    user_readonly_fields = ['name','owner_id','slice_id','description','blocksize','private','default_gateway_caps']
    
    user_readonly_inlines = [VolumeAccessRightROInline]

    suit_form_tabs =(('general', 'Volume Details'),
                     #('volumeKeys', 'Access Keys'),
                     ('volumeAccessRights', 'Volume Access Rights'),
    )
    

# left panel:
admin.site.register(SyndicateService, SyndicateServiceAdmin)
admin.site.register(Volume, VolumeAdmin)

