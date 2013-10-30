from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_volume.views',

                        url(r'^failure/?$', 'failure'),

                        url(r'^allvolumes/?$', 'allvolumes'),

                        url(r'^myvolumes/?$', 'myvolumes'),
                        
                        url(r'^createvolume/?$', 'createvolume'),
                        
			url(r'^(?P<volume_id>[\w\.-]+)/activate/?',
					     'activatevolume'),

                        url(r'^(?P<volume_id>[\w\.-]+)/deactivate/?',
                         'deactivatevolume'),

                        url(r'^(?P<volume_id>[\w\.-]+)/delete/?',
                         'deletevolume'),

                        url(r'^(?P<volume_id>[\w\.-]+)/addpermissions/?',
                         'addpermissions'),

                        url(r'^(?P<volume_id>[\w\.-]+)/changepermissions/?',
                         'changepermissions'),

                        url(r'^(?P<volume_id>[\w\.-]+)/permissions/?',
                         'volumepermissions'),

                        url(r'^(?P<volume_id>[\w\.-]+)/settings/?',
                         'volumesettings'),

                        url(r'^(?P<volume_id>[\w\.-]+)/change/description/?',
                         'changedesc'),
                        
                        url(r'^(?P<volume_id>[\w\.-]+)/change/password/?',
                         'changevolumepassword'),

                        url(r'^(?P<volume_id>[\w\.-]+)/change/ags/?',
                         'changegateways_ag'),
                        
                        url(r'^(?P<volume_id>[\w\.-]+)/change/rgs/?',
                         'changegateways_rg'),

                        url(r'^(?P<volume_id>[\w\.-]+)/privacy/?',
                         'volumeprivacy'),
                        
                        url(r'^(?P<volume_id>[\w\.-]+)/?',
                         'viewvolume'),
                        
                        url(r'^(?P<volume_id>[\w\.-]+)/edit/?',
                         'volumeedit'),
                        
                        url(r'^(?P<volume_id>[\w\.-]+)/submit/?',
                         'volumesubmit'),

)
