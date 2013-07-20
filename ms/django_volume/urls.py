from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_volume.views',

                        url(r'^allvolumes/?$', 'allvolumes'),

                        url(r'^myvolumes/?$', 'myvolumes'),
                        
                        url(r'^createvolume/?$', 'createvolume'),
                        
			url(r'^(?P<volume_name>[\w\.-]+)/activate/?',
					     'activatevolume'),

                        url(r'^(?P<volume_name>[\w\.-]+)/deactivate/?',
                         'deactivatevolume'),

                        url(r'^(?P<volume_name>[\w\.-]+)/delete/?',
                         'deletevolume'),

                        url(r'^(?P<volume_name>[\w\.-]+)/addpermissions/?',
                         'addpermissions'),

                        url(r'^(?P<volume_name>[\w\.-]+)/changepermissions/?',
                         'changepermissions'),

                        url(r'^(?P<volume_name>[\w\.-]+)/permissions/?',
                         'volumepermissions'),

                        url(r'^(?P<volume_name>[\w\.-]+)/settings/?',
                         'volumesettings'),

                        url(r'^(?P<volume_name>[\w\.-]+)/change/description/?',
                         'changevolume'),
                        
                        url(r'^(?P<volume_name>[\w\.-]+)/change/password/?',
                         'changevolumepassword'),

                        url(r'^(?P<volume_name>[\w\.-]+)/change/ags/?',
                         'changegateways_ag'),
                        
                        url(r'^(?P<volume_name>[\w\.-]+)/change/rgs/?',
                         'changegateways_rg'),

                        url(r'^(?P<volume_name>[\w\.-]+)/privacy/?',
                         'volumeprivacy'),
                        
                        url(r'^(?P<volume_name>[\w\.-]+)/?',
                         'viewvolume'),

)