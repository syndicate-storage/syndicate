from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_volume.views',
					    url(r'^(?P<volume_name>\w*)/activate',
					     'activatevolume'),

                        url(r'^(?P<volume_name>\w*)/deactivate',
                         'deactivatevolume'),

                        url(r'^(?P<volume_name>\w*)/delete',
                         'deletevolume'),

                        url(r'^(?P<volume_name>\w*)/addpermissions',
                         'addpermissions'),

                        url(r'^(?P<volume_name>\w*)/changepermissions',
                         'changepermissions'),

                        url(r'^(?P<volume_name>\w*)/permissions',
                         'volumepermissions'),
                        
                        url(r'^(?P<volume_name>\w*)',
                         'viewvolume'),

)