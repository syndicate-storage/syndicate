from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_home.views',
                       url(r'^$', 'home'),
                       url(r'^make_user/(?P<email>.*)', 'make_user'),
                       url(r'^allvolumes/?$', 'allvolumes'),
                       url(r'^myvolumes/?$', 'myvolumes'),
                       url(r'^settings/?$', 'settings'),
                       url(r'^downloads/?$','downloads'),
                       url(r'^createvolume/?$', 'createvolume'),
                       url(r'^volume/(?P<volume_name>\w*)/activate', 'activatevolume'),
                       url(r'^volume/(?P<volume_name>\w*)/deactivate', 'deactivatevolume'),
                       url(r'^volume/(?P<volume>\w*)/delete', 'deletevolume'),
                       url(r'^volume/(?P<volume_name>\w*)/addpermissions', 'addpermissions'),
                       url(r'^volume/(?P<volume_name>\w*)/changepermissions', 'changepermissions'),
                       url(r'^volume/(?P<volume_name>\w*)/permissions', 'volumepermissions'),
                       url(r'^logout/?$', 'logout'),
                       url(r'^volume/(?P<volume>\w*)', 'viewvolume'),
)