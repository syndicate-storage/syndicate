from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_ug.views',
                       url(r'^create/?$', 'create'),
                       url(r'^delete/?$', 'delete'),
                       url(r'^create/(?P<volume_name>[\w\.-]*)/(?P<ug_name>[\w\.-]*)/(?P<ug_password>[\w\.-]*)/(?P<host>[\w\.-]*)/(?P<port>[\d]*)/?', 'urlcreate'),
                       url(r'^delete/(?P<ug_name>[\w\.-]*)/(?P<ug_password>[\w\.-]*)/?', 'urldelete'),
)