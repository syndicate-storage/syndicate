from django.conf.urls import patterns, include, url

# The long URLS are for admin purposes and will be eliminated. Make GUI, and have posts work for both admin and real gateways in the wild.

urlpatterns = patterns('django_ug.views',
					   url(r'^allgateways/?$', 'allgateways'),
					   url(r'^mygateways/?$', 'mygateways'),
                       url(r'^create/?$', 'create'),
                       url(r'^delete/(?P<g_name>[\w\.=]*)/?$', 'delete'),
                       url(r'^create/(?P<volume_name>[\w\.-]*)/(?P<g_name>[\w\.-]*)/(?P<g_password>[\w\.-]*)/(?P<host>[\w\.-]*)/(?P<port>[\d]*)/?', 'urlcreate'),
                       url(r'^delete/(?P<g_name>[\w\.-]*)/(?P<g_password>[\w\.-]*)/?', 'urldelete'),
)