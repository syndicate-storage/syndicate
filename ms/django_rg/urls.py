from django.conf.urls import patterns, include, url

# The long URLS are for admin purposes and will be eliminated. Make GUI, and have posts work for both admin and real gateways in the wild.

urlpatterns = patterns('django_rg.views',
	                   url(r'^changeprivacy/(?P<g_name>[\w\.-]+)/?$', 'changeprivacy'),
					   url(r'^changejson/(?P<g_name>[\w\.-]+)/?$', 'changejson'),
					   url(r'^changepassword/(?P<g_name>[\w\.-]+)/?$', 'changepassword'),		
					   url(r'^changelocation/(?P<g_name>[\w\.-]+)/?$', 'changelocation'),
					   url(r'^removevolumes/(?P<g_name>[\w\.-]+)/?$', 'removevolumes'),
					   url(r'^addvolume/(?P<g_name>[\w\.-]+)/?$', 'addvolume'),

					   url(r'^viewgateway/(?P<g_name>[\w\.-]+)/?$', 'viewgateway'),					   
					   url(r'^allgateways/?$', 'allgateways'),
                       url(r'^create/?$', 'create'),
                       url(r'^delete/(?P<g_name>[\w\.-]+)/?$', 'delete'),
                       url(r'^create/(?P<volume_name>[\w\.-]+)/(?P<g_name>[\w\.-]+)/(?P<g_password>[\w\.-]+)/(?P<host>[\w\.-]+)/(?P<port>[\d]+)/?', 'urlcreate'),
                       url(r'^delete/(?P<g_name>[\w\.-]+)/(?P<g_password>[\w\.-]+)/?', 'urldelete'),
)