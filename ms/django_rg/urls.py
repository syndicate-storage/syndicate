from django.conf.urls import patterns, include, url

# The long URLS are for admin purposes and will be eliminated. Make GUI, and have posts work for both admin and real gateways in the wild.

urlpatterns = patterns('django_rg.views',
	                   url(r'^changeprivacy/(?P<g_id>[\w\.-]+)/?$', 'changeprivacy'),
					   url(r'^changejson/(?P<g_id>[\w\.-]+)/?$', 'changejson'),
					   url(r'^changepassword/(?P<g_id>[\w\.-]+)/?$', 'changepassword'),		
					   url(r'^changelocation/(?P<g_id>[\w\.-]+)/?$', 'changelocation'),
					   url(r'^removevolumes/(?P<g_id>[\w\.-]+)/?$', 'removevolumes'),
					   url(r'^addvolume/(?P<g_id>[\w\.-]+)/?$', 'addvolume'),

					   url(r'^viewgateway/(?P<g_id>[\w\.-]+)/?$', 'viewgateway'),					   
					   url(r'^allgateways/?$', 'allgateways'),
                       url(r'^create/?$', 'create'),
                       url(r'^delete/(?P<g_id>[\w\.-]+)/?$', 'delete'),
                       url(r'^create/(?P<volume_name>[\w\.-]+)/(?P<g_name>[\w\.-]+)/(?P<g_password>[\w\.-]+)/(?P<host>[\w\.-]+)/(?P<port>[\d]+)/?', 'urlcreate'),
                       url(r'^delete/(?P<g_name>[\w\.-]+)/(?P<g_password>[\w\.-]+)/?', 'urldelete'),
)
