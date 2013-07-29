from django.conf.urls import patterns, include, url

# The long URLS are for admin purposes and will be eliminated (deprecated).

urlpatterns = patterns('django_ug.views',
                       url(r'^changepassword/(?P<g_id>[\w\.-]+)/?$',
                        'changepassword'),       

                       url(r'^changewrite/(?P<g_id>[\w\.-]+)/?$',
                        'changewrite'),       

                       url(r'^changelocation/(?P<g_id>[\w\.-]+)/?$',
                        'changelocation'),

                       url(r'^changevolume/(?P<g_id>[\w\.-]+)/?$',
                        'changevolume'), 

                       url(r'^viewgateway/(?P<g_id>[\w\.-]+)/?$',
                        'viewgateway'),

                       url(r'^allgateways/?$',
                        'allgateways'),

                       url(r'^mygateways/?$',
                        'mygateways'),

                       url(r'^create/?$',
                        'create'),

                       url(r'^delete/(?P<g_id>[\w\.-]+)/?$',
                        'delete'),

                       url(r'^create/(?P<volume_name>[\w\.-]+)/(?P<g_id>[\w\.-]+)/(?P<g_password>[\w\.-]+)/(?P<host>[\w\.-]+)/(?P<port>[\d]+)/(?P<read_write>[\w]+)/?',
                        'urlcreate'),

                       url(r'^delete/(?P<g_id>[\w\.-]+)/(?P<g_password>[\w\.-]+)/?',
                        'urldelete'),
)
