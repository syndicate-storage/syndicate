from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_home.views',
                       url(r'^$', 'home'),
                       url(r'^make_user/(?P<email>.*)', 'make_user'),
                       url(r'^allvolumes/?$', 'allvolumes'),
                       url(r'^myvolumes/?$', 'myvolumes'),
                       url(r'^settings/?$', 'settings'),
                       url(r'^downloads/?$','downloads'),
                       url(r'^createvolume/?$', 'createvolume'),
                       url(r'^logout/?$', 'logout'),

)