from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_home.views',
                       url(r'^$', 'home'),
                       url(r'^thanks/?$', 'thanks'),
                       url(r'^make_user/(?P<email>.*)', 'make_user'),
                       url(r'^settings/?$', 'settings'),
                       url(r'^downloads/?$','downloads'),
                       url(r'^logout/?$', 'logout'),

)