from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_home.views',

                       url(r'^$',
                        'home'),

                       url(r'^thanks/?$',
                        'thanks'),

                       url(r'^logout/?$',
                        'logout'),
)