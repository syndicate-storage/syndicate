from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_home.views',

                       url(r'^$',
                        'home', name='home'),

                       url(r'^thanks/?$',
                        'thanks', name='thanks'),

                       url(r'^logout/?$',
                        'logout', name='logout'),

                       url(r'^providers/?$', 
                        'providers', name='providers'),

                       url(r'^tutorial/?$', 
                        'tutorial', name='tutorial'),

                       url(r'^faq/?$', 
                        'faq', name='faq'),

                       url(r'^about/?$', 
                        'about', name='about')
)