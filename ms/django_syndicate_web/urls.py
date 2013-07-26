from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
                        url(r'^syn/RG/',
                         include('django_rg.urls')),

                        url(r'^syn/AG/',
                         include('django_ag.urls')),

                        url(r'^syn/UG/',
                         include('django_ug.urls')),

						url(r'^syn/volume/',
                         include('django_volume.urls')),

						url(r'^syn/?',
                         include('django_home.urls')),
)
