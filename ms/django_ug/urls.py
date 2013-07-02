from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_ug.views',
                       url(r'^create/?', 'create'),
                       url(r'^delete/?', 'delete'),
)