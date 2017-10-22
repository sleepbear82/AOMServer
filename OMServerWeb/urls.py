from django import VERSION  
if VERSION[0:2]>(1,9):  
    from django.conf.urls import include, url  
elif VERSION[0:2]>(1,3):  
    from django.conf.urls import include, patterns, url  
else:  
    from django.conf.urls.defaults import include, patterns, url

from stats.views import index

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    url(r'^stats/', include('stats.urls')),
    url(r'^$', index),
] + static(settings.STATIC_URL, document_root = settings.STATIC_ROOT)
