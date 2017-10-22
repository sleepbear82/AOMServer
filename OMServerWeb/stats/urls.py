from django.conf.urls import url

from stats.views import *

urlpatterns = [
    url(r'^$',index),
    url(r'scheduler_load/$',scheduler_load),
    url(r'scheduler_rebalance/$',scheduler_rebalance),
]
