from django.contrib import admin
from rest_framework import routers

from apps.core.views.user_view import UserView
from apps.core.views.user_group import UserGroupViewSet

admin.site.site_title = "WeOps-Lite 管理"
admin.site.site_header = admin.site.site_title
public_router = routers.DefaultRouter()
public_router.register(r"api/public/user_view", UserView, basename="user_view")
public_router.register(r"api/user_group", UserGroupViewSet, basename="user_group")

urlpatterns = public_router.urls
