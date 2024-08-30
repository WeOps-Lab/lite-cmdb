import logging

from django.conf import settings
from django.core.cache import caches
from django.utils import translation
from django.utils.deprecation import MiddlewareMixin
from django.utils.translation import gettext as _

from apps.core.utils.keycloak_client import KeyCloakClient
from apps.core.utils.web_utils import WebUtils

cache = caches["login_db"]


class KeyCloakAuthMiddleware(MiddlewareMixin):
    def __init__(self, get_response):
        super().__init__(get_response)
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def set_userinfo(request, token_info, roles, groups):
        """设置用户信息"""
        request.userinfo = {
            "username": token_info.get("username", ""),
            "name": token_info.get("name", ""),
            "email": token_info.get("email", ""),
            "roles": roles,
            "groups": groups,
            "is_superuser": "admin" in roles,
        }

    def process_request(self, request):
        # 开发模式，默认放行
        # if DEBUG is True:
        #     self.set_userinfo(request, DEBUG_USERINFO)
        #     return None
        token = request.META.get(settings.AUTH_TOKEN_HEADER_NAME)
        if token is None:
            return WebUtils.response_401(_("please provide Token"))
        token = token.split("Bearer ")[-1]
        session_key = request.session.session_key
        if session_key:
            cache_token = cache.get(session_key)
            if cache_token == token:
                return None
        client = KeyCloakClient()
        is_active, user_info = client.token_is_valid(token)
        if not is_active:
            return WebUtils.response_401(_("token validation failed"))
        cache.set(session_key, token, settings.LOGIN_CACHE_EXPIRED)
        if user_info.get("locale"):
            translation.activate(user_info["locale"])
        roles = user_info["realm_access"]["roles"]
        groups = client.get_user_groups(user_info["sub"], "admin" in roles)
        self.set_userinfo(request, user_info, roles, groups)
        return None
