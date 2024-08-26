# -*- coding: utf-8 -*-
"""
自定义drf pagination 综合页码和url
使用方法:
config/default  REST_FRAMEWORK 中添加 如下配置

REST_FRAMEWORK = {
    "DEFAULT_PAGINATION_CLASS": "packages.drf.pagination.CustomPageNumberPagination",
}

"""

from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class CustomPageNumberPagination(PageNumberPagination):
    page_size_query_param = "page_size"
    max_page_size = 10000

    def paginate_queryset(self, queryset, request, view=None):
        page_size = request.GET.get("page_size")
        if page_size is None or page_size in ["0", "-1"]:
            return None
        return super().paginate_queryset(queryset, request, view)

    def get_paginated_response(self, data):
        return Response(
            dict(
                [
                    ("count", self.page.paginator.count),
                    ("items", data),
                ]
            )
        )
