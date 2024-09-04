from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import viewsets
from rest_framework.decorators import action

from apps.cmdb.services.credential import CredentialManage
from apps.core.utils.web_utils import WebUtils


class CredentialViewSet(viewsets.ViewSet):
    @swagger_auto_schema(
        operation_id="create_credential",
        operation_description="创建凭据",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "credential_type": openapi.Schema(type=openapi.TYPE_STRING, description="凭据类型"),
                "data": openapi.Schema(type=openapi.TYPE_OBJECT, description="凭据数据"),
            },
            required=["credential_type", "data"],
        ),
    )
    def create(self, request):
        result = CredentialManage.create_credential(
            request.data["credential_type"],
            request.data["data"],
            request.userinfo.get("username", ""),
        )
        return WebUtils.response_success(result)

    @swagger_auto_schema(
        operation_id="vault_detail",
        operation_description="查询凭据详情",
        manual_parameters=[
            openapi.Parameter("id", openapi.IN_PATH, description="凭据ID", type=openapi.TYPE_INTEGER),
        ],
    )
    def retrieve(self, request, pk: str):
        data = CredentialManage.vault_detail(int(pk))
        return WebUtils.response_success(data)

    @swagger_auto_schema(
        operation_id="encryption_field",
        operation_description="获取加密字段值",
        manual_parameters=[
            openapi.Parameter(
                "id",
                openapi.IN_PATH,
                description="凭据ID",
                type=openapi.TYPE_INTEGER,
            ),
            openapi.Parameter(
                "field",
                openapi.IN_QUERY,
                description="字段",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    @action(detail=True, methods=["get"], url_path="encryption_field")
    def encryption_field(self, request, pk: str, field: str):
        data = CredentialManage.get_encryption_field(int(pk), field)
        return WebUtils.response_success(data)

    @swagger_auto_schema(
        operation_id="credential_list",
        operation_description="查询凭据列表",
        manual_parameters=[
            openapi.Parameter(
                "credential_type",
                openapi.IN_QUERY,
                description="凭据类型",
                type=openapi.TYPE_STRING,
            )
        ],
    )
    def list(self, request):
        credential_type = request.GET.get("credential_type")
        result = CredentialManage.credential_list(
            credential_type,
            request.userinfo.get("username", ""),
            request.GET.get("page", 1),
            request.GET.get("page_size", 10),
        )
        return WebUtils.response_success(result)

    @swagger_auto_schema(
        operation_id="batch_delete_credential",
        operation_description="批量删除凭据",
        manual_parameters=[openapi.Parameter("ids", openapi.IN_QUERY, description="凭据Ids", type=openapi.TYPE_STRING)],
    )
    def destroy(self, request):
        ids = request.GET.get("ids")
        ids = ids.split(",") if ids else []
        CredentialManage.batch_delete_credential(ids)
        return WebUtils.response_success()

    @swagger_auto_schema(
        operation_id="update_credential",
        operation_description="更新凭据",
        manual_parameters=[openapi.Parameter("id", openapi.IN_PATH, description="凭据id", type=openapi.TYPE_STRING)],
        request_body=openapi.Schema(type=openapi.TYPE_OBJECT, description="凭据数据"),
    )
    def update(self, request, pk: str):
        CredentialManage.update_credential(int(pk), request.data)
        return WebUtils.response_success()
