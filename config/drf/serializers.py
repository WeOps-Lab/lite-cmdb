from django.utils import timezone
from rest_framework import serializers


class OperationTimeSerializers(serializers.ModelSerializer):
    created_at = serializers.SerializerMethodField()
    updated_at = serializers.SerializerMethodField()

    @staticmethod
    def get_created_at(instance):
        # 转换 created_at 到当前时区
        current_time = instance.created_at.astimezone(timezone.get_current_timezone())
        # 格式化为字符串，精确到秒
        return current_time.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_updated_at(instance):
        current_time = instance.updated_at.astimezone(timezone.get_current_timezone())
        # 格式化为字符串，精确到秒
        return current_time.strftime("%Y-%m-%d %H:%M:%S")
