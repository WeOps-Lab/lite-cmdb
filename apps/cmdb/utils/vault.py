import os

import hvac
from dotenv import load_dotenv

load_dotenv()


class HvacClient(object):
    def __init__(self):
        self.client = self.get_hvac_client()

    def get_hvac_client(self):
        client = hvac.Client(url=os.getenv("VAULT_URL"), token=os.getenv("VAULT_TOKEN"))
        return client

    def set_secret(self, path: str, secret: dict):
        """设置secret"""
        self.client.secrets.kv.v2.create_or_update_secret(path, secret)

    def read_secret(self, path):
        """读取secret"""
        result = self.get_hvac_client().secrets.kv.v2.read_secret(path)
        secret = result.get("data", {}).get("data")
        if not secret:
            raise Exception(f"[vault read secret error] path:{path},result:{result}")
        return secret

    def delete_secret(self, path):
        """删除secret"""
        self.client.secrets.kv.v2.delete_metadata_and_all_versions(path)
