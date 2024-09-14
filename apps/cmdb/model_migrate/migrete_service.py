import ast
import json

import pandas as pd

from apps.cmdb.constants import (
    CLASSIFICATION,
    CREATE_CLASSIFICATION_CHECK_ATTR_MAP,
    CREATE_MODEL_CHECK_ATTR,
    MODEL,
    MODEL_ASSOCIATION,
)
from apps.cmdb.graph.neo4j import Neo4jClient


class ModelMigrate:
    def __init__(self):
        self.model_config = self.get_model_config()

    def get_model_config(self):
        # 读取 Excel 文件
        file_path = "apps/cmdb/model_migrate/model_config.xlsx"

        # 指定第二行（索引1）作为表头，并读取所有 sheet 页
        sheets_dict = pd.read_excel(file_path, sheet_name=None, header=1)

        sheets_map = {}

        # 遍历所有 sheet 页，并将每个 sheet 页的 DataFrame 转换为列表字典
        for sheet_name, sheet_data in sheets_dict.items():
            # 对 NaN 值进行填充，修改原 DataFrame
            sheet_data.fillna("", inplace=True)

            # 如果 sheet_data 是 Series（单列数据），转换为 DataFrame
            if isinstance(sheet_data, pd.Series):
                sheet_data = sheet_data.to_frame()  # 将 Series 转换为 DataFrame

            # 将 DataFrame 转换为字典格式，使用 'records' 使每行成为一个字典
            data = sheet_data.to_dict(orient="records")

            sheets_map[sheet_name] = data

        return sheets_map

    def migrate_classifications(self):
        """初始化模型分类"""
        with Neo4jClient() as ag:
            exist_items, _ = ag.query_entity(CLASSIFICATION, [])
            result = ag.batch_create_entity(
                CLASSIFICATION,
                self.model_config.get("classifications", []),
                CREATE_CLASSIFICATION_CHECK_ATTR_MAP,
                exist_items,
            )
        return result

    def migrate_models(self):
        """初始化模型"""
        models = []
        for model in self.model_config.get("models", []):
            attrs = []
            attr_key = f"attr-{model['model_id']}"
            if attr_key in self.model_config:
                attrs = self.model_config[attr_key]
            for attr in attrs:
                try:
                    attr["option"] = ast.literal_eval(attr["option"])
                except Exception:
                    pass
            models.append({**model, "attrs": json.dumps(attrs)})

        with Neo4jClient() as ag:
            exist_items, _ = ag.query_entity(MODEL, [])
            result = ag.batch_create_entity(MODEL, models, CREATE_MODEL_CHECK_ATTR, exist_items)
        return result

    def migrate_associations(self):
        """初始模型关联"""
        associations = []
        for model in self.model_config.get("models", []):
            asso_key = f"asso-{model['model_id']}"
            if asso_key in self.model_config:
                associations.extend(self.model_config[asso_key])
        with Neo4jClient() as ag:
            models, _ = ag.query_entity(MODEL, [])
            model_map = {i["model_id"]: i["_id"] for i in models}
            asso_list = [
                dict(
                    dst_id=model_map.get(i["dst_model_id"]),
                    src_id=model_map.get(i["src_model_id"]),
                    model_asst_id=f"{i['src_model_id']}_{i['asst_id']}_{i['dst_model_id']}",
                    **i,
                )
                for i in associations
            ]
            result = ag.batch_create_edge(MODEL_ASSOCIATION, MODEL, MODEL, asso_list, "model_asst_id")
        return result

    def main(self):
        # 创建模型分类
        classification_resp = self.migrate_classifications()
        # 创建模型
        model_resp = self.migrate_models()
        # 创建模型关联
        association_resp = self.migrate_associations()

        return dict(classification=classification_resp, model=model_resp, association=association_resp)
