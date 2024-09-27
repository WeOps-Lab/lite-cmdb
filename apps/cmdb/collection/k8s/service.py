from datetime import datetime, timezone

from apps.cmdb.collection.common import Collection
from apps.cmdb.collection.k8s.constants import (
    COLLECTION_METRICS,
    NAMESPACE_CLUSTER_RELATION,
    NODE_CLUSTER_RELATION,
    POD_NAMESPACE_RELATION,
    POD_WORKLOAD_RELATION,
    WORKLOAD_NAME_DICT,
    WORKLOAD_NAMESPACE_RELATION,
    WORKLOAD_TYPE_DICT,
    WORKLOAD_WORKLOAD_RELATION,
)
from apps.cmdb.constants import INSTANCE, INSTANCE_ASSOCIATION
from apps.cmdb.graph.neo4j import Neo4jClient
from apps.cmdb.services.model import ModelManage


class MetricsCannula:
    def __init__(self, organization: list, cluster_name: str):
        self.organization = organization
        self.cluster_name = cluster_name
        self.collection_metrics = self.get_collection_metrics()
        self.now_time = datetime.now(timezone.utc).isoformat()

    def get_collection_metrics(self):
        """获取采集指标"""
        new_metrics = NewMetrics(self.cluster_name)
        return new_metrics.run()

    def namespace_controller(self):
        """namespace控制器"""
        params = [
            {"field": "model_id", "type": "str=", "value": "k8s_namespace"},
            {"field": "collect_task", "type": "str=", "value": self.cluster_name},
        ]
        with Neo4jClient() as ag:
            already_namespace, _ = ag.query_entity(INSTANCE, params)

        return Management(
            self.organization,
            self.cluster_name,
            "k8s_namespace",
            already_namespace,
            self.collection_metrics["namespace"],
            ["inst_name"],
            self.now_time,
        ).controller()

    def workload_controller(self):
        """workload控制器"""
        params = [
            {"field": "model_id", "type": "str=", "value": "k8s_workload"},
            {"field": "collect_task", "type": "str=", "value": self.cluster_name},
        ]
        with Neo4jClient() as ag:
            already_workload, _ = ag.query_entity(INSTANCE, params)

        return Management(
            self.organization,
            self.cluster_name,
            "k8s_workload",
            already_workload,
            self.collection_metrics["workload"],
            ["inst_name"],
            self.now_time,
        ).controller()

    def pod_controller(self):
        """pod控制器"""
        params = [
            {"field": "model_id", "type": "str=", "value": "k8s_pod"},
            {"field": "collect_task", "type": "str=", "value": self.cluster_name},
        ]
        with Neo4jClient() as ag:
            already_pod, _ = ag.query_entity(INSTANCE, params)
        return Management(
            self.organization,
            self.cluster_name,
            "k8s_pod",
            already_pod,
            self.collection_metrics["pod"],
            ["inst_name"],
            self.now_time,
        ).controller()

    def node_controller(self):
        """node控制器"""
        params = [
            {"field": "model_id", "type": "str=", "value": "k8s_node"},
            {"field": "collect_task", "type": "str=", "value": self.cluster_name},
        ]
        with Neo4jClient() as ag:
            already_node, _ = ag.query_entity(INSTANCE, params)
        return Management(
            self.organization,
            self.cluster_name,
            "k8s_node",
            already_node,
            self.collection_metrics["node"],
            ["inst_name"],
            self.now_time,
        ).controller()

    def cannula_controller(self):
        """纳管控制器"""
        k8s_node = self.node_controller()
        k8s_namespace = self.namespace_controller()
        k8s_workload = self.workload_controller()
        k8s_pod = self.pod_controller()
        return dict(node=k8s_node, namespace=k8s_namespace, workload=k8s_workload, pod=k8s_pod)


class NewMetrics:
    def __init__(self, cluster_name):
        self.cluster_name = cluster_name
        self.metrics = self.get_metrics()
        self.collection_metrics_dict = {i: [] for i in COLLECTION_METRICS.keys()}

    def get_metrics(self):
        metrics = []
        metrics.extend(COLLECTION_METRICS["namespace"])
        metrics.extend(COLLECTION_METRICS["workload"])
        metrics.extend(COLLECTION_METRICS["node"])
        metrics.extend(COLLECTION_METRICS["pod"])
        return metrics

    def query_data(self):
        """查询数据"""
        sql = f"""
        WITH latest_time AS (
            SELECT max(TimeUnix) AS latest_timestamp
            FROM otel.otel_metrics_gauge
            WHERE MetricName IN {self.metrics}
            AND Attributes['instance_id'] = '{self.cluster_name}'
        )
        SELECT
            MetricName, Attributes, TimeUnix, Value
        FROM
            otel.otel_metrics_gauge
        WHERE
            MetricName IN {self.metrics}
            AND Attributes['instance_id'] = '{self.cluster_name}'
            AND TimeUnix = (SELECT latest_timestamp FROM latest_time)
        FORMAT JSON
        """
        data = Collection().query(sql)
        return data.get("data", [])

    def format_data(self, data):
        """格式化数据"""

        for index_data in data:
            index_dict = dict(
                index_key=index_data["MetricName"],
                index_value=index_data["Value"],
                index_time=index_data["TimeUnix"],
                **index_data["Attributes"],
            )
            if index_data["MetricName"] in COLLECTION_METRICS["namespace"]:
                self.collection_metrics_dict["namespace"].append(index_dict)
            elif index_data["MetricName"] in COLLECTION_METRICS["workload"]:
                self.collection_metrics_dict["workload"].append(index_dict)
            elif index_data["MetricName"] in COLLECTION_METRICS["node"]:
                self.collection_metrics_dict["node"].append(index_dict)
            elif index_data["MetricName"] in COLLECTION_METRICS["pod"]:
                self.collection_metrics_dict["pod"].append(index_dict)

        self.format_namespace_metrics()
        self.format_pod_metrics()
        self.format_node_metrics()
        self.format_workload_metrics()

    def format_namespace_metrics(self):
        """格式化namespace"""
        result = []
        for index_data in self.collection_metrics_dict["namespace"]:
            result.append(
                dict(
                    inst_name=f"{index_data['instance_id']}/{index_data['namespace']}",
                    name=index_data["namespace"],
                    assos=[
                        {
                            "model_id": "k8s_cluster",
                            "inst_name": self.cluster_name,
                            "asst_id": "belong",
                            "model_asst_id": NAMESPACE_CLUSTER_RELATION,
                        }
                    ],
                )
            )
        self.collection_metrics_dict["namespace"] = result

    def format_workload_metrics(self):
        """格式化workload"""
        replicaset_owner_dict, replicaset_metrics, workload_metrics = {}, [], []
        for index_data in self.collection_metrics_dict["workload"]:
            if index_data["index_key"] == "kube_replicaset_labels":
                replicaset_metrics.append(index_data)
            elif index_data["index_key"] == "kube_replicaset_owner":
                replicaset_owner_dict[(index_data["namespace"], index_data["replicaset"])] = index_data
            else:
                workload_metrics.append(index_data)
        for replicaset_info in replicaset_metrics:
            owner_info = replicaset_owner_dict.get((replicaset_info["namespace"], replicaset_info["replicaset"]))
            if owner_info and owner_info["owner_kind"].lower() in WORKLOAD_TYPE_DICT.values():
                replicaset_info.update(
                    owner_kind=owner_info["owner_kind"].lower(),
                    owner_name=owner_info["owner_name"],
                )
        workload_metrics.extend(replicaset_metrics)
        result = []
        for workload_info in workload_metrics:
            inst_name_key = WORKLOAD_NAME_DICT[workload_info["index_key"]]
            namespase = (f"{workload_info['instance_id']}/{workload_info['namespace']}",)
            if workload_info.get("owner_kind"):
                # 关联workload
                assos = [
                    {
                        "model_id": "k8s_workload",
                        "inst_name": f"{namespase}/{workload_info['owner_name']}",
                        "asst_id": "group",
                        "model_asst_id": WORKLOAD_WORKLOAD_RELATION,
                    }
                ]
            else:
                # 关联namespace
                workload_info.update(k8s_namespace=namespase)
                assos = [
                    {
                        "model_id": "k8s_namespace",
                        "inst_name": namespase,
                        "asst_id": "belong",
                        "model_asst_id": WORKLOAD_NAMESPACE_RELATION,
                    }
                ]

            result.append(
                dict(
                    inst_name=f"{workload_info['instance_id']}/{workload_info['namespace']}/{workload_info[inst_name_key]}",  # noqa
                    name=workload_info[inst_name_key],
                    workload_type=WORKLOAD_TYPE_DICT[workload_info["index_key"]],
                    assos=assos,
                )
            )

        self.collection_metrics_dict["workload"] = result

    def format_pod_metrics(self):
        """格式化pod"""
        inst_index_info_list, inst_limit_resource_dict, inst_request_resource_dict = [], {}, {}
        for index_data in self.collection_metrics_dict["pod"]:
            if index_data["index_key"] == "kube_pod_info":
                inst_index_info_list.append(index_data)
            elif index_data["index_key"] == "kube_pod_container_resource_limits":
                inst_limit_resource_dict[(index_data["pod"], index_data["resource"])] = index_data["index_value"]
            elif index_data["index_key"] == "kube_pod_container_resource_requests":
                inst_request_resource_dict[(index_data["pod"], index_data["resource"])] = index_data["index_value"]

        result = []
        for inst_index_info in inst_index_info_list:
            namespase = f"{inst_index_info['instance_id']}/{inst_index_info['namespace']}"

            info = dict(
                inst_name=inst_index_info["uid"],
                name=inst_index_info["pod"],
                ip_addr=inst_index_info["pod_ip"],
            )

            limit_cpu = inst_limit_resource_dict.get((inst_index_info["pod"], "cpu"))
            if limit_cpu:
                info.update(limit_cpu=float(limit_cpu))
            limit_memory = inst_limit_resource_dict.get((inst_index_info["pod"], "memory"))
            if limit_memory:
                info.update(limit_memory=int(float(limit_memory) / 1024**3))
            request_cpu = inst_request_resource_dict.get((inst_index_info["pod"], "cpu"))
            if request_cpu:
                info.update(request_cpu=float(request_cpu))
            request_memory = inst_request_resource_dict.get((inst_index_info["pod"], "memory"))
            if request_memory:
                info.update(request_memory=int(float(request_memory) / 1024**3))

            assos = [
                {
                    "model_id": "k8s_node",
                    "inst_name": f"{inst_index_info['instance_id']}/{inst_index_info['node']}",
                    "asst_id": "group",
                    "model_asst_id": POD_WORKLOAD_RELATION,
                }
            ]

            if inst_index_info["created_by_kind"] in WORKLOAD_TYPE_DICT.values():
                # 关联workload
                inst_index_info.update(k8s_workload=f"{inst_index_info['created_by_name']}")
                assos.append(
                    {
                        "model_id": "k8s_workload",
                        "inst_name": f"{namespase}/{inst_index_info['created_by_name']}",
                        "asst_id": "group",
                        "model_asst_id": POD_WORKLOAD_RELATION,
                    }
                )
            else:
                # 关联namespace
                inst_index_info.update(k8s_namespace=namespase)
                assos.append(
                    {
                        "model_id": "k8s_namespace",
                        "inst_name": namespase,
                        "asst_id": "group",
                        "model_asst_id": POD_NAMESPACE_RELATION,
                    }
                )
            info.update(assos=assos)
            result.append(info)

        self.collection_metrics_dict["pod"] = result

    def format_node_metrics(self):
        """格式化node"""
        inst_index_info_list, inst_resource_dict, inst_role_dict = [], {}, {}
        for index_data in self.collection_metrics_dict["node"]:
            if index_data["index_key"] == "kube_node_info":
                inst_index_info_list.append(index_data)
            elif index_data["index_key"] == "kube_node_role":
                if index_data["node"] not in inst_role_dict:
                    inst_role_dict[index_data["node"]] = []
                inst_role_dict[index_data["node"]].append(index_data["role"])
            elif index_data["index_key"] == "kube_node_status_capacity":
                inst_resource_dict[(index_data["node"], index_data["resource"])] = index_data["index_value"]
        result = []
        for inst_index_info in inst_index_info_list:
            info = dict(
                inst_name=f"{inst_index_info['instance_id']}/{inst_index_info['node']}",
                name=inst_index_info["node"],
                ip_addr=inst_index_info.get("internal_ip"),
                os_version=inst_index_info.get("os_image"),
                kernel_version=inst_index_info.get("kernel_version"),
                kubelet_version=inst_index_info.get("kubelet_version"),
                container_runtime_version=inst_index_info.get("container_runtime_version"),
                pod_cidr=inst_index_info.get("pod_cidr"),
                assos=[
                    {
                        "model_id": "k8s_cluster",
                        "inst_name": self.cluster_name,
                        "asst_id": "group",
                        "model_asst_id": NODE_CLUSTER_RELATION,
                    }
                ],
            )
            info = {k: v for k, v in info.items() if v}
            cpu = inst_resource_dict.get((inst_index_info["node"], "cpu"))
            if cpu:
                info.update(cpu=int(cpu))
            memory = inst_resource_dict.get((inst_index_info["node"], "memory"))
            if memory:
                info.update(memory=int(float(memory) / 1024**3))
            disk = inst_resource_dict.get((inst_index_info["node"], "ephemeral_storage"))
            if disk:
                info.update(storage=int(float(disk) / 1024**3))
            role = ",".join(inst_role_dict.get(inst_index_info["node"], []))
            if role:
                info.update(role=role)
            result.append(info)
        self.collection_metrics_dict["node"] = result

    def run(self):
        """执行"""
        data = self.query_data()
        self.format_data(data)
        return self.collection_metrics_dict


class Management:
    def __init__(self, organization, cluster_name, model_id, old_data, new_data, unique_keys, collect_time):
        self.organization = organization
        self.collect_time = collect_time
        self.cluster_name = cluster_name
        self.model_id = model_id
        self.old_data = old_data
        self.new_data = new_data
        self.unique_keys = unique_keys
        self.check_attr_map = self.get_check_attr_map()

    def get_check_attr_map(self):
        attrs = ModelManage.search_model_attr(self.model_id)
        check_attr_map = dict(is_only={}, is_required={}, editable={})
        for attr in attrs:
            if attr["is_only"]:
                check_attr_map["is_only"][attr["attr_id"]] = attr["attr_name"]
            if attr["is_required"]:
                check_attr_map["is_required"][attr["attr_id"]] = attr["attr_name"]
            if attr["editable"]:
                check_attr_map["editable"][attr["attr_id"]] = attr["attr_name"]

        return check_attr_map

    def format_data(self):
        """数据格式化"""
        old_map, new_map = {}, {}
        for info in self.old_data:
            key = tuple(info[key] for key in self.unique_keys)
            old_map[key] = info
        for info in self.new_data:
            key = tuple(info[key] for key in self.unique_keys)
            new_map[key] = info
        return old_map, new_map

    def contrast(self, old_map, new_map):
        """数据对比"""
        add_list, update_list, delete_list = [], [], []
        for key, info in new_map.items():
            if key not in old_map:
                add_list.append(info)
            else:
                info.update(_id=old_map[key]["_id"])
                update_list.append(info)
        for key, info in old_map.items():
            if key not in new_map:
                delete_list.append(info)
        return add_list, update_list, delete_list

    def add_inst(self, inst_list):
        """新增实例"""
        if not inst_list:
            return

        result = {"success": [], "failed": []}
        with Neo4jClient() as ag:
            exist_items, _ = ag.query_entity(INSTANCE, [{"field": "model_id", "type": "str=", "value": self.model_id}])
            for instance_info in inst_list:
                try:
                    instance_info.update(
                        model_id=self.model_id,
                        organization=self.organization,
                        collect_task=self.cluster_name,
                        auto_collect=True,
                        collect_time=self.collect_time,
                    )
                    assos = instance_info.pop("assos", [])
                    entity = ag.create_entity(INSTANCE, instance_info, self.check_attr_map, exist_items)
                    # 创建关联
                    assos_result = self.setting_assos(entity, assos)
                    exist_items.append(entity)
                    result["success"].append(dict(inst_info=entity, assos_result=assos_result))
                except Exception as e:
                    result["failed"].append({"instance_info": instance_info, "error": str(e)})
        return result

    def update_inst(self, inst_list):
        """更新实例"""
        if not inst_list:
            return

        result = {"success": [], "failed": []}
        with Neo4jClient() as ag:
            exist_items, _ = ag.query_entity(INSTANCE, [{"field": "model_id", "type": "str=", "value": self.model_id}])
            for instance_info in inst_list:
                try:
                    instance_info.update(
                        model_id=self.model_id,
                        organization=self.organization,
                        collect_task=self.cluster_name,
                        auto_collect=True,
                        collect_time=self.collect_time,
                    )
                    assos = instance_info.pop("assos", [])
                    exist_items = [i for i in exist_items if i["_id"] != instance_info["_id"]]
                    entity = ag.set_entity_properties(
                        INSTANCE, [instance_info["_id"]], instance_info, self.check_attr_map, exist_items
                    )
                    # 更新关联
                    assos_result = self.setting_assos(dict(model_id=self.model_id, _id=entity[0]["_id"]), assos)
                    exist_items.append(entity[0])
                    result["success"].append(dict(inst_info=entity[0], assos_result=assos_result))
                except Exception as e:
                    result["failed"].append({"instance_info": instance_info, "error": str(e)})
        return result

    def delete_inst(self, inst_list):
        """删除实例"""
        if not inst_list:
            return

        result = {"success": [], "failed": []}
        with Neo4jClient() as ag:
            for instance_info in inst_list:
                try:
                    ag.detach_delete_entity(INSTANCE, instance_info["_id"])
                    result["success"].append(instance_info)
                except Exception as e:
                    result["failed"].append({"instance_info": instance_info, "error": str(e)})
        return result

    def setting_assos(self, src_info, dst_list):
        """设置关联关系"""
        assos_result = {"success": [], "failed": []}
        for dst_info in dst_list:
            try:
                with Neo4jClient() as ag:
                    dst_entity, _ = ag.query_entity(
                        INSTANCE,
                        [
                            {"field": "model_id", "type": "str=", "value": dst_info["model_id"]},
                            {"field": "inst_name", "type": "str=", "value": dst_info["inst_name"]},
                        ],
                    )
                    if not dst_entity:
                        raise Exception(f"target instance {dst_info['model_id']}:{dst_info['inst_name']} not found")
                    dst_id = dst_entity[0]["_id"]
                    asso_info = dict(
                        model_asst_id=dst_info["model_asst_id"],
                        src_model_id=src_info["model_id"],
                        src_inst_id=src_info["_id"],
                        dst_model_id=dst_info["model_id"],
                        dst_inst_id=dst_id,
                        asst_id=dst_info["asst_id"],
                    )
                    ag.create_edge(
                        INSTANCE_ASSOCIATION, src_info["_id"], INSTANCE, dst_id, INSTANCE, asso_info, "model_asst_id"
                    )
                    assos_result["success"].append(asso_info)
            except Exception as e:
                assos_result["failed"].append(
                    {"src_info": src_info, "dst_info": dst_info, "error": getattr(e, "message", e)}
                )
        return assos_result

    def controller(self):
        old_map, new_map = self.format_data()
        add_list, update_list, delete_list = self.contrast(old_map, new_map)
        delete_result = self.delete_inst(delete_list)
        add_result = self.add_inst(add_list)
        update_result = self.update_inst(update_list)
        return dict(add=add_result, update=update_result, delete=delete_result)
