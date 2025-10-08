"""
Módulo para construir dinámicamente un archivo docker-compose.yml
"""

import json
from typing import Any, Dict, List

import yaml


class WorkerBuilder:
    def __init__(self, name):
        self.params = {"container_name": name}

    def set_image(self, image: str):
        self.params["image"] = image

    def set_build(self, context: str = ".", dockerfile: str = "/Dockerfile"):
        self.params["build"] = {"context": context, "dockerfile": dockerfile}

    def set_entrypoint(self, entrypoint: str):
        self.params["entrypoint"] = entrypoint

    def set_networks(self, networks: List[str]):
        self.params["networks"] = networks

    def set_depends_on(self, service: str, condition: str = None):
        if "depends_on" not in self.params:
            self.params["depends_on"] = {}
        if condition:
            if service not in self.params["depends_on"]:
                self.params["depends_on"][service] = {}
            self.params["depends_on"][service]["condition"] = condition
        else:
            self.params["depends_on"] = [service]

    def set_environment(self, environment: Dict[str, str]):
        self.params["environment"] = environment

    def set_replicas(self, replicas: int):
        if "environment" not in self.params:
            self.params["environment"] = {}
        self.params["environment"]["REPLICAS"] = str(replicas)

    def set_id(self, replica_id: int):
        if "environment" not in self.params:
            self.params["environment"] = {}
        self.params["environment"]["REPLICA_ID"] = str(replica_id)

    def add_from(self, from_type: str, from_name: str, strategy: str = None, routing_key: List[str] = None):
        if "environment" not in self.params:
            self.params["environment"] = {}
        if "FROM" not in self.params["environment"]:
            self.params["environment"]["FROM"] = json.dumps([])
        current_from = json.loads(self.params["environment"]["FROM"])
        if not strategy or not routing_key:
            current_from.append((from_type, from_name))
        else:
            current_from.append((from_type, from_name, strategy, routing_key))
        self.params["environment"]["FROM"] = json.dumps(current_from)

    def add_to(self, to_type: str, to_name: str, strategy: str = None, routing_key: List[str] = None):
        if "environment" not in self.params:
            self.params["environment"] = {}
        if "TO" not in self.params["environment"]:
            self.params["environment"]["TO"] = json.dumps([])
        current_to = json.loads(self.params["environment"]["TO"])
        if not strategy or not routing_key:
            current_to.append((to_type, to_name))
        else:
            current_to.append((to_type, to_name, strategy, routing_key))
        self.params["environment"]["TO"] = json.dumps(current_to)

    def add_enricher(self, enricher_type: str, enricher: str, strategy: str = None, routing_key: List[str] = None):
        if "environment" not in self.params:
            self.params["environment"] = {}
        if "ENRICHER" not in self.params["environment"]:
            self.params["environment"]["ENRICHER"] = json.dumps([])
        current_enricher = json.loads(self.params["environment"]["ENRICHER"])
        if not strategy or not routing_key:
            current_enricher.append((enricher, strategy))
        else:
            current_enricher.append((enricher_type, enricher, strategy, routing_key))
        self.params["environment"]["ENRICHER"] = json.dumps(current_enricher)

    def set_module_name(self, module_name: str):
        if "environment" not in self.params:
            self.params["environment"] = {}
        self.params["environment"]["MODULE_NAME"] = module_name

    def build(self):
        return self.params

    def set_volume(self, from_vol: str, to_vol: str):
        self.params["volumes"] = [f"{from_vol}:{to_vol}"]
        return self.params

    def set_ports(self, param):
        self.params["ports"] = param
        return self.params

    def set_healthcheck(self, test: str, interval: str, timeout: str, retries: int, start_period: str):
        self.params["healthcheck"] = {
            "test": test,
            "interval": interval,
            "timeout": timeout,
            "retries": retries,
            "start_period": start_period,
        }
        return self


class DockerComposeBuilder:
    def __init__(self):
        self.services = {}

        self._init_networks()
        self._init_volumes()

    def _init_networks(self) -> None:
        """Inicializa las redes necesarias."""
        self.networks = {"coffee": {"driver": "bridge"}}

    def _init_volumes(self) -> None:
        """Inicializa los volúmenes necesarios."""
        self.volumes = {"rabbitmq-volume": None}

    def add_gateway(self) -> "DockerComposeBuilder":
        """Agrega el servicio Gateway."""
        worker = WorkerBuilder("gateway")
        worker.set_build(context=".", dockerfile="./gateway/Dockerfile")
        worker.set_entrypoint(entrypoint="python main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on(service="rabbitmq", condition="service_healthy")

        self.services["gateway"] = worker.build()
        return self

    def add_client(self, full_dataset: bool) -> "DockerComposeBuilder":
        """Agrega el servicio Client."""
        worker = WorkerBuilder("client")
        worker.set_build(context=".", dockerfile="./client/Dockerfile")
        worker.set_entrypoint(entrypoint="python main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on(service="gateway")
        worker.set_environment({"LOGGING_LEVEL": "DEBUG"})
        dataset_from = "./.data" + "/dataset_full" if full_dataset else "/dataset_min"
        worker.set_volume(from_vol=dataset_from, to_vol="/client/.data")

        self.services["client"] = worker.build()
        return self

    def add_demux(
        self, demux_id: int, name: str, from_queue: str, to_queue: str, replicas: int
    ) -> "DockerComposeBuilder":
        """Agrega el servicio Demux."""
        worker = WorkerBuilder(name)
        worker.set_image("demux_worker")
        worker.set_build(context=".", dockerfile="./worker/Dockerfile")
        worker.set_entrypoint(entrypoint="python /worker/demux/demux_main.py")
        worker.set_environment(
            {"PYTHONUNBUFFERED": "1", "LOGGING_LEVEL": "INFO", "MIDDLEWARE_HOST": "rabbitmq", "REPLICAS": str(replicas)}
        )
        worker.set_networks(["coffee"])
        worker.set_depends_on(service="rabbitmq", condition="service_healthy")
        worker.set_id(demux_id)
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        worker.add_to(to_type="EXCHANGE", to_name=to_queue, strategy="FANOUT", routing_key=["common"])

        self.services[name] = worker.build()
        return self

    def add_filter(
        self, name: str, filter_id: int, from_queue: str, replicas: int, to_queues: List[str], module_name: str = None
    ) -> "DockerComposeBuilder":
        worker = WorkerBuilder(name)
        worker.set_image("filter_worker")
        worker.set_id(filter_id)
        worker.set_build(".", "./worker/Dockerfile")
        worker.set_entrypoint("python /worker/filters/filter_main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on("rabbitmq", "service_healthy")
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        [worker.add_to(to_type="QUEUE", to_name=queue) for queue in to_queues]
        worker.set_module_name(module_name)
        worker.set_replicas(replicas)
        self.services[name] = worker.build()
        return self

    def add_aggregator(
        self, name: str, aggregator_id: int, from_queue: str, to_queue: str, module_name: str, replicas
    ) -> "DockerComposeBuilder":
        worker = WorkerBuilder(name)
        worker.set_image("aggregator_worker")
        worker.set_id(aggregator_id)
        worker.set_build(context=".", dockerfile="./worker/Dockerfile")
        worker.set_entrypoint("python /worker/aggregators/aggregator_main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on("rabbitmq", "service_healthy")
        worker.set_module_name(module_name)
        worker.set_replicas(replicas)
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        worker.add_to(to_type="QUEUE", to_name=to_queue)

        self.services[name] = worker.build()
        return self

    def add_merger(
        self, name: str, merger_id: int, from_queue: str, to_queue: str, module_name: str, replicas: int
    ) -> "DockerComposeBuilder":
        worker = WorkerBuilder(name)
        worker.set_image("merger_worker")
        worker.set_id(merger_id)
        worker.set_build(context=".", dockerfile="./worker/Dockerfile")
        worker.set_entrypoint("python /worker/mergers/merger_main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on("rabbitmq", "service_healthy")
        worker.set_replicas(replicas)
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        worker.add_to(to_type="QUEUE", to_name=to_queue)
        worker.set_module_name(module_name)

        self.services[name] = worker.build()
        return self

    def add_enricher(
        self,
        name: str,
        enricher_id: int,
        from_queue: str,
        to: str,
        enricher: str,
        enricher_routing_key: List[str],
        enricher_strategy: str,
        module_name: str,
        replicas: int,
        to_type: str,
        to_strategy: str = None,
        to_routing_key: List[str] = None,
    ) -> "DockerComposeBuilder":
        worker = WorkerBuilder(name)
        worker.set_image("enricher_worker")
        worker.set_id(enricher_id)
        worker.set_build(context=".", dockerfile="./worker/Dockerfile")
        worker.set_entrypoint("python /worker/enrichers/enricher_main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on("rabbitmq", "service_healthy")
        worker.set_replicas(replicas)
        worker.set_module_name(module_name)
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        worker.add_to(to_type=to_type, to_name=to, strategy=to_strategy, routing_key=to_routing_key)
        worker.add_enricher(
            enricher_type="EXCHANGE", enricher=enricher, strategy=enricher_strategy, routing_key=enricher_routing_key
        )

        self.services[name] = worker.build()
        return self

    def add_sink(
        self, name: str, sink_id: int, from_queue: str, to_queue: str, replicas: int, module_name: str
    ) -> "DockerComposeBuilder":
        worker = WorkerBuilder(name=name)
        worker.set_image("sink_worker")
        worker.set_id(sink_id)
        worker.set_build(context=".", dockerfile="./worker/Dockerfile")
        worker.set_entrypoint("python /worker/sinks/sink_main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on(service="rabbitmq", condition="service_healthy")
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        worker.add_to(to_type="QUEUE", to_name=to_queue)
        worker.set_module_name(module_name)
        worker.set_replicas(replicas)

        self.services[name] = worker.build()
        return self

    def add_rabbitmq(self) -> "DockerComposeBuilder":
        worker = WorkerBuilder("rabbitmq")
        worker.set_image("rabbitmq:4.1.4-management")
        worker.set_volume(from_vol="rabbitmq-volume", to_vol="/var/lib/rabbitmq")
        worker.set_environment(
            {"RABBITMQ_DEFAULT_USER": "admin", "RABBITMQ_DEFAULT_PASS": "admin", "RABBITMQ_NODENAME": "rabbit@rabbitmq"}
        )
        worker.set_networks(["coffee"])
        worker.set_ports(["5672:5672", "8080:15672"])
        worker.set_healthcheck(
            test="rabbitmq-diagnostics -q check_running && rabbitmq-diagnostics"
                 " -q check_local_alarms && rabbitmq-diagnostics -q check_port_connectivity",
            interval="5s",
            timeout="10s",
            retries=10,
            start_period="40s",
        )
        self.services["rabbitmq"] = worker.build()

        return self

    def add_router(
        self, name: str, router_id: int, from_queue: str, to_queue: str, module_name: str, replicas: int
    ) -> "DockerComposeBuilder":
        worker = WorkerBuilder(name=name)
        worker.set_image("router_worker")
        worker.set_id(router_id)
        worker.set_build(context=".", dockerfile="./worker/Dockerfile")
        worker.set_entrypoint("python /worker/router/router_main.py")
        worker.set_networks(["coffee"])
        worker.set_depends_on("rabbitmq", "service_healthy")
        worker.set_replicas(replicas)
        worker.set_module_name(module_name)
        worker.add_from(from_type="QUEUE", from_name=from_queue)
        worker.add_to(to_type="QUEUE", to_name=to_queue)  # TODO!: REVISAR

        self.services[name] = worker.build()
        return self

    def build(self) -> Dict[str, Any]:
        return {
            "name": "coffee-shop-analyzer",
            "services": self.services,
            "volumes": self.volumes,
            "networks": self.networks,
        }

    def save(self, output_file: str = "docker-compose.yml") -> None:
        docker_compose = self.build()
        with open(output_file, "w") as f:
            yaml.dump(docker_compose, f, default_flow_style=False, sort_keys=False, width=1000)
        print(f"Archivo {output_file} generado exitosamente!")
