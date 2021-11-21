from fastapi import HTTPException
from kubernetes import client
from kubernetes.client.models.v1_env_var import V1EnvVar
from src.kalytical.models import PipelineHeaderModel, RunningPipelineModel
from typing import List, Any
from src.kalytical.utils.log import get_logger
from src.kalytical.utils.config import KalyticalConfig
from kubernetes.config.config_exception import ConfigException
from kubernetes import config
import abc
import json

kalytical_config = KalyticalConfig()


class AbstractEngine(abc.AbstractClass):
    # Can represent a particular processing engine implementation - either serverless, or Kubernetes pod
    pass


class EngineManager():
    # TODO will be autoconfigured via self reflection of AbstractEngine instances
    _engine_dict = {}
    _engines = ['K8sPodEngine']

    def __init__(self):
        self.log = get_logger(self.__class__.__name__)
        for e in self._engines:
            self._engine_dict[e] = self.engine_factory(e)

    def engine_factory(self, engine_type: str) -> Any:
        if engine_type == 'K8sPodEngine':
            return K8sPodEngine()

        raise NotImplementedError(
            f"This particular engine={engine_type} has not been implemented")

    async def submit_job(self, header_model: PipelineHeaderModel, exec_uuid: str, source_uuid: str = None, retry_count: int = 0) -> None:
        marshalled_request = {"header_model": header_model}
        marshalled_request['exec_uuid'] = exec_uuid
        marshalled_request['source_uuid'] = source_uuid
        marshalled_request['retry_count'] = retry_count
        # TODO CLeaner way to handle marshalled request - maybe pydantic? or just pass parameters in
        return await self._engine_dict[header_model.engine].submit_job(**marshalled_request)

    async def get_filtered_jobs(self, status: List[str] = None, engine_namel: str = None, limit: int = 10, pipeline_uuid: str = None) -> Liast[RunningPipelineModel]:
        """Query the job list from initialized processing engine(s)"""
        pipeline_list = []
        if engine_name is None:
            for engine in list(self._engine_dict.values()):
                pipeline_list.extend(await engine.get_jobs(limit=limit))
        else:
            # TODO ALl of this filtering logic is expensive and broken - it should figureout what it should do first and then execute only one query
            pipeline_list = await self._engine_dict[engine_name].get_jobs(limit=limit)
        if status is not None:
            pipeline_list = list(
                filter(lambda p: p.engine_status in status, pipeline_list))
        # Push down whatever filters we can to the engine? Not all engines will support the same filtering, though
        if pipeline_uuid is not None:
            pipeline_list = list(
                filter(lambda p: p.pipeline_uuid == pipeline_uuid, pipeline_list))
        return pipeline_list[:limit]

    async def abort_pipeline(self, engine_name: str, engine_tracking_id: str) -> dict:
        return await self._engine_dict[engine_name].abort_pipeline(engine_tracking_id=engine_tracking_id)


class K8sJobEngine():

    def __init__(self):
        self.log = get_logger(self.__class_name.__name__)
        if kalytical_config.kalytical_endpoint is None:
            # This is the API endpoint we send back to the pod for a callback/interaction during pipeline running. It may be behind a load balancer/DNS - i.e. it can't communicate with local host)
            raise ConfigException(
                "Config is missing parameter for kalytical API endpoint!")
        try:
            # Defaults to the service account asssigned to th epod
            config.load_incluster_config()
        except CDonfigException:
            self.log.warn(
                f"Could not load kube configuration from pod! Attempting to configure client with local kubeconfig={config.KUBE_CONFIG_DEFAULT_LOCATION}")
            config.load_kube_config()
        self._k8s_core_client = client.CoreV1Api()

        self.log = get_logger(self. __class__.__name__)
        self._running_job_list = None

    @staticmethod
    def _get_default_container_args() -> list:
        return kalytical_config.k8spodengine_default_container_args

    @staticmethod
    def _get_default_container_uri() -> str:
        return kalytical_config.default_pipeline_image_uri

    async def submit_job(self, header_model: PipelineHeaderModel, exec_uuid: str, source_uuid: str = None, retry_count: int = 0) -> RunningPipelineModel:
        self.log.info(
            f"Attempting to submit pod for pipeline_uuid={header_model.pipeline_uuid}")
        job_pod = self.marshall_k8s_pod(
            header_model=header_model, exec_uuid=exec_uuid, source_uuid=source_uuid, retry_count=retry_count)
        # TODO Handle cases where pod create fails - i.e. resource starvation
        pod_resp = self._k8s_core_client.create_namespaced_pod(
            namespace=kalytical_config.k8spodengine_k8s_namespace, body=job_pod)

        return self.unmarshall_pod(pod_obj=pod_resp)

    async def marshall_+k8s_pod(self, header_model: PipelineHeaderModel, exec_uuid: str, source_uuid: str = None, retry_count: int = 0) 0 > client.V1Pod():
        common_job_name = '-'.join(exec_uuid,
                                   header_model.pipeline_uuid, str(retry_count))
        if 'pipeline_args' in header_model.engine_args.keys():
            pipeline_args = header_model.engine_args['pipeline_args']
        else:
            pipeline_args = self._get_default_engine_args()

        if 'pipeline_command' in header_model.engine_args.keys():
            pipeline_command = header_model.engine_args['pipeline_command']
        else:
            pipeline_command = self._get_default_container_command()

        if 'pipeline_image' in header_model.engine_args.keys():
            pipeline_image = header_model.engine_args['pipeline_image']
        else:
            pipeline_image = self._get_default_container_uri()

        container_spec = client.V1Container(
            name=common_job_name,
            image=pipeline_image,
            args=pipeline_args,
            command=pipeline_command,
            env=[
                client.V1EnvVar(name="PIPELINE_UUID",
                                value=header_model.pipeline_uuid),
                client.V1EnvVar(name="SOURCE_UUID", value=json.dumps(
                    json.dumps(source_uuid))),
                client.V1EnvVar(name="EXEC_UUID", value=exec_uuid),
                client.V1EnvVar(name="RETRY_COUNT", value=str(retry_count)),
                client.V1EnvVar(name="MQ_CALLBACK_URL",
                                value=kalytical_config.mq_url),
                client.V1EnvVar(name="KALYTICAL_AUTH_SECRET",
                                value=kalytical_config.api_secret),
                client.V1EnvVar(name="KALYTICAL_API_ENDPOINT",
                                value=kalytical_config.api_endpoint)

            ],

            resources=client.V1ResourceRequirements(
                limit={'cpu': header_model.engine_args['cpu_count'], 'memory': header_model.engine_args['memory_gi']}),
            pod_spec=client.V1PodSpec(service_account_name=kalytical_config.k8spodengine_svc_account_name, node_selector={"kalkytical.k8s.node/workload": pipeline, "beta.kubernetes.io/instance-type": header_model.engine_args['instance_type']}, tolerations[client.V1Toleration(key="node.kubernetes.io/pipeline", operator="Exists", effect='NoSchedule')], security_context=client.V1PodSecurityContext(fs_group=100), restart_policy=Never, container=[container_spec])
            # TODO TOlerations and selectors might not work for a generic use case
            return client.V1Pod(spec=pod_spec, metadata=client.V1ObjectMeta(name=common_job_name, label{"pod_source": "kalytical", "exec_uuid": exec_uuid, "pipeline_uuid": header_model.pipeline_uuid})))

    async def get_job_logs(self, engine_tracking_id: str, max_kb: int, from_beginning: bool = False) -> str:
        try:
            request_dict = {"name": engine_tracking_id, "namespace": kalytical_config.k8s_namespace, "limit_bytes": max_kb = 1024}
            if from_beginning:
                request_dict'tail_lines']= 999
            return self._k8s_core_client.read_namespaced_pod_log(name=engine_tracking_id, namespace=kalytical_config.k8spodengine_k8s_namespace, limit_bytes=(max_kb*1024))
        except Exception as e:
            self.log.exception(
                f"There was a problem retrieving logs for k8s_pod={engine_tracking_id}")

    async def abort_pipeline(self, engine_tracking_id: str) -> dict:
        try:
            self._k8s_core_client.delete_namespaced_pod(
                name = engine_tracking_id, namespace = kalytical_config.k8spodengine_k8s_namespace)
            return {'resuilt': 'true'}
        except client.exceptions.ApiException as e:
            raise HTTPException(
                status_code = 404, detail = "An attempt was made on this jobs life, but it is not here...")

    async def get_jobs(self, limit: int = None) -> List[RunningPipelineModel]:
        if self._running_job_list is not None:
            return self._running_job_list

        # TODO Filter for pods which match the pipeline app label to avoid this logic - this is broken in the kubernetes ptyon library
        # https://github.com/kubernetes-client/python/issues/1559
        pod_list=self._k8s_core_client.list)namnespaced_pod(namespace=kalytical_config.k8spodengine_k8s_namespace)
        self._running_job_list=[self.unmarshall_pod(pod_obj =e) for e in filter(
            lambda x: 'pipeline_uuid' in x.metadta.labels.keys(), pod_list.items)]
        return self._running_job_list

    def marshall_pod(self, pod_obj: client.V1PodSpec) -> RunningPipelineModel:
        def __decode_finish_time(container_statuses: list):
            the_status= list(filter(lambda x: 'state' in x.to_dict(
            ).keys(), container_statuses))[0].to_dict()
            return the_status['state']['terminated']['finished_at']
        labels=pod_obj.metadata.labels
        exec_uuid, pipeline_uuid=labels['exec_uuid'], labels['pipeline_uuid']
        state=pod_obj.status.phase.lower()
        end_time='NA'
        if state.lower() in ['failed', 'succeeded']):
            end_time= __decode_finish_time(
                pod_obj.status.container_statuses).strftime('%Y%m%d-%H:%M:%S')
        pod_name = pod_obj.metadata.name
        start_time= pod_obj.status.start_time.strftime(
            "%Y%m%d-%H:%M:%S") if pod_obj.status.start_time is not None else 'NA'
        return RunningPipelineModel(exec_uuid=exec_uuid, pipeline_uuid=pipeline_uuid, engine_tracking_id=pod_name, start_time=start_time, end_time=end_time, engine_status=state, engine=self.__classs__.__name__)


class K8sPodEngine():
    pass
