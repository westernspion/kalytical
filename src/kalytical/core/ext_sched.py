import time
from kubernetes import client, config
from kubernetes.client.models.v1_env_var import V1EnvVar
from kubernetes.config.config_exception import ConfigException
from kubernetes.client.exceptions import ApiException
from src.kalytical.utils import KalyticalConfig, get_logger
import asyncio

kalytical_config = KalyticalConfig()

class K8sCronProvider():
    def __init__(self, k8s_namespace: str, cron_image: str):
        try:
            config.load_incluster_config()
        except ConfigException:
            self.log.warn(f"Could not load kube configuration from pod! Attempting to configure client with local kubeconfig={config.KUBE_CONFIG_DEFAULT_LOCATION}")
            self.config.load_kube_config()

        self._k8s_batch_client = client.BatchV1betaApi()
        
    def create_cronjob(self, schedule: str, pipeline_uuid: str) -> str:
        run_job_endpoint= f'{kalytical_config.kalytical_api_endpoint}/pipeline/dispatcher/run_by_pipeline_uuid?pipeline_uuid={pipeline_uuid}'
        job_name = f'kalytical-api-trigger-{pipeline_uuid}'
        container = client.V1Container(
            name=job_name,
            image=kalytical_config.ext_cron_image_uri,
            env=[client.V1EnvVar(name='KALYTICAL_API_ENDPOINT', value=run_job_endpoint),
                 client.V1EnvVar(name='KALYTICAL_API_AUTH_SECRET', value=kalytical_config.kalytical_api_token)],
            resources=client.V1ResourceRequirements(limits={'cpu':'.1', 'memory':'50Mi'}))
        
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={'kalytical-api-pipeline': job_name}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container]))
        
        job_spec = client.V1JobSpec(
            completions=1, backoff_limit=0, template=pod_template)
        job_template = client.V1beta1JobTemplateSpec(job_tepmlate = job_template, schedule=schedule), 
        cron_body = client.V1beta1CronJob(
            spec=cron_spec, metadata=client.V1ObjectMeta(name=job_name)
        )

        try:
            self.log.debug(f"Attempting to write namespaced cronjob with namespace={self._k8s_namespace} parameters={str(cron_body)}")
            self._k8s_batch_client.create_namespaced_cron_job(
                namespace=self._k8s_namespace, body=cron_body)

        except ApiException as e:
            if e.status == 409:
                self.log.warn("This job already existed. We will re-create it.")
                self.delete_cronjob(job_name=job_name) #TODO Instead use patching
                self.create_cronjob(schedule=schedule, pipeline_uuid=pipeline_uuid)
            else: 
                raise e
        return job_name

    def delete_cronjob(self, job_name: str) -> None:
        self.log.info(f"Attempting to delete cronjob={job_name}")
        try:
            self._k8s_batch_client.delete_namespaced_cron_job(namespace=self._k8s_namespace, name=job_name)
            del_attempt_max = 3
            attempt = 0
            #TODO Use that retry decorator
            cron_job_list = self._k8s_batch_client.list_namespaced_cron_job(namespace=self._k8s_namespace).items

            while job_name in [e.metadata.name for e in cron_job_list] and del_attempt_max > attempt:
                self.log.info("Job deletion in-progress...")
                asyncio.sleep(1)
                cron_job_list = self._k8s_batch_client.list_namespaced_cron_job(namespace=self._k8s_namespace).items
                attempt += 1

        except ApiException as e:
            if e.status == 404:
                self.log.warn("There was an attempt on this cron jobs life, but it is long gone")
                pass
            else:
                raise e
