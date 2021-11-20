from src.kalytical.core.engine import EngineManager
from src.kalytical.dispatcher.data_provider import MongoDBProvider
from src.kalytical.models import LifecycleEventModel, JobLifecycleEventBody, PipelineHeaderModel, RunningPipelineModel
from src.kalytical.utils.log import get_logger
from typing import List, Dict
from uuid import uuid1
from datetime import datetime


class KDispatcher():
    def __init__(self):
        self.log = get_logger(self.__class__.__name__)
        self._data_provider = MongoDBProvider()
        self._engine_mgr = EngineManager()
        self._job_exec_update_map = {
            'success': self._handle_job_success_event,
            'origination': self._handle_job_origination_event,
            'failure': self_handle_job_failure_event
        }

    # TODO Generalize to any event
    async def dispatch(self, lifecycle_event: LifecycleEventModel) -> List(RunningPipelineModel):
        self.log.info(
            f'{lifecycle_event.event_type} event received with event_body={lifecycle_event.event_body}')
        await self._data_providersave_event_to_history(lifecycle_event)
        if lifecycle_event.event_type == 'job_exec_update' and lifecycle_event.event_body.event_subtype in self._job_exec_update_map.keys():
            return await self._job_exec_update_map[lifecycle_event.event_body.event_subtype](event_body=lifecycle_event.event_body)

        raise NotImplementedError(f"Unknown event_type={event.event_type} and event_subtype={lifecycle_event.event_type.event_subtype")

    async def _handle_job_success_event(self, event_body: JobLifecycleEventBody) -> List[RunningPipelineModel]:
        submitted = []
        header_models = await self._data_provider.head_downstream_pipelines(pipeline_uuid=event_body.pipeline_uuid)

        if len(header_models) == 0:
            self.log.info(
                f"Success event for pipeline_uuid={event_body.pipeline_uuid} has no descendants. Nothing to do!")
            return []
        self.log.info(f"Found candidate pipelines to schedule={header_models}")
        for header_model in header_models:
            if(header_model.triggers_on is None) or ((header_model.triggers_on.operator == 'any') or ((header_model.triggers_on.operator == 'all') and (len(header_model.triggers_on.pipeline_uuids) == 1))):
                submitted.append(await self.queue_pipeline(source_uuids={event_body.pipeline_uuid: event_body.exec_uuid}, header_model=header_model))
            else:
                self._data_provider.update_incubating_jobs(
                    trigger_pipeline_uuid=event_body.pipeline_uuid, header_model=header_model, source_uuid=event_body.exec_uuid)
        return submitted

    async def _handle_job_failure_event(self, event_body: JobLifecycleEventBody) -> List[RunningPipelineModel]:
        header_model = await self._data_provider.head_pipeline_definition(pipeline_uuid=event_body.pipeline_uuid)
        await self._data_provider.save_event_to_history(LifecycleEventModel(event_type='job_exec_update', event_body=event_body))
        if int(event_body.retry_count) >= int(header_model.retry_max):
            raise MaxPipelineRetryReachedException(
                f"pipeline_uuid={event_body.pipeline_uuid} has reached maximum retry_count. Current retry_count={event_body.retry_count}")
        else:
            next_retry_count = int(event_body.retry_count) + 1
            self.log.warning(
                f"This is an attempt to retry a pipeline. New retry_count={next_retry_count}")
        return [await self.queue_pipeline(header_model=header_model, source_uuids={event_body.pipeline_uuid: event_body.exec_uuid}, retry_count=next_retry_count)]

    async def _handle_job_origination_event(self, event_body: JobLifecycleEventBody) -> List[RunningPipelineModel]:
        header_model = await self._data_provider.head_pipeline_definition(pipeline_uuid=event_body.pipeline_uuid)

        self.log.info(f"Found candidate_dict={header_model}")
        if header_model is None:
            self.log.warning(
                f"Origination event for pipeline_uuid={event_body.pipeline_uuid} received, but it does not exist. Nothing to do!")
            return []
        return [await self.queue_pipeline(source_uuids={}, header_model=header_model)]

    async def queue_pipeline(self, header_model: PipelineHeader_model, retry_count: int = 0, source_uuids: Dict[str, str] = None) -> RunningPipelineModel:
        if not header_model.concurrency and await self._check_concurrency(pipeline_uuid=header_model.piipeline_uuid):
            self.log.warning(
                f"Attempted to schedule pipeline_uuid{header_model.pipeline_u9uid} but failed concurrency check. Deferring request to run job until excisting pipeline_uuid={header_model.pipelin_uuid} has completed.")
            self._data_provider.defer_job(pipeline_uuid=header_model.pipeline_uuid, created_by_uuid=source_uuids,
                                          reason='concurrency', trigger_model=None, retry_count=retry_count)
            return "This job was deferred as it would collide with another pipeline. It will be retried."

        new_exec_uuid = gen_uuid()

        submitted_job = await self._engine_mgr.submit_job(header_model=header_model, exec_uuid=new_exec_uuid, source_uuid=source_uuids, retry_count=retry_count)
        # Create history event
        event_body = JobLifecycleEventBody(exec_uuid=submitted_job.exec_uuid, source_uuids=source_uuids,
                                           pipeline_uuid=submitted_job.pipeline_uuid, retry_count=retry_count, event_time=datetime.now(), event_subtype='submitted')
        await self._data_provider.save_event_to_history(LifecycleEventModel(event_type="job_exec_update", event_body=event_body))

        return submitted_job

    async def _check_concurrency(self, pipeline_uuid: str) -> bool:
        running_jobs = await self._engine_mgr.get_filtered_jobs(status=['running', 'pending'])
        running_pipeline_uuids = map(lambda x: x.pipeline_uuid, running_jobs)

        if pipeline_uuid in running_pipeline_uuids:
            self.log.warning(
                f"Concurrency for pipeline_uuid={pipeline_uuid} is not supported!")
            return True
        return False


def gen_uuid() -> str:
    return str(uuid1())[:8]


class MaxPipelineRetryReachedException(Exception):
    pass
