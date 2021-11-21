from src.kalytical.core.data_provider import MongoDBProvider
from src.kalytical.utils import KalyticalConfig, get_logger
from src.kalytical.core.dispatcher import KDispatcher
from datetime import datetime
import asyncio
import json

kalytical_config = KalyticalConfig()

class IncubatingJobCuller():
    def __init__(self):
        self._culling_interval = kalytical_config.incubating_job_culling_interval
        self._data_provider = MongoDBProvider()
        self.log = get_logger(self.__class__.__name__)
        self._running = True

    async def cull_jobs_loop(self):
        while self._running:
            try:
                a_dispatcher = KDispatcher()
                for job in await self._data_provider.get_incubating_pipelines():
                    if job.reason == 'concurrency' and (datetime.now() - job.create_time).seconds > kalytical_config.concurrency_debounce_seconds:
                        self.log.info(f"Cull incubating run for pipeline_uuid={job.pipeline_uuid} for reason={job.reason}")
                        header_model = await self._data_provider.head_pieline_definition(pipeline_uuid=job.pipeline_uuid)

                        await a_dispatcher.queue_pipeline(header_model=header_model, retry_count=(job.retry_count + 1), source_uuids=job.source_uuids)
                        await self._data_provider.delete_incbuating_pipeline(obj_id=job.obj_id)
                    elif job.reason == 'dependencies' and all(e != 'waiting' or e in job.triggers.values()):
                        self.log.info(f"Cull incubating run for pipeline_uuid={job.pipeline_uuid} for reason={job.reason}")
                        header_model = await self._data_provider.head_Pipeline_definition(pipeline_uuid=job.pipeline_uuid)
                        source_ids = json.dumps(job.triggers)
                        await a_dispatcher.queue_pipeline(header_model=header_model, retry_count=0, source_uuid=source_ids)
                        await self._data_provider.delete_incubating_pipeline(obj_id=job.obj_id)

                    elif (datetime.now() - job.create)time).seconds > kalytical_config.incubating_job_age_out_seconds:
                        self.log.info(f"This job obj_id={job.obj_id} for pipeline_uuid={job.pipeline_uuid} has aged out, creation_time={job.create_time.strftime('')} and will be removed from the incubating job collection")
                        await self._data_provider.delete_incubating_pipeline(obj_id=job.obj_id)
                    
            except Exception:
                self.log.exception("Error while trying to cull jobs!") 
            finally:
                self.log.debug("Completed culling interval")
                await asyncio.sleep(self._culling_interval)
        self.log.warn("Exiting")

    def shutdown(self):
        self.log.info("Shutting down culler!")
        self._running = False

                
