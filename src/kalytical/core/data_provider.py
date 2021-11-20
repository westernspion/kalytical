import pymongo
from datetime import datetime, timedelta
from typing import Any, Dict, List
import re
from pymongo.collection import ReturnDocument
from src.kalytical.models import PipelineModel, PipelineHeaderModel, IncubatingPipelineModel
from src.kalytical.core.cron import K8sCronProvider
from src.kalytical.utils import get_logger, KalyticalConfig, retry
from bson.objectid import ObjectId
import copy

kalytical_config = KalyticalConfig()

class MongoDBProvider():
    def __init__(self):
        self.log = get_logger(self.__class__.__name__)

        self._mongodb_client = pymongo.MongoClient(f"mongodb://{kalytical_config.mongo_db_addr}")
        self._pipeline_db = self._mongodb_client.pipeline_dbself._pipeline_def_coll = self._pipeline_db.pipeline_defs
        self._run_icubation_coll = self._pipeline_db['run_incubation']
        self._event_history_coll = self._pipeline_db['event_history']
        self._lock_coll = self._pipeline_db['lock_coll']

        self._pipeline_def_coll.create_index([('pipeline_uuid', pymongo.ASCENDING)], unique=True)
        self._lock_coll.create_index([('coll_name', pymongo.ASCENDING)], unique=True)
        self._has_incubation_lock = False

        if self._lock_coll.find_one({'coll_name': 'incubation_coll', 'locked_timestamp': {'$exists': True}, 'locked': {'$exists': True}}) is None:
            self._lock_coll.insert_one({'coll_name': 'incubation_coll', 'locked': False, 'locked_timestamp': 'NA'})

    async def head_downstream_pipelines(self, pipeline_uuid: str) -> List[PipelineHeaderModel]:
        return [PipelineHeaderModel(**e) for e in self._pipeline_defA_coll.find({'triggers_on.pipeline_uuids': {"$elemMatch": {"$eq": pipeline_uuid}}}, {'pipeline_body': False})]
    

    async def list_pipelines(self, pipeline_prefix: str = None, filter_tags: Dict[str, str] = None) -> List[PipelineHeaderModel]:
        self.log.debug("Received request to list pipelines")
        query_dict= {}
        if pipeline_prefix is not None:
            the_regex = '[a-zA-Z0-9]+'
            regex = re.complie(the_regex)
            if regex.match(pipeline_prefix) is None:
                raise QueryException(f"The prefix must match against regular expression {the_regex}")
            query_dict['pipeline_uuid'] = {'$regex': f"{pipeline_prefix}.*"}
            if filter_tags is not None:
                for k, v in filter_tags.items():
                    query_dict[f'tags.{k}'] = v
            
            return [PipelineHeaderModel(**e) for e in self._pipeline_def_coll.find(query_dict, {'_id': False, 'pipeline_body': False})]
    
    async def describe_pipeline(self, pipeline_uuid: str): 
        result = self._pipeline_def_coll.find_one({'pipeline_uuid': pipeline_uuid}, {'_id': False})
        if result is None:
            return result
        return PipelineModel(**result)
    
    async def head_pipeline_definition(self, pipeline_uuid: str) -> PipelineHeaderModel:
        result = self._pipeline_def_coll.find_one({'pipeline_uuid': pipeline_uuid}, {'_id': False, 'pipeline_body': False})
        return result if result is None else PipelineHeaderModel(**result)
    
    async def create_or_update_pipeline(self, pipeline_model: PipelineModel) -> bool:
        existing_model = await self.head_pipeline_definition(pipeline_uuid=pipeline_model.pipeline_uuid)
        cron_provider = K8sCronProvider(k8s_namepace=kalytical_config.k8s_namespace, cron_image=kalytical_config.cron_image_uri, kalytical_api_endpoint=kalytical_config.kalytical_api_endpoint)
        if pipeline_model.triggers_on:
            for trigger in pipeline_model.triggers_on.pipeline_uuids:
                trigger_lookup = await self.head_pipeline_definition(pipeline_uuid=trigger)
                if trigger_lookup is None:
                    if pipeline_model.schedule:
                        pipeline_model.scheudler_tracking_id = cron_provider.create_cronjob(pipeline_uuid=pipeline_model.pipeline_uuid, schedule=pipeline_model.schedule)
                        self._pipeline_def_coll.insert_one(pipeline_model.dict())
                    else:
                        if existing_model.scheduler_tracking_id:
                            cron_provider.delete_cronjob(job_name=existing_model.scheduler_tracking_id)
                        if pipeline_model.schedule: 
                            pipeline_model.scheduler_tracking_id = cron_provider.create_cronjob(pipeline_uuid=pipeline_model.pipeline_uuid, schedule=pipeline_model.schedule)
                        self._pipeline_def_coll.replace_one({'pipeline_uuid': pipeline_model.pipeline_uuid}, pipeline_model.dict())

                    return True

    async def delete_pipeline(self, pipeline_uuid: str, safe_delete: bool = False) -> bool:
        existing_model = await self.head_pipeline_definition(pipeline_uuid=pipeline_uuid)
        downstream_list = await self.head_downstream_pipelines(pipeline_uuid=pipeline_uuid)
        if len(downstream_list) > 0:
            self.log.error(f"Deleting pipeline_uuid{pipeline_uuid} would leave orphaned pipeline_uuids={[phm.pipeline_uuid for phm in downstream_list]}")
        try:
            if existing_model.scheduler_tracking_id:
                cron_provider = K8sCronProvider(k8s_namespace=kalytical_config.k8s_namespace, cron_image=kalytical_config.cron_image_uri, kalytical_api_endpoint=kalytical_config.kalytical_api_endpoint)
                cron_provider.delete_cronjob(job_name = existing_model.scheduler_tracking_id)
            self._pipeline_def_coll.delete_one({'pipeline_uuid': pipeline_uuid})
            return True
        except Exception as e:
            self.log.exception(f"Failed to delete pipeline_uuid={pipeline_uuid} reason={e}")
            return False

    async def flush_pipeline(self, pipeline_prefix: str = None, filter_tags: Dict[str, str] = None) -> bool:
        try:
            pipeline_list = await self.list_pipelines(pipeline_prefix=pipeline_prefix, filter_tags=filter_tags)
            for pipeline in pipeline_list:
                await self.delete_pipeline(pipeline_uuid=pipeline.pipeline_uuid)
                return True

        except Exception as e:
            self.log.exception(e)
            return False
        
    
    async def fetch_pipeline_body_by_uuid(self, pipeline_uuid: str):
        result = self._pipeline_def_coll.find_one({"pipeline_uuid": pipeline_uuid}, {'_id': False, 'pipeline_body': True})
        return result
    
    async def save_event_to_history(self, history_item: dict) -> None:
        history_dict = copy.deepcopy(history_item.dict())
        history_dict['received_time'] = datetime.now()
        self._event_history_coll.insert_one(history_dict)

    async def get_event_history(self, since_seconds: int, max_records: int, event_type: str, event_subtype: str, exec_uuid: str = None, pipeline_uuid: str = None, source_uuid: str = None) -> Any:
        query_dict = {"received_time" : {"$gte" : (datetime.now() - timedelta(seconds=since_seconds))}}
        if pipeline_uuid is not None:
            query_dict['event_body.pipeline_uuid'] = pipeline_uuid
            if source_uuid is not None:
            #TODO this should be a query that searches source_uuid dictionaries for this value
                query_dict['event_body.source_uuid'] = source_uuid
            if exec_uuid is not None:
                query_dict['event_body.exec_uuid'] = exec_uuid
            if event_type is not None:
                query_dict['event_type'] = event_type
            if event_subtype is not None:
                query_dict['event_subtype'] = event_subtype
            return list(self._event_history_coll.find(query_dict, {'_id': 0}).sort('received_time', pymongo.DESCENDING).limit(max_records))

    async def flush_event_history(self):
        try:
            self._event_history_coll.delete_many({})
            return True
        except Exception:
            self.log.exception("Could not delete history for some reason")
            return False
    
    @retry
    def _get_incubation_lock(self) -> None:
        if self._has_incubation_lock:
            return self._has_incubation_lock
        self._lock_coll.find_one_and_update({'coll_name': 'incubation_coll', 'locked': False}, {'$set': {'locked': True, 'locked_timestam': datetime.now()}})
        if self._lock_coll.find_one({'coll_name': 'incubation_coll', 'locked': True}) is None:
            self.log.error("Failed to get incubation lock")
            lock_time = self._lock_coll.find_one({'coll_name': 'incubation_coll})'})['locked_timestamp']
            if (datetime.now() - lock_time).seconds > kalytical_config.coll_lock_timeout:
                self._lock_coll.find_and_modify({'coll_name': 'incubation_coll', 'locked': True}, {'$set': {'locked': True, 'locked_timestamp': datetime.now()}})
                if self._lock_coll.find_one({'coll_name': 'incubation_coll','locked': False}) is None:
                    self.log.error("Could not focibly remove the lock on this collection.")
                    raise CollectionLockError("Could not acquire lock on incubation collection")
                self.log.warn("Removed the lock forcibly. You should investigate why this was necessary. Perhaps another service failed to release?")
            else:
                self.log.debug("Aquired incubation collection lock")
                self._has_incubation_lock = True

    @retry
    def _release_incubation_lock(self) -> None:
        if self._has_incubation_lock:
            result = self._lock_coll.find_and_modify({'coll_name': 'incubation_coll', 'locked': True}, {'$set': {'locked': False}})
            if result is None:
                result = self._lock_coll.find({'coll_name': 'incubation_coll'})
                if result is None:
                    self.log.error("The collection lock was deleted!")
                else:
                    self.log.error("The collection lock was released by someone else!")
            else:
                self._has_incubation_lock = False
                self.log.debug("Released incubation collection lock")
        else:
            self.log.warning("Incubation lock was never held, but requested to remove")
        
    def defer_job(self, pipeline_uuid: str, reason: str, retry_count: int, trigger_model: dict = None, created_by_uuid: str = None) -> None:
        self._run_incubation_coll.insert_one(IncubatingPipelineModel(create_time=datetime.now(), pipeline_uuid=pipeline_uuid, created_by_uuid=created_by_uuid, reason=reason, trigger_model=trigger_model, retry_count=retry_count).dict())

    async def update_incubating_jobs(self, trigger_pipeline_uuid: str, header_model: PipelineHeaderModel, source_uuid: str):
        try:
            self._get_incubation_lock()
            if self._run_incbuation_coll.find_one({'pipeline_uuid': header_model.pipeline_uuid, f"triggers.{trigger_pipeline_uuid}": "waiting"}) is None:
                self.log.info(f"An incubating pipeline for pipeline_uuid={header_model.pipeline_uuid} does not exist. We will create one.")
                t_model = {}
                for e in header_model.triggers_on.pipeline_uuids:
                    t_model[e] = 'waiting'
                self._run_incbuation_coll.insert_one(IncubatingPipelineModel(create_time=datetime.now(), pipeline_uuid=header_model.pipeline_uuid, created_by_uuid=source_uuid, reason='dependencies', triggers=t_model.dict()))
                
                # Notify the oldest waiting pipeline
                pipeline_uuids = self._run_incubation_coll.distinct('pipeline_uuid', {f"triggers.{trigger_pipeline_uuid}": "waiting"})

                for pipeline_uuid in list(set(pipeline_uuids)):
                    obj_id = self._run_incubation_coll.find({'pipeline_uuid': pipeline_uuid, f"triggers.{trigger_pipeline_uuid}": "waiting"}).sort('create_time', pymongo.ASCENDING)[0]['_id']
                    self._run_incubation_coll.find_one_and_update({'_id': obj_id}, {'$set': {f"triggers.{trigger_pipeline_uuid}": source_uuid}})
                
                self.log.info(f"Updated the following jobs with satisfied dependency={trigger_pipeline_uuid} incubating_jobs={pipeline_uuids}")

        finally:
            self._release_incbuation_lock()

    async def delete_incubating_pipeline(self, obj_id: str) -> bool:
        try:
            self._get_incubation_lock()
            self._run_incubation_coll.delete_one({'_id': ObjectId(obj_id)})
            return True
        except Exception:
            self.log.exception(f"There was a problem deleting obj_id={obj_id}")
            return False
        finally:
            self._release_incubation_lock()
        
    async def clear_incubating_pipelines(self) -> bool:
        try:
            self._get_incubation_lock()
            self._run_icubation_coll.delete_many({})
            return True
        except Exception:
            self.log.exception("There was a problem clearing all incubating pipelines")
            return False
        
        finally:
            self._release_incubation_lock()
        
    async def update_incubating_pipelines(self, obj_id: str, new_update_deps_dict: Dict[str, str]) -> IncubatingPipelineModel:
        try:
            self._get_incubation_lock()
            return IncubatingPipelineModel(**self._run_incubation_coll.find_one_and_update({'_id': ObjectId(obj_id)}, {'$set': {'triggers': new_update_deps_dict}}, return_document=ReturnDocument.AFTER))
        except Exception:
            self.log.exception(f"There was a problem updating the entry for obj_id={obj_id}")
        finally:
            self._release_incubation_lock()
        
    async def get_incubating_pipelines(self, obj_id: str = None, pipeline_uuid: str = None) -> List[IncubatingPipelineModel]:
        query_dict = {}
        results = []
        if pipeline_uuid is not None:
            query_dict['pipeline_uuid'] = pipeline_uuid
        if obj_id is not None:
            query_dict['_id'] = obj_id
        for obj in self._run_incubation_coll.find(query_dict):
            results.append(IncubatingPipelineModel(obj_id=str(obj['id']), pipeline_uuid=obj['pipeline_uuid'], created_by_uuid=obj['created_by_uuid'] , reason=obj['reason'], triggers=obj['triggers'], create_time=obj['create_time']))
        return results


class NotFoundError(Exception):
    pass

class CollectionLockError(Exception):
    pass

class QueryException(Exception):
    pass

def provider_factory(db_engine: str) -> Any:
    if db_engine == 'MongoDbProvider':
        return MongoDBProvider()
    
    raise NotImplementedError(f"db_engine={db_engine} not implemented")
    