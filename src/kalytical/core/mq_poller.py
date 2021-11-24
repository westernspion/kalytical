import aioboto3
import asyncio
import json
from src.kalytical.models import LifecycleEventModel, JobLifecycleEventBody
from src.kalytical.utils import KalyticalConfig, get_logger
from typing import Any

kalytical_config = KalyticalConfig()

class MQ_Poller():
    def __init__(self):
        self.log = get_logger(self.__class__.__name__)
        self._session = aioboto3.Session()
        self._running = True

    async def fetch_message_loop(self):
        while self._running:
            try:
                async with self_session.client('sqs') as sqs_client:
                    response = await sqs_client.receive_message(QueueUrl=kalytical_config.sqs_url, WaitTimeSeconds=2)
                    if 'Messages' in response.keys():
                        self.log.info(f"Received message_count={len(response['Messages'])}")
                        for message in response['Messages']:
                            await self._handle_message(message=message, sqs_client=sqs_client)
                self.log.debug("Completed polling interval")
            except Exception:
                self.log.exception("Error while trying to poll for messages!")

            finally:
                await sqs_client.delete_message(QueueUrl=kalytical_config.sqs_url, ReceiptHandle=message['ReceiptHandle'])
    def _unmarshall_sqs(self, message_dict: dict) -> LifecycleEventModel:
        try:
            return LifecycleEventModel(event_type=message_dict['event_type'], event_body=JobLifecycleEventBody(**message_dict['event_body']))

        except Exception:
            self.log.exception(f"Could not parse lifecycle_event={message_dict}")
    
    def shutdown(self):
        self.log.info("Shutting down poller!")
        self._running = False