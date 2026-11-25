
# IntakeAPI

## Import the libraries and modules
import os
import json
import time
import asyncio
import logging
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from pydantic import (BaseModel, Field, ConfigDict, 
                      TypeAdapter, ValidationError)


##TODO: Path Finding
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EVENTS_FILE = os.path.join(BASE_DIR, "..", "event_generator",\
                            "events.jsonl")

##TODO: Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - \
                    %(levelname)s - %(message)s")


##TODO: CONSTANTS
ENCODING = "utf-8"

MAX_EVENTS = 50
ONE_MINUTE = 60

##TODO: Validate Events through Schema
class SchemaValidator(BaseModel):
    model_config = ConfigDict(extra="allow")
    event_id:str 
    event_type:str
    timestamp:str
    source_system:str
    business_unit:str
    schema_version:str
    seller_id:str

class Events:
    def __init__(self):
        self.window = timedelta(seconds=ONE_MINUTE)
        self.events_data = deque()
        self.seller_data = defaultdict(deque)
        self.sqsEventData = defaultdict(deque)
        self.sameEvents = deque()


    async def _rate_limit(self) ->deque:
        ##TODO: Received Events
        with open(EVENTS_FILE, "r", encoding=ENCODING) as events:
            for event in events:
                if event.strip():
                    json_events = json.loads(event.strip())
                    if json_events["event_id"] not in self.sameEvents:

                        ##TODO: Check Same Events
                        self.sameEvents.append(json_events["event_id"])

                        seller_id = json_events["seller_id"]
                        event_timestamp = datetime.fromisoformat\
                            (json_events["timestamp"])

                        timestamps = self.seller_data[seller_id]

                        while timestamps and (event_timestamp-timestamps[0])\
                              >= self.window:
                            timestamps.popleft()

                        if len(timestamps) >= MAX_EVENTS:
                            await asyncio.sleep(ONE_MINUTE)
                            timestamps.clear()
                        timestamps.append(event_timestamp)
                        self.events_data.append(json_events)
                    else:
                        logging.warning(f"DuplicateEventError: The event \
                                        {json_events["event_id"]}\
                                         has already existed.")

        try:              
            batch_validator = TypeAdapter(list[SchemaValidator])
            validateEvents = batch_validator.validate_python(self.events_data)
            return self.events_data
        except ValidationError as e:
            logging.error(f"ValidationError:\n{e}")

    async def sqs(self):
        rate_limited_events = await self._rate_limit()
        for event in rate_limited_events:
            business_unit = event["business_unit"]
            sqs_events = self.sqsEventData[business_unit]
            sqs_events.append(event)
        return self.sqsEventData
        

    async def validatedEvents(self):
        return await self.sqs()

# if __name__ == "__main__":
#     events = Events()
#     print(asyncio.run(events.validatedEvents()))