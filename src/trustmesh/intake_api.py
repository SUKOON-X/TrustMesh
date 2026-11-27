
## Import the libraries and modules
import os
import json
import time
import asyncio
import logging
import hashlib
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from pydantic import (BaseModel, Field, ConfigDict, TypeAdapter, ValidationError, Json)


##TODO: Path Finding
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EVENTS_FILE = os.path.join(BASE_DIR, "..", "event_generator", "events.jsonl")

##TODO: Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


##TODO: CONSTANTS
ENCODING = "utf-8"
MAX_EVENTS = 50
SECONDS = 60

##TODO: Validate Events through Schema
class SchemaValidator(BaseModel):
    model_config = ConfigDict(extra="allow")
    event_id:str
    event_type:str
    timestamp:str 
    source_system:str
    business_unit:str
    schema_version:str | None = None
    seller_id:str



class Events:
    def __init__(self):
        self.eventsData = deque()
        self.sameEvents = set()

    def _receivedEvents(self) ->deque:
        """Receive incomming e-commerce marketplace events and aslo validate them."""
        raw_events = deque()
        with open(EVENTS_FILE, "r", encoding=ENCODING) as events:
            for event in events:
                if event.strip():
                    dict_events = json.loads(event.strip())
                    try:
                        batchValidator = TypeAdapter(SchemaValidator | list[SchemaValidator])
                        validateEvents = batchValidator.validate_python(dict_events)
                        raw_events.append(dict_events)
                    except ValidationError as e:
                        logging.warning(f"ValidationError: {e}")
        return raw_events

    def _deDuplication(self):
        """Verify duplicate evets and reject them."""
        raw_events = deque()
        for event in self._receivedEvents():
            event_hash = hashlib.sha256(json.dumps(event).encode(ENCODING)).hexdigest()
            if event_hash not in self.sameEvents:
                self.sameEvents.add(event_hash)
                raw_events.append(event)
            else:
                logging.warning(f"FoundDuplicateEvent: The event \
                                {event.get('event_id')} has already existed.")
        return raw_events

    async def _rateLimits(self):
        """Prevent application from too much incomming events from the seller side."""
        raw_events = deque()
        seller_data = defaultdict(deque)
        window = timedelta(seconds=SECONDS)

        for event in self._deDuplication():
            seller_id = event.get("seller_id")
            event_timestamps = datetime.fromisoformat(event.get("timestamp"))
            timestamps = seller_data[seller_id]

            while timestamps and (event_timestamps-timestamps[0]) >= window:
                timestamps.popleft()

            if len(timestamps) >= MAX_EVENTS:
                await asyncio.sleep(SECONDS)
                timestamps.clear()
            timestamps.append(event_timestamps)
            raw_events.append(event)
        return raw_events

    async def sqs(self):
        """Arrange the event according to the busines units."""
        raw_events = defaultdict(deque)
        rateLimitedEvents = await self._rateLimits()
        for event in rateLimitedEvents:
            business_unit = event["business_unit"]
            sqs_events = raw_events[business_unit]
            sqs_events.append(event)
        return raw_events




if __name__ == "__main__":
    events = Events()
    print(asyncio.run(events.sqs()))