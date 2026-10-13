from copy import deepcopy


class KafkaDirtyDataInjector:

    def inject_events(self, events):

        for index, event in enumerate(events):

            event = deepcopy(event)

            
            if index == 0:
                event.watch_time_sec = None

            
            elif index == 1:
                event.interaction_type = "like"
                event.watch_time_sec = 30

            
            elif index == 2:
                event.interaction_type = "play"
                event.watch_time_sec = 0

            
            elif index == 3:
                event.interaction_type = "play"
                event.watch_time_sec = 121

            
            elif index == 4:
                event.interaction_type = "share"

            
            elif index == 5:
                event.device_type = "console"

            
            elif index == 6:
                # Valid ISO8601 format but logically impossible (Unix epoch)
                # Prevents downstream Spark/Flink parser crashes while preserving dirty semantics
                event.event_timestamp = "1970-01-01T00:00:00Z"

            yield event

    def inject(self, *, events):
        return self.inject_events(events)