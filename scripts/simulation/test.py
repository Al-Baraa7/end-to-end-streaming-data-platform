from datetime import datetime, timedelta, timezone

base = datetime(2031, 3, 1, tzinfo=timezone.utc)

user_created_at = base
subscription_start = base + timedelta(days=1)
subscription_end = base + timedelta(days=30)
video_created_at = base + timedelta(days=2)


event_times = [
    base + timedelta(days=3),
    base + timedelta(days=5),
    base + timedelta(days=10),
]

for event_time in event_times:
    valid = (
        event_time >= user_created_at
        and event_time >= subscription_start
        and event_time <= subscription_end
        and event_time >= video_created_at
    )

    assert valid

print("STREAMING TIMELINE TEST PASSED")