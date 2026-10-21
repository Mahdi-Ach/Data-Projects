SELECT
    event_type,
    COUNT(*) as total_events,
    MIN(timestamp) as first_event,
    MAX(timestamp) as last_event
FROM events_clean
GROUP BY event_type