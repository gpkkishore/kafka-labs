-- TUMBLING window: fixed, non-overlapping 10-second buckets, per action type.
-- Event time + watermark: a window [10:00:00, 10:00:10) is emitted once the watermark passes 10:00:10,
-- i.e. after an event with event_time >= 10:00:15 has been seen (5s out-of-orderness allowed).
SET 'pipeline.name' = 'tumbling-action-counts';

INSERT INTO action_counts_10s
SELECT window_start, window_end, action,
       COUNT(*)                AS event_count,
       COUNT(DISTINCT user_id) AS user_count
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, action;
