WITH next_msg AS (
  SELECT id, content, enqueued_at, COALESCE(receive_count, 0) + 1 as receive_count
  FROM messages
  WHERE queue_name = $1 AND (visibility_ts IS NULL OR visibility_ts < now_ms())
  ORDER BY priority ASC, enqueued_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE messages
SET visibility_ts = (now_ms() + $2::BIGINT), receive_count = next_msg.receive_count
FROM next_msg
WHERE messages.id = next_msg.id
RETURNING messages.id, messages.content, messages.enqueued_at, messages.visibility_ts, messages.receive_count;
