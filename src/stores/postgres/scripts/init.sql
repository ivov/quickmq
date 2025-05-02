CREATE TABLE IF NOT EXISTS queues (
  name TEXT PRIMARY KEY,
  max_msg_size INTEGER NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  queue_name TEXT NOT NULL REFERENCES queues(name),
  content TEXT NOT NULL,
  priority INTEGER NOT NULL,
  enqueued_at TIMESTAMP WITH TIME ZONE NOT NULL,
  visibility_ts BIGINT,
  receive_count INTEGER DEFAULT 0,
  CONSTRAINT valid_priority CHECK (priority BETWEEN 1 AND 9)
);

CREATE INDEX IF NOT EXISTS idx_messages_queue_priority_enqueued_at 
ON messages(queue_name, priority, enqueued_at);

CREATE INDEX IF NOT EXISTS idx_messages_visibility_ts
ON messages(visibility_ts);

CREATE OR REPLACE FUNCTION now_ms() RETURNS BIGINT AS $$
BEGIN
  RETURN (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
END;
$$ LANGUAGE plpgsql;