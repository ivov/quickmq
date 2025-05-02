INSERT INTO messages (id, queue_name, content, priority, enqueued_at)
VALUES ($1, $2, $3, $4, NOW());