DELETE FROM messages 
WHERE id = $1 AND queue_name = $2
  AND ($3 = '0' OR visibility_ts = $3::BIGINT);