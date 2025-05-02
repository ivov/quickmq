SELECT COUNT(*) 
FROM messages 
WHERE queue_name = $1;