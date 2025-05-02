INSERT INTO queues (name, max_msg_size, created_at) 
VALUES ($1, $2, NOW());