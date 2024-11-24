CREATE TABLE messages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    content VARCHAR(255),
    ackFromDs1 VARCHAR(255),
    ackFromDs2 VARCHAR(255)
);
