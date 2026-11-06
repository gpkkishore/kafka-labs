-- 15-kafka-connect: source database for the JDBC source connector
CREATE DATABASE IF NOT EXISTS shop;
USE shop;

CREATE TABLE customers (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(200) NOT NULL,
  tier        VARCHAR(20)  NOT NULL DEFAULT 'bronze',
  updated_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  status      VARCHAR(20) NOT NULL DEFAULT 'NEW',
  created_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

INSERT INTO customers (name, email, tier) VALUES
  ('Alice Smith', 'alice@example.com', 'gold'),
  ('Bob Jones',   'bob@example.com',   'silver'),
  ('Carol White', 'carol@example.com', 'bronze');

INSERT INTO orders (customer_id, amount, status) VALUES (1, 25.50, 'NEW'), (2, 99.99, 'NEW'), (1, 10.00, 'PAID');

GRANT ALL PRIVILEGES ON shop.* TO 'app'@'%';
