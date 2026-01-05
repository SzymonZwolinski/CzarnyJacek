CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    role VARCHAR(20) DEFAULT 'user'
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10, 2),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_fraud BOOLEAN DEFAULT FALSE
);

-- Haslo: 'admin' (hash bcrypt dla przykladu)
INSERT INTO users (username, password_hash, role) 
VALUES ('admin', '$2b$12$94tEIZ0/Ei3WNN2C3AaFQ.xsC5ElyUYUCkCeb/ltxSyqyzCHCccTu', 'admin');

INSERT INTO transactions (user_id, amount, is_fraud) VALUES 
(1, 100.00, FALSE),
(1, 50000.00, TRUE),
(1, 25.50, FALSE);