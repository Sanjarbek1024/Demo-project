USE BankingETL;
GO

-- ================================================================
-- USERS
-- ================================================================

IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_users')
    ALTER TABLE users DROP CONSTRAINT pk_users;
GO

ALTER TABLE users ALTER COLUMN id INT NOT NULL;
GO

ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);
GO

-- ================================================================
-- CARDS
-- ================================================================

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_cards_users')
    ALTER TABLE cards DROP CONSTRAINT fk_cards_users;
GO

IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_cards')
    ALTER TABLE cards DROP CONSTRAINT pk_cards;
GO

ALTER TABLE cards ALTER COLUMN id INT NOT NULL;
GO

ALTER TABLE cards ALTER COLUMN user_id INT NOT NULL;
GO

ALTER TABLE cards ADD CONSTRAINT pk_cards PRIMARY KEY (id);
GO

ALTER TABLE cards ADD CONSTRAINT fk_cards_users FOREIGN KEY (user_id) REFERENCES users(id);
GO



-- ================================================================
-- TRANSACTIONS
-- ================================================================

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_transactions_from_card')
    ALTER TABLE transactions DROP CONSTRAINT fk_transactions_from_card;
GO

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_transactions_to_card')
    ALTER TABLE transactions DROP CONSTRAINT fk_transactions_to_card;
GO

IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_transactions')
    ALTER TABLE transactions DROP CONSTRAINT pk_transactions;
GO

ALTER TABLE transactions ALTER COLUMN id INT NOT NULL;
GO

ALTER TABLE transactions ALTER COLUMN from_card_id INT NOT NULL;
GO

ALTER TABLE transactions ALTER COLUMN to_card_id INT NOT NULL;
GO

ALTER TABLE transactions ADD CONSTRAINT pk_transactions PRIMARY KEY (id);
GO

ALTER TABLE transactions ADD CONSTRAINT fk_transactions_from_card FOREIGN KEY (from_card_id) REFERENCES cards(id);
GO

ALTER TABLE transactions ADD CONSTRAINT fk_transactions_to_card FOREIGN KEY (to_card_id) REFERENCES cards(id);
GO


-- ================================================================
-- SCHEDULED PAYMENTS
-- ================================================================

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_sched_payments_card')
    ALTER TABLE scheduled_payments DROP CONSTRAINT fk_sched_payments_card;
GO

IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_scheduled_payments')
    ALTER TABLE scheduled_payments DROP CONSTRAINT pk_scheduled_payments;
GO

ALTER TABLE scheduled_payments ALTER COLUMN id INT NOT NULL;
GO

ALTER TABLE scheduled_payments ALTER COLUMN card_id INT NOT NULL;
GO

ALTER TABLE scheduled_payments ADD CONSTRAINT pk_scheduled_payments PRIMARY KEY (id);
GO

ALTER TABLE scheduled_payments ADD CONSTRAINT fk_sched_payments_card FOREIGN KEY (card_id) REFERENCES cards(id);
GO


-- ================================================================
-- LOGS
-- ================================================================

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_logs_transaction')
    ALTER TABLE logs DROP CONSTRAINT fk_logs_transaction;
GO

IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_logs')
    ALTER TABLE logs DROP CONSTRAINT pk_logs;
GO

ALTER TABLE logs ALTER COLUMN id INT NOT NULL;
GO

ALTER TABLE logs ALTER COLUMN transaction_id INT NOT NULL;
GO

ALTER TABLE logs ADD CONSTRAINT pk_logs PRIMARY KEY (id);
GO

ALTER TABLE logs ADD CONSTRAINT fk_logs_transaction FOREIGN KEY (transaction_id) REFERENCES transactions(id);
GO

-- ================================================================
-- REPORTS
-- ================================================================

IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_reports')
    ALTER TABLE reports DROP CONSTRAINT pk_reports;
GO

ALTER TABLE reports ALTER COLUMN id INT NOT NULL;
GO

ALTER TABLE reports ADD CONSTRAINT pk_reports PRIMARY KEY (id);
GO


-- ================================================================
-- DERIVED TABLES
-- ================================================================

IF OBJECT_ID('fraud_detection', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_fraud_detection')
        ALTER TABLE fraud_detection DROP CONSTRAINT pk_fraud_detection;
END
GO

ALTER TABLE fraud_detection ALTER COLUMN transaction_id INT NOT NULL;
GO

ALTER TABLE fraud_detection ADD CONSTRAINT pk_fraud_detection PRIMARY KEY (transaction_id);
GO

-- ================================================================
-- VIP USERS
-- ================================================================

IF OBJECT_ID('vip_users', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_vip_users')
        ALTER TABLE vip_users DROP CONSTRAINT pk_vip_users;
END
GO

ALTER TABLE vip_users ALTER COLUMN user_id INT NOT NULL;
GO

ALTER TABLE vip_users ADD CONSTRAINT pk_vip_users PRIMARY KEY (user_id);
GO

ALTER TABLE vip_users ADD CONSTRAINT fk_vip_users FOREIGN KEY (user_id) REFERENCES users(id);
GO

-- ================================================================
-- blocked_users
-- ================================================================

IF OBJECT_ID('blocked_users', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'pk_blocked_users')
        ALTER TABLE blocked_users DROP CONSTRAINT pk_blocked_users;
END
GO

ALTER TABLE blocked_users ALTER COLUMN card_id INT NOT NULL;
GO

ALTER TABLE blocked_users ADD CONSTRAINT pk_blocked_users PRIMARY KEY (card_id);
GO