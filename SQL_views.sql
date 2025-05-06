USE BankingETL;
GO

-- =================================================================
-- 1. FRAUD DETECTION VIEW
-- Shubhali tranzaksiyalarni aniqlash (limitdan oshgan, yirik + failed,
-- noma'lum statusdagi holatlar)
-- =================================================================

CREATE OR ALTER VIEW vw_fraud_detection AS
SELECT 
    t.id AS transaction_id,
    t.from_card_id,
    t.to_card_id,
    t.amount,
    c.limit_amount,
    t.status,
    t.created_at,
    t.transaction_type,
    CASE
        WHEN t.amount > c.limit_amount THEN 'Over limit'
        WHEN t.amount > 14000000 AND t.status = 'failed' THEN 'High amount failed'
        WHEN t.status NOT IN ('success', 'pending', 'failed') THEN 'Suspicious status'
    END AS fraud_reason
FROM transactions t
JOIN cards c ON t.from_card_id = c.id
WHERE 
    t.amount > c.limit_amount
    OR (t.amount > 14000000 AND t.status = 'failed')
    OR t.status NOT IN ('success', 'pending', 'failed');
GO

-- =================================================================
-- 2. VIP USERS VIEW
-- Yuqori balans yoki katta tranzaksiya hajmiga ega foydalanuvchilar
-- =================================================================

CREATE OR ALTER VIEW vw_vip_users AS
SELECT 
    u.id AS user_id,
    u.name,
    u.total_balance,
    COUNT(t.id) AS total_transactions,
    SUM(t.amount) AS total_amount,
    CASE 
        WHEN u.total_balance > 800000000 THEN 'High balance'
        WHEN SUM(t.amount) > 100000000 THEN 'High transaction volume'
    END AS vip_reason
FROM users u
LEFT JOIN cards c ON u.id = c.user_id
LEFT JOIN transactions t ON c.id = t.from_card_id
GROUP BY u.id, u.name, u.total_balance, u.is_vip
HAVING 
    u.total_balance > 800000000 OR 
    SUM(t.amount) > 100000000;
GO

-- =================================================================
-- 3. BLOCKED USERS (No activity in 90 days)
-- So‘nggi 90 kun ichida hech qanday tranzaksiya qilmagan foydalanuvchilar
-- =================================================================
CREATE OR ALTER VIEW vw_blocked_users_1 AS
SELECT 
    u.id AS user_id,
    u.name,
    u.email,
    MAX(t.created_at) AS last_transaction_date,
    'No activity in the last 90 days' AS block_reason
FROM dbo.users u
LEFT JOIN dbo.cards c ON u.id = c.user_id
LEFT JOIN dbo.transactions t ON c.id = t.from_card_id
GROUP BY u.id, u.name, u.email
HAVING MAX(t.created_at) IS NULL OR MAX(t.created_at) < DATEADD(DAY, -90, GETDATE());
GO

-- =================================================================
-- 4. BLOCKED USERS (Too many failed transactions)
-- 5 martadan ko‘p muvaffaqiyatsiz tranzaksiya qilgan foydalanuvchilar
-- =================================================================
CREATE OR ALTER VIEW vw_blocked_users_2 AS
SELECT 
    u.id AS user_id,
    u.name,
    COUNT(t.id) AS failed_txns,
    'Excessive failed transactions' AS block_reason
FROM dbo.users u
JOIN dbo.cards c ON u.id = c.user_id
JOIN dbo.transactions t ON c.id = t.from_card_id
WHERE t.status = 'failed'
GROUP BY u.id, u.name
HAVING COUNT(t.id) >= 5;
GO

-- =================================================================
-- 5. BLOCKED USERS (All cards have zero/negative limit)
-- Barcha kartalari limitdan mahrum bo‘lgan foydalanuvchilar
-- =================================================================
CREATE OR ALTER VIEW dbo.vw_blocked_users AS
WITH UserCardStats AS (
    SELECT
        user_id,
        COUNT(*) AS total_cards,
        SUM(CASE WHEN limit_amount <= 0 THEN 1 ELSE 0 END) AS blocked_like_cards
    FROM dbo.cards
    GROUP BY user_id
)
SELECT
    u.id AS user_id,
    u.name,
    u.email,
    u.phone_number,
    ucs.total_cards,
    ucs.blocked_like_cards,
    CASE 
        WHEN ucs.total_cards = ucs.blocked_like_cards THEN 'All cards have zero or negative limit'
        ELSE 'Partial block (not included here)' -- Faqat to‘liq bloklanganlar viewga kiritiladi
    END AS block_reason
FROM dbo.users u
JOIN UserCardStats ucs ON u.id = ucs.user_id
WHERE ucs.total_cards > 0
  AND ucs.total_cards = ucs.blocked_like_cards;
GO

-- =================================================================
-- 6. DAILY SUMMARY REPORT VIEW
-- Har kungi tranzaksiya statistikasi: umumiy soni, summasi, statuslari
-- =================================================================
CREATE OR ALTER VIEW vw_daily_summary AS
SELECT 
    CAST(t.created_at AS DATE) AS transaction_date,
    COUNT(t.id) AS total_transactions,
    SUM(t.amount) AS total_amount,
    SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) AS failed_transactions,
    SUM(CASE WHEN t.status = 'success' THEN 1 ELSE 0 END) AS completed_transactions
FROM transactions t
GROUP BY CAST(t.created_at AS DATE);
GO
