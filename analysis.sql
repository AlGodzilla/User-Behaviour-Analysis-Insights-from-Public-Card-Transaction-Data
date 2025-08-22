-- SQL Analysis Queries for User Behaviour Dataset

-- Query 1: Number of cards by type and brand
SELECT card_brand, card_type, COUNT(*) AS card_count
FROM cards
GROUP BY card_brand, card_type
ORDER BY card_count DESC;

-- Query 2: Top 10 merchant categories by number of transactions
SELECT mcc,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount
FROM transactions
GROUP BY mcc
ORDER BY txn_count DESC, total_amount DESC
LIMIT 10;

-- Query 3: Transaction volume per month
SELECT strftime('%Y-%m', date) AS month,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount
FROM transactions
GROUP BY month
ORDER BY month;

-- Query 4: Number of users by age group (10‑year buckets)
SELECT (current_age/10)*10 AS age_group,
       COUNT(*) AS num_users
FROM users
GROUP BY age_group
ORDER BY age_group;

-- Query 5: Average transaction amount by credit score group (100‑point buckets)
SELECT (u.credit_score/100)*100 AS credit_score_group,
       AVG(t.amount) AS avg_transaction_amount,
       COUNT(*) AS num_transactions
FROM transactions t
JOIN users u ON t.client_id = u.id
GROUP BY credit_score_group
ORDER BY credit_score_group;

-- Query 6: Average number of credit cards per user
SELECT AVG(num_credit_cards) AS avg_credit_cards
FROM users;

-- Query 7: Percentage of cards flagged on the dark web
SELECT SUM(CASE WHEN card_on_dark_web='Yes' THEN 1 ELSE 0 END)*100.0/COUNT(*) AS pct_dark_web
FROM cards;

-- Query 8: Number of cards held vs. average total debt per user
SELECT num_credit_cards,
       AVG(CAST(REPLACE(REPLACE(total_debt, '$',''),',','') AS REAL)) AS avg_total_debt,
       COUNT(*) AS num_users
FROM users
GROUP BY num_credit_cards
ORDER BY num_credit_cards;

-- Query 9: Average credit limit by card brand and type
SELECT card_brand,
       card_type,
       AVG(CAST(REPLACE(REPLACE(credit_limit, '$',''),',','') AS REAL)) AS avg_credit_limit,
       COUNT(*) AS num_cards
FROM cards
GROUP BY card_brand, card_type
ORDER BY avg_credit_limit DESC;

-- Query 10: Per capita income bracket vs. average credit score
SELECT ROUND(CAST(REPLACE(REPLACE(per_capita_income, '$',''),',','') AS REAL)/10000)*10000 AS income_bracket,
       AVG(credit_score) AS avg_credit_score,
       COUNT(*) AS num_users
FROM users
GROUP BY income_bracket
ORDER BY income_bracket;