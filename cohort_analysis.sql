create database ai_retention;
use ai_retention;

create table transactions;

select * from campaigns;
select * from support_tickets;
select * from transactions;
select * from users;
select * from user_activity;

select *  from transactions;

-- Total Users
SELECT COUNT(DISTINCT user_id) AS total_users
FROM users;

-- 2️⃣ Active Users (Last 30 Days)
SELECT COUNT(DISTINCT user_id) AS active_users_30d
FROM transactions
WHERE date >= CURRENT_DATE - INTERVAL 90 day;

SELECT 
  COUNT(DISTINCT t.user_id) * 100.0 / COUNT(DISTINCT u.user_id) AS active_user_pct
FROM users u
LEFT JOIN transactions t
  ON u.user_id = t.user_id
 AND t.date >= CURRENT_DATE - INTERVAL 90 day;

-- (No txn in 90+ days)
WITH last_txn AS (
  SELECT user_id, MAX(date) AS last_txn_date
  FROM transactions
  GROUP BY user_id
)
SELECT COUNT(*) AS churned_users
FROM last_txn
WHERE last_txn_date < CURRENT_DATE - INTERVAL 90 day;

-- Churn Rate:
SELECT 
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users) AS churn_rate_pct
FROM (
  SELECT user_id
  FROM transactions
  GROUP BY user_id
  HAVING MAX(date) < CURRENT_DATE - INTERVAL 90 day)x;
 
-- Inactive Buckets (30–59 / 60–89 / 90+ Days)
WITH last_txn AS (
  SELECT user_id, MAX(date) AS last_txn_date
  FROM transactions
  GROUP BY user_id
)
SELECT
  CASE
    WHEN last_txn_date >= CURRENT_DATE - INTERVAL 30 day THEN 'Active'
    WHEN last_txn_date >= CURRENT_DATE - INTERVAL 60 day THEN '30-59 Days'
    WHEN last_txn_date >= CURRENT_DATE - INTERVAL 90 day THEN '60-89 Days'
    ELSE '90+ Days'
  END AS inactivity_bucket,
  COUNT(*) AS users
FROM last_txn
GROUP BY inactivity_bucket
ORDER BY users DESC;

-- 5️⃣ Monthly Retention (Cohort Base)
CREATE TABLE monthly_user_activity AS
SELECT
  user_id,
  DATE_FORMAT(date, '%Y-%m-01') AS month
FROM transactions
GROUP BY user_id, DATE_FORMAT(date, '%Y-%m-01');

ALTER TABLE monthly_user_activity
MODIFY user_id VARCHAR(20);

CREATE INDEX idx_user_month
ON monthly_user_activity (user_id, month);

SELECT
  m1.month AS cohort_month,
  COUNT(DISTINCT m1.user_id) AS users_month_1,
  COUNT(DISTINCT m2.user_id) AS users_month_2,
  ROUND(
    COUNT(DISTINCT m2.user_id) * 100.0 / COUNT(DISTINCT m1.user_id),
    2
  ) AS retention_pct
FROM monthly_user_activity m1
LEFT JOIN monthly_user_activity m2
  ON m1.user_id = m2.user_id
 AND m2.month = DATE_ADD(m1.month, INTERVAL 1 MONTH)
GROUP BY m1.month
ORDER BY m1.month;


-- 6️⃣ Revenue Metrics (Sanity Check)
SELECT
  DATE_FORMAT(date, '%Y-%m-01') AS month,
  SUM(amount) AS monthly_revenue
FROM transactions
GROUP BY month
ORDER BY month;

-- Revenue per Active User:
WITH active_users AS (
  SELECT DISTINCT user_id
  FROM transactions
  WHERE date >= CURRENT_DATE - INTERVAL 90 day
)
SELECT 
  SUM(t.amount) / COUNT(DISTINCT a.user_id) AS revenue_per_active_user
FROM transactions t
JOIN active_users a ON t.user_id = a.user_id;

-- 7️⃣ Failed Transactions vs Churn (Driver Validation)

WITH churned_users AS (
  SELECT user_id
  FROM transactions
  GROUP BY user_id
  HAVING MAX(date) < CURRENT_DATE - INTERVAL 90 day
)
SELECT
  CASE 
    WHEN ua.failed_txns >= 3 THEN 'High Failed Txns'
    ELSE 'Low Failed Txns'
  END AS failure_group,
  COUNT(*) AS users
FROM user_activity ua
JOIN churned_users c ON ua.user_id = c.user_id
GROUP BY failure_group;

-- 8️⃣ Unresolved Tickets Impact
SELECT
  resolved,
  COUNT(DISTINCT user_id) AS users
FROM support_tickets
GROUP BY resolved;

-- Now connect it to churn:
WITH churned_users AS (
  SELECT user_id
  FROM transactions
  GROUP BY user_id
  HAVING MAX(date) < CURRENT_DATE - INTERVAL 90 day
)
SELECT
  st.resolved,
  COUNT(DISTINCT st.user_id) AS churned_users
FROM support_tickets st
JOIN churned_users c ON st.user_id = c.user_id
GROUP BY st.resolved;

-- 9️⃣ Campaign Engagement vs Activity
SELECT
  campaign_type,
  COUNT(*) AS total_users,
  SUM(CASE WHEN engaged = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS engagement_pct
FROM campaigns
GROUP BY campaign_type;



