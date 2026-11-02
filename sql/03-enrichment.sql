SET 'sql.tables.scan.idle-timeout' = '5s';

-- Cada transação nova recebe status, risco e limite da conta no momento da transação (join temporal)
CREATE TABLE `desafio.transactions.enriched` AS
SELECT
  t.transaction_id,
  t.`after`.card_id     AS card_id,
  t.`after`.account_id  AS account_id,
  t.`after`.merchant_id AS merchant_id,
  t.`after`.amount      AS amount,
  a.status              AS account_status,
  a.risk_level          AS risk_level,
  a.credit_limit        AS credit_limit,
  t.`$rowtime`          AS event_time
FROM `desafio.public.transactions` AS t
JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.`after`.account_id = a.account_id
WHERE t.op = 'c';
