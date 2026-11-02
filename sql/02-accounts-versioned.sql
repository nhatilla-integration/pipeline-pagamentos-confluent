SET 'sql.tables.scan.idle-timeout' = '5s';

-- Versão atual de cada conta, com chave primária: base do join temporal.
-- A chave vem da coluna account_id (chave da mensagem Kafka, NOT NULL);
-- `after`.account_id não serve como PK porque `after` é nulo nos DELETEs.
CREATE TABLE `desafio.accounts.versioned` (
  PRIMARY KEY (account_id) NOT ENFORCED,
  WATERMARK FOR updated_at AS updated_at - INTERVAL '1' SECOND
) WITH ('changelog.mode' = 'upsert')
AS SELECT
  account_id,
  `after`.customer_id  AS customer_id,
  `after`.status       AS status,
  `after`.credit_limit AS credit_limit,
  `after`.risk_level   AS risk_level,
  `$rowtime`           AS updated_at
FROM `desafio.public.accounts`
WHERE op IN ('r', 'c', 'u');
