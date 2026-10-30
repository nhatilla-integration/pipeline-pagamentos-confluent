SET 'sql.tables.scan.idle-timeout' = '5s';

-- RULE_VELOCITY: 3 ou mais transações no mesmo cartão em uma janela de 60 segundos.
-- Só transações novas (op = 'c'): o snapshot inicial chega todo no mesmo segundo e geraria alertas falsos.
CREATE TABLE `desafio.fraud.detected` AS
SELECT
  `after`.card_id      AS card_id,
  window_start,
  window_end,
  COUNT(*)             AS tx_count,
  SUM(`after`.amount)  AS total_amount,
  'RULE_VELOCITY'      AS rule_name
FROM TABLE(
  TUMBLE(TABLE `desafio.public.transactions`, DESCRIPTOR(`$rowtime`), INTERVAL '60' SECONDS)
)
WHERE op = 'c'
GROUP BY `after`.card_id, window_start, window_end
HAVING COUNT(*) >= 3;
