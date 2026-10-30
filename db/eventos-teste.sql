-- Camada 3: gerar um evento de cada tipo (rodar um bloco por vez)

-- INSERT  -> op = c
INSERT INTO transactions (card_id, account_id, merchant_id, amount)
SELECT card_id, account_id, 5, 89.90 FROM cards WHERE card_id = 10;

-- UPDATE  -> op = u, com before e after
UPDATE accounts SET status = 'BLOCKED' WHERE account_id = 3;

-- DELETE  -> op = d, after = null, seguido de um tombstone
DELETE FROM transactions WHERE transaction_id = 500;


-- Camada 4: disparar a regra de fraude (3 transações no cartão 7, de uma vez)
INSERT INTO transactions (card_id, account_id, merchant_id, amount)
SELECT card_id, account_id, 3, v
FROM cards, (VALUES (49.90), (120.00), (310.50)) AS x(v)
WHERE card_id = 7;

-- ~70 segundos depois: uma transação em outro cartão, para o relógio do Flink fechar a janela
INSERT INTO transactions (card_id, account_id, merchant_id, amount)
SELECT card_id, account_id, 1, 15.00 FROM cards WHERE card_id = 20;


-- Teardown (no fim do projeto)
-- SELECT pg_drop_replication_slot('desafio_slot');
-- DROP PUBLICATION desafio_pub;
