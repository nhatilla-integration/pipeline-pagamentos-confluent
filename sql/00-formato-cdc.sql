-- Rodar no SQL workspace do Flink (catálogo: default · database: Nati_32)
--
-- Por padrão o Confluent Cloud lê tópicos do Debezium como 'avro-debezium-registry':
-- desmonta o envelope e trata o tópico como changelog ('retract').
-- Janelas de tempo (TUMBLE) exigem um fluxo só de inserções, então lemos o envelope cru
-- (before / after / op / source) como eventos simples (append) e filtramos por op.

SHOW CREATE TABLE `desafio.public.transactions`;

ALTER TABLE `desafio.public.transactions` SET ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
ALTER TABLE `desafio.public.accounts`     SET ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
