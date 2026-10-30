-- Rodar primeiro no SQL workspace do Flink (catálogo desafio-final, banco desafio-basic)
SHOW TABLES;
SHOW CREATE TABLE `desafio.public.transactions`;

-- As queries seguintes leem o envelope do CDC (colunas before, after, op).
-- Se o SHOW CREATE TABLE mostrar 'value.format' = 'avro-debezium-registry', rode:
-- ALTER TABLE `desafio.public.transactions` SET ('value.format' = 'avro-registry');
-- ALTER TABLE `desafio.public.accounts`     SET ('value.format' = 'avro-registry');
