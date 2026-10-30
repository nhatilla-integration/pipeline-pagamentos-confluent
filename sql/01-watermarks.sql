-- Com pouco tráfego, o watermark padrão demora a avançar; aqui ele fica 1 s atrás do evento mais recente.
ALTER TABLE `desafio.public.transactions` MODIFY WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '1' SECOND;
ALTER TABLE `desafio.public.accounts`     MODIFY WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '1' SECOND;
