-- Dados sintéticos determinísticos (seed 42)
-- Rodar no SQL Editor do Neon, depois de ativar a replicação lógica e criar o cdc_user pela Console.

SELECT setseed(0.42);

CREATE TABLE customers (
  customer_id SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  email       TEXT NOT NULL,
  city        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE accounts (
  account_id   SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL REFERENCES customers,
  status       TEXT NOT NULL,
  credit_limit NUMERIC(15,2) NOT NULL,
  risk_level   TEXT NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE cards (
  card_id    SERIAL PRIMARY KEY,
  account_id INT NOT NULL REFERENCES accounts,
  brand      TEXT NOT NULL,
  last4      CHAR(4) NOT NULL
);
CREATE TABLE merchants (
  merchant_id SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  category    TEXT NOT NULL,
  city        TEXT NOT NULL
);
CREATE TABLE transactions (
  transaction_id BIGSERIAL PRIMARY KEY,
  card_id        INT NOT NULL REFERENCES cards,
  account_id     INT NOT NULL REFERENCES accounts,
  merchant_id    INT NOT NULL REFERENCES merchants,
  amount         NUMERIC(15,2) NOT NULL,
  currency       CHAR(3) NOT NULL DEFAULT 'BRL',
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO customers (name, email, city)
SELECT 'Cliente ' || g, 'cliente' || g || '@exemplo.com',
       (ARRAY['Campinas','São Paulo','Rio de Janeiro','Belo Horizonte','Curitiba'])[1 + floor(random()*5)::int]
FROM generate_series(1, 50) g;

INSERT INTO accounts (customer_id, status, credit_limit, risk_level)
SELECT customer_id, 'ACTIVE', round((1000 + random()*19000)::numeric, 2),
       (ARRAY['LOW','MEDIUM','HIGH'])[1 + floor(random()*3)::int]
FROM customers;

INSERT INTO cards (account_id, brand, last4)
SELECT account_id, (ARRAY['VISA','MASTERCARD','ELO'])[1 + floor(random()*3)::int],
       lpad(floor(random()*10000)::int::text, 4, '0')
FROM accounts, generate_series(1, 2);

INSERT INTO merchants (name, category, city)
SELECT 'Loja ' || g, (ARRAY['MERCADO','ELETRONICOS','RESTAURANTE','POSTO','FARMACIA'])[1 + floor(random()*5)::int], 'Campinas'
FROM generate_series(1, 20) g;

INSERT INTO transactions (card_id, account_id, merchant_id, amount)
SELECT c.card_id, c.account_id, r.merchant_id, r.amount
FROM (SELECT 1 + floor(random()*100)::int AS card_id,
             1 + floor(random()*20)::int  AS merchant_id,
             round((5 + random()*495)::numeric, 2) AS amount
      FROM generate_series(1, 500)) r
JOIN cards c ON c.card_id = r.card_id;

-- UPDATE e DELETE chegam com o estado anterior completo
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE cards        REPLICA IDENTITY FULL;
ALTER TABLE merchants    REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- quais tabelas o CDC vai ler
CREATE PUBLICATION desafio_pub FOR TABLE customers, accounts, cards, merchants, transactions;

-- o cdc_user só lê
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
