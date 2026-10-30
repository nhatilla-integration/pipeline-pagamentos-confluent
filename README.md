# Pipeline de pagamentos em tempo real — Confluent Cloud

Pipeline de ponta a ponta: **Postgres (Neon) → CDC (Debezium) → Kafka → Flink SQL → alerta de fraude**.
Desafio de projeto do bootcamp IBM Confluent — Dados em tempo real para agentes de IA (DIO).

## Arquitetura

<sua evidência aqui: diagrama ou print do Stream Lineage>

## Como subir do zero

Pré-requisitos: conta no Confluent Cloud, conta no Neon, Confluent CLI, Git Bash e `envsubst`.

1. `cp .env.example .env` e preencha os valores conforme as camadas abaixo.
2. Siga as camadas na ordem.

## Camada 1 — Fundação

**O que faz:** environment `desafio-final`, cluster Basic `desafio-basic` (aws/us-east-1) e duas service accounts com menor privilégio.

**Por quê:** <sua explicação aqui>

**Evidência:**

```text
<sua evidência aqui: saída de evidencias/01 a 03>
```

## Camada 2 — Contrato

**O que faz:** schema Avro `com.bootcamp.payments.Transaction` no Schema Registry, com valor em `decimal(15,2)`, e teste de compatibilidade BACKWARD.

**Por quê:** <sua explicação aqui>

**Evidência:**

```text
<sua evidência aqui: evidencias/04 (compatível) e 05 (incompatível)>
```

## Camada 3 — Ingestão (CDC)

**O que faz:** conector `PostgresCdcSourceV2` lendo o WAL do Postgres pelo slot `desafio_slot`.

**Por quê CDC e não batch:** <sua explicação aqui>

**Evidência (eventos c, u e d):**

```json
<sua evidência aqui: evidencias/07-envelopes-cdc.json>
```

## Camada 4 — Processamento (Flink SQL)

**O que faz:** join temporal das transações com a versão da conta no momento da transação, e regra `RULE_VELOCITY` (3+ transações no mesmo cartão em 60 s).

**Por que essa janela:** <sua explicação aqui, incluindo a limitação da janela tumbling>

**Evidência:**

<sua evidência aqui: evidencias/08 e 09>

## Consumo

**Evidência:**

```text
<sua evidência aqui: evidencias/10 e 11>
```

## Camada 5 — Operação

### Observabilidade

<sua evidência aqui: evidencias/12 a 14>

### Segurança

| Identidade | Permissões | Por quê |
| --- | --- | --- |
| desafio-producer | create/write/describe em tópicos `desafio*`; DeveloperWrite em subjects `desafio*` | Usada só pelo conector CDC |
| desafio-consumer | read em tópicos e grupos `desafio*`; DeveloperRead em subjects `desafio*` | Quem consome os alertas não pode escrever |
| conta pessoal | Admin | Só para criar e derrubar o ambiente |

### Custo

```text
<sua evidência aqui: evidencias/15-custo.txt>
```

Duas formas de gastar menos: <sua explicação aqui>

### Confiabilidade

| Trecho | Garantia | Ponto de atenção |
| --- | --- | --- |
| Postgres → Kafka (CDC) | At-least-once; o slot guarda a posição | Slot esquecido lota o disco do banco |
| Flink | Exactly-once interno com checkpoints | Janela tumbling não pega padrão que cruza a borda |
| Kafka → consumidor | At-least-once | Elo fraco: a deduplicação é código próprio |

## Como derrubar tudo

<sua evidência aqui: evidencias/16-teardown.txt>

## Quanto custou

US$ <valor> no período de <data> a <data>.
