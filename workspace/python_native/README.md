# Arquitetura de Ingestão: Uso de Cursores Nativos vs. Pandas em Pipelines Python

A escolha entre a utilização de cursores nativos (com loops de iteração) e a biblioteca Pandas para pipelines de ingestão de dados em formatos orientados a linha (como CSV, JSON Lines e Avro) baseia-se em critérios técnicos fundamentais de gerenciamento de memória e eficiência de infraestrutura.

---

## 1. Gerenciamento de Memória RAM e Escala (Streaming vs. In-Memory)

A principal limitação no uso do Pandas para processos de ingestão bruta (*Data Ingestion*) reside na forma como a biblioteca gerencia a memória:

* **Abordagem do Pandas (In-Memory):** Para criar um DataFrame, o Pandas necessita carregar a totalidade dos registros extraídos do banco de dados na memória RAM de uma única vez. Devido à estrutura interna de metadados e conversão de tipos para estruturas do NumPy, o espaço ocupado na RAM pode ser de três a quatro vezes superior ao tamanho real dos dados no banco. Em cenários de alta volumetria, esse comportamento resulta frequentemente em falhas por esgotamento de memória (*Out of Memory - OOM*).
* **Abordagem de Cursores Nativos (Streaming):** A utilização de cursores permite extrair e processar os dados de forma linear ou em lotes controlados (utilizando métodos como `fetchmany`). O consumo de memória RAM permanece baixo, previsível e constante, independentemente de a tabela de origem conter milhares ou dezenas de milhões de registros.

---

## 2. Compatibilidade Anatômica com Formatos Orientados a Linha

Os formatos utilizados nesta camada de ingestão possuem características específicas que determinam a melhor ferramenta de manipulação:

* **CSV e JSON Lines:** São formatos baseados em texto plano e estritamente orientados a linhas. A conversão de registros do banco de dados para esses formatos envolve essencialmente a concatenação de strings com delimitadores ou a serialização de dicionários simples, finalizados por uma quebra de linha (`\n`). O interpretador nativo do Python executa essas operações com velocidade e baixo custo computacional, sem o overhead exigido para construir a matriz bidimensional de um DataFrame.
* **Apache Avro:** É um formato binário autodescritivo, porém focado em armazenamento orientado a linhas (*row-based*). A classificação e persistência dos dados exigem o mapeamento sequencial de registros que correspondam a um contrato rígido (Schema JSON). Cursores nativos fornecem o fluxo perfeito de tuplas e dicionários sequenciais demandados pelas bibliotecas de serialização do Avro (como o `fastavro`), sem a necessidade de conversões intermediárias complexas.

---

## 3. Matriz de Propriedades dos Formatos na Ingestão Nativa

| Formato | Orientação Base | Tipo de Dado | Estrutura de Consumo no Loop | Tipo de Buffer Utilizado |
| :--- | :--- | :--- | :--- | :--- |
| **CSV** | Linha | Texto Plano | Tupla Simples (`linha[x]`) | `io.StringIO` |
| **JSONL** | Linha | Texto / Semi-estruturado | Dicionário Chave-Valor | `io.StringIO` |
| **Avro** | Linha | Binário / Estruturado | Dicionário + Validação de Schema | `io.BytesIO` |

---

## 4. Eficiência no Fluxo de Carga (Buffering Direto)

Pipelines eficientes de engenharia de dados evitam a escrita de arquivos intermediários no disco local do container ou servidor de execução. 

Ao integrar o cursor nativo a buffers de memória virtuais (`io.StringIO` para texto ou `io.BytesIO` para binários), os dados extraídos do banco de dados são formatados e transmitidos via protocolo HTTP diretamente para o Object Storage de destino. Esse fluxo contínuo e direto otimiza o uso de I/O (Entrada/Saída) da infraestrutura, garantindo que o script atue estritamente como um barramento de passagem rápida do dado bruto para o Data Lake.

---

## Resumo de Diretrizes Técnicas

* **Apropriado para Cursores e Loops Nativos:** Operações de extração e carga bruta (*Extract and Load*) para formatos orientados a linha (CSV, JSONL, Avro), focando no menor custo de infraestrutura e eliminação do risco de quebra por falta de memória.
* **Apropriado para Pandas / PyArrow:** Etapas subsequentes de transformação analítica (*Transform* / OLAP) ou quando há necessidade de reestruturação para formatos orientados a coluna (como o Parquet), onde o volume de dados já foi filtrado ou a infraestrutura possui capacidade de computação em memória dedicada.