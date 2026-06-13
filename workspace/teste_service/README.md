# Validação de Infraestrutura: Testes de Conectividade Local

Este diretório contém componentes minimalistas estruturados em formato de Notebooks Jupyter (`.ipynb`) projetados com o objetivo exclusivo de auditar, testar e validar a disponibilidade física e a integridade de acesso aos serviços de infraestrutura que compõem o ecossistema local de engenharia de dados.

A execução bem-sucedida destes scripts é considerada um pré-requisito obrigatório antes do disparo de qualquer pipeline avançado de extração, ingestão ou transformação analítica.

---

## 1. Estrutura de Arquivos

* **`01_teste_postgres.ipynb`**: Valida a conectividade da porta TCP do banco de dados transacional PostgreSQL, confirmando a autenticação e extraindo metadados básicos de versão do servidor.
* **`02_teste_minio.ipynb`**: Valida o acesso HTTP à API do Object Storage MinIO (compatível com o protocolo AWS S3), auditando as credenciais de acesso e listando os buckets mapeados no Data Lake.
* **`03_teste_spark.ipynb`**: Inicializa a `SparkSession` local para validar a subida do ambiente Java Virtual Machine (JVM) e o correto processamento de dados em memória através do motor distribuído do Apache Spark.

---

## 2. Decisões de Arquitetura e Boas Práticas

### Isolamento de Escopo
Cada serviço possui um arquivo de teste estritamente isolado. Esta separação impede falhas em cascata, garantindo que um eventual problema em um serviço específico (como um container temporariamente offline ou bloqueio de porta de rede) possa ser diagnosticado de forma individualizada, sem interferência ou dependência mútua entre as validações.

### Tratamento Interceptado de Exceções (`try...except`)
A inclusão de blocos de controle de exceção nos scripts de testes de conectividade não possui finalidade de tratamento de regras de negócio, mas atua como um **painel de diagnóstico limpo**. 

Caso um serviço esteja inacessível, o interpretador intercepta o erro de rede de baixo nível (como *Connection Refused*, falhas de autenticação ou estouros de *Timeout*) e exibe uma mensagem direta e legível em formato de log. Essa abordagem elimina a exibição do *Traceback* padrão do Python — que polui a tela do ambiente interativo com dezenas de linhas de código interno das bibliotecas (`psycopg2` ou `boto3`) —, acelerando a identificação da causa raiz do problema de infraestrutura.

### Desalocação Explícita de Recursos
Todos os scripts implementam rotinas rigorosas de encerramento de sessão e conexão ao final da execução (`conn.close()` para PostgreSQL e `spark.stop()` para Apache Spark). Essa prática assegura que os scripts de teste liberem imediatamente os *sockets* de rede abertos no servidor transacional e limpem o bloco de memória alocado na JVM do cluster, evitando o acúmulo de conexões ociosas ou desperdício de memória RAM no ambiente de desenvolvimento.

---

## 3. Diretrizes de Execução

1. Certifique-se de que a orquestração de containers da infraestrutura local esteja em execução ativa.
2. Abra os arquivos sequencialmente (`01`, `02` e `03`).
3. Execute as células de cima para baixo de forma linear utilizando o comando `Shift + Enter`.
4. Caso todas as etapas retornem mensagens de sucesso, a infraestrutura estará homologada e liberada para o recebimento de cargas de dados.