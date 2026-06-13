import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
import random
import json
from datetime import datetime, timedelta

# Configura o Faker para dados no padrão brasileiro
fake = Faker('pt_BR')

# =====================================================================
# CONFIGURAÇÃO DE CONEXÃO E VOLUMETRIA
# =====================================================================
DB_CONFIG = {
    "dbname": "postgres", 
    "user": "postgres",             
    "password": "tailwind2026",        
    "host": "localhost",
    "port": "5442"
}

# Variáveis para você controlar o tamanho da massa de dados
VOL_PEDIDOS = 10000       # Vai gerar ~30.000 itens
VOL_EVENTOS = 50000       # Volume pesado por causa do JSONB
VOL_LEDGER = 20000
VOL_PRODUTOS = 2000
VOL_FORNECEDORES = 5000

def conectar_banco():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        exit(1)

# =====================================================================
# FUNÇÕES DE GERAÇÃO E INGESTÃO DE DADOS
# =====================================================================

def popular_pedidos_e_itens(cursor):
    print(f"-> Gerando {VOL_PEDIDOS} Pedidos e seus respectivos Itens (com datas retroativas)...")
    pedidos = []
    itens = []
    
    for _ in range(VOL_PEDIDOS):
        id_pedido = fake.uuid4()
        id_cliente = f"CLI-{random.randint(1000, 9999)}"
        
        # Gera uma data histórica (de 6 meses atrás até ontem)
        data_historica = fake.date_time_between(start_date="-180d", end_date="-1d")
        status = random.choices(['PENDENTE', 'PAGO', 'ENVIADO', 'CANCELADO'], weights=[10, 60, 20, 10])[0]
        
        num_itens = random.randint(1, 5)
        total_pedido = 0
        
        for _ in range(num_itens):
            id_produto = f"SKU-{random.randint(100, 999)}"
            qtd = random.randint(1, 3)
            preco = round(random.uniform(20.0, 500.0), 2)
            total_pedido += (qtd * preco)
            
            # O item nasce na mesma data do pedido
            itens.append((fake.uuid4(), id_pedido, id_produto, qtd, preco, data_historica))
        
        # O pedido recebe a data_historica tanto na data da compra, quanto na criacao e ultima atualizacao
        pedidos.append((id_pedido, id_cliente, data_historica, status, total_pedido, data_historica, data_historica))

    execute_batch(cursor, 
        """INSERT INTO pedidos 
           (id_pedido, id_cliente, data_pedido, status, total_pedido, data_criacao, data_ultima_atualizacao) 
           VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
        pedidos)
    
    execute_batch(cursor, 
        """INSERT INTO itens_pedido 
           (id_item, id_pedido, id_produto, quantidade, preco_unitario, data_criacao) 
           VALUES (%s, %s, %s, %s, %s, %s)""", 
        itens)

def popular_eventos_web(cursor):
    print(f"-> Gerando {VOL_EVENTOS} Eventos Web (JSONB)... isso pode levar alguns segundos...")
    eventos = []
    
    for _ in range(VOL_EVENTOS):
        tipo_evento = random.choice(['page_view', 'add_to_cart', 'checkout', 'login_failed'])
        payload = {
            "ip": fake.ipv4(),
            "user_agent": fake.user_agent(),
            "device": random.choice(["mobile", "desktop", "tablet"])
        }
        
        if tipo_evento == 'page_view':
            payload['url'] = fake.uri()
        elif tipo_evento == 'add_to_cart':
            payload['product_id'] = f"SKU-{random.randint(100, 999)}"
            if random.random() > 0.3:
                payload['utm_source'] = random.choice(['google', 'facebook', 'organic'])
                
        # Data do evento histórica
        data_evento = fake.date_time_between(start_date="-30d", end_date="-1d")
        
        eventos.append((data_evento, tipo_evento, json.dumps(payload), data_evento))
        
    execute_batch(cursor, 
        """INSERT INTO eventos_web 
           (timestamp_evento, tipo_evento, payload, data_criacao) 
           VALUES (%s, %s, %s, %s)""", 
        eventos, page_size=500) # page_size otimiza a inserção de grandes volumes

def popular_ledger_financeiro(cursor):
    print(f"-> Gerando {VOL_LEDGER} linhas de Ledger Financeiro...")
    transacoes = []
    
    for _ in range(VOL_LEDGER):
        conta_orig = fake.bban()
        conta_dest = fake.bban()
        tipo = random.choice(['CREDITO', 'DEBITO'])
        valor = round(random.uniform(10.50, 15000.75), 4) 
        hash_auditoria = fake.sha256()
        
        # Data histórica da transação
        data_transacao = fake.date_time_between(start_date="-60d", end_date="-1d")
        
        transacoes.append((conta_orig, conta_dest, tipo, valor, data_transacao, hash_auditoria, data_transacao))
        
    execute_batch(cursor, 
        """INSERT INTO transacoes_financeiras 
           (conta_origem, conta_destino, tipo_movimento, valor, data_transacao, hash_auditoria, data_criacao) 
           VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
        transacoes, page_size=500)

def popular_e_simular_soft_delete(cursor):
    print(f"-> Gerando {VOL_PRODUTOS} Produtos no Catálogo...")
    produtos = []
    skus_gerados = []
    
    for i in range(VOL_PRODUTOS):
        sku = f"PROD-{10000+i}"
        skus_gerados.append(sku)
        
        # Produto cadastrado há mais de um ano
        data_cadastro = fake.date_time_between(start_date="-2y", end_date="-1y")
        
        produtos.append((
            sku, 
            fake.catch_phrase(), 
            random.choice(['Eletrônicos', 'Móveis', 'Roupas', 'Livros']), 
            round(random.uniform(50.0, 3000.0), 2),
            data_cadastro,
            data_cadastro
        ))
        
    execute_batch(cursor, 
        """INSERT INTO produtos_catalogo 
           (codigo_sku, nome_produto, categoria, preco_atual, data_criacao, data_ultima_atualizacao) 
           VALUES (%s, %s, %s, %s, %s, %s)""", 
        produtos, page_size=500)
        
    print("   -> Simulando Updates recentes e Soft Deletes para ativar os Triggers...")
    
    # Simula que 5% dos produtos sofreram reajuste de preço hoje (isso aciona o trigger de update no banco)
    skus_para_update = random.sample(skus_gerados, int(VOL_PRODUTOS * 0.05))
    for sku in skus_para_update:
        cursor.execute("UPDATE produtos_catalogo SET preco_atual = preco_atual * 1.1 WHERE codigo_sku = %s", (sku,))

    # Simula que 2% dos produtos saíram de linha hoje (Soft Delete)
    skus_para_delete = random.sample(skus_gerados, int(VOL_PRODUTOS * 0.02))
    for sku in skus_para_delete:
        cursor.execute("UPDATE produtos_catalogo SET fl_excluido = TRUE, data_exclusao = CURRENT_TIMESTAMP WHERE codigo_sku = %s", (sku,))

def popular_fornecedores_legado(cursor):
    print(f"-> Gerando {VOL_FORNECEDORES} Fornecedores Opaque...")
    fornecedores = []
    
    for _ in range(VOL_FORNECEDORES):
        # Limpeza da máscara do CNPJ
        cnpj_limpo = fake.cnpj().replace('.', '').replace('/', '').replace('-', '')
        
        fornecedores.append((
            fake.uuid4()[:8], 
            fake.company(), 
            cnpj_limpo, 
            random.choice(['APROVADO', 'BLOQUEADO', 'EM_ANALISE']), 
            round(random.uniform(1000, 50000), 2)
        ))
        
    execute_batch(cursor, 
        """INSERT INTO fornecedores_legado 
           (id_fornecedor, razao_social, cnpj, status_credito, limite_credito) 
           VALUES (%s, %s, %s, %s, %s)""", 
        fornecedores, page_size=500)

# =====================================================================
# EXECUÇÃO PRINCIPAL
# =====================================================================
if __name__ == "__main__":
    print("Iniciando geração escalonada de dados...")
    conn = conectar_banco()
    cur = conn.cursor()
    
    try:
        popular_pedidos_e_itens(cur)
        popular_eventos_web(cur)
        popular_ledger_financeiro(cur)
        popular_e_simular_soft_delete(cur)
        popular_fornecedores_legado(cur)
        
        # Confirma as transações
        conn.commit()
        print("\nDados gerados com sucesso! O banco está pronto e populado para a aula de ingestão.")
        
    except Exception as e:
        conn.rollback()
        print(f"\nErro durante a geração dos dados: {e}")
    finally:
        cur.close()
        conn.close()