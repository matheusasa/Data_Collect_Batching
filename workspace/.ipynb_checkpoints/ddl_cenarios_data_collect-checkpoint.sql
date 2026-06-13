-- =====================================================================
-- PREPARAÇÃO DO AMBIENTE (SISTEMA OPERACIONAL DA EMPRESA)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS public;
SET search_path TO public;

-- Extensão para geração de UUIDs na aplicação
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================================
-- FUNÇÃO GENÉRICA DE ORIGEM
-- =====================================================================
-- Utilizada pelos sistemas transacionais para manter a data de alteração atualizada.
-- É o que salva a vida do Engenheiro de Dados na hora de fazer extração incremental.
CREATE OR REPLACE FUNCTION tf_atualiza_timestamp_modificacao()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_ultima_atualizacao = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =====================================================================
-- CENÁRIO 1: TABELAS RELACIONAIS PADRÃO (E-commerce)
-- Extração: Incremental baseada em data_ultima_atualizacao
-- =====================================================================

CREATE TABLE pedidos (
    id_pedido UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_cliente VARCHAR(50) NOT NULL,
    data_pedido TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) CHECK (status IN ('PENDENTE', 'PAGO', 'ENVIADO', 'CANCELADO')),
    total_pedido NUMERIC(10, 2) NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_ultima_atualizacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_pedidos_upd
    BEFORE UPDATE ON pedidos
    FOR EACH ROW EXECUTE FUNCTION tf_atualiza_timestamp_modificacao();

CREATE TABLE itens_pedido (
    id_item UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_pedido UUID NOT NULL REFERENCES pedidos(id_pedido) ON DELETE CASCADE,
    id_produto VARCHAR(50) NOT NULL,
    quantidade INT NOT NULL CHECK (quantidade > 0),
    preco_unitario NUMERIC(10, 2) NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- CENÁRIO 2: TABELA DE EVENTOS COM JSONB (Telemetria)
-- Extração: Incremental baseada em timestamp_evento (Insert-Only)
-- =====================================================================

CREATE TABLE eventos_web (
    id_evento UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp_evento TIMESTAMPTZ NOT NULL,
    tipo_evento VARCHAR(50) NOT NULL, 
    payload JSONB NOT NULL,           
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_eventos_payload ON eventos_web USING GIN (payload);

-- =====================================================================
-- CENÁRIO 3: TABELA FINANCEIRA DE ALTA PRECISÃO (Ledger)
-- Extração: Incremental baseada na data_transacao (Insert-Only)
-- =====================================================================

CREATE TABLE transacoes_financeiras (
    id_transacao UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conta_origem VARCHAR(50) NOT NULL,
    conta_destino VARCHAR(50) NOT NULL,
    tipo_movimento VARCHAR(10) CHECK (tipo_movimento IN ('CREDITO', 'DEBITO', 'ESTORNO')),
    valor NUMERIC(15, 4) NOT NULL CHECK (valor > 0), 
    moeda VARCHAR(3) DEFAULT 'BRL',
    data_transacao TIMESTAMPTZ NOT NULL,
    hash_auditoria VARCHAR(256) NOT NULL,                     
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- CENÁRIO 4: TABELA COM SOFT DELETE (Catálogo)
-- Extração: Incremental baseada em data_ultima_atualizacao.
-- O Object Storage receberá a linha com fl_excluido = TRUE e o Spark lidará com isso.
-- =====================================================================

CREATE TABLE produtos_catalogo (
    id_produto UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    codigo_sku VARCHAR(50) UNIQUE NOT NULL,
    nome_produto VARCHAR(100) NOT NULL,
    categoria VARCHAR(50),
    preco_atual NUMERIC(10, 2) NOT NULL,
    fl_excluido BOOLEAN NOT NULL DEFAULT FALSE,
    data_exclusao TIMESTAMPTZ, 
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_ultima_atualizacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_produtos_catalogo_upd
    BEFORE UPDATE ON produtos_catalogo
    FOR EACH ROW EXECUTE FUNCTION tf_atualiza_timestamp_modificacao();

-- =====================================================================
-- CENÁRIO 5: TABELA LEGADA OPAQUE (Sem controle de atualização)
-- Extração: Full Load (Carga Total) diária para o Object Storage, 
-- pois não há como o pipeline saber o que mudou.
-- =====================================================================

CREATE TABLE fornecedores_legado (
    id_fornecedor VARCHAR(50) PRIMARY KEY,
    razao_social VARCHAR(150) NOT NULL,
    cnpj VARCHAR(14) NOT NULL,
    status_credito VARCHAR(20),
    limite_credito NUMERIC(12, 2)
);
-- Nota: Como esta tabela não possui 'data_ultima_atualizacao' nem controle de 
-- soft delete, o pipeline será forçado a extrair um "Snapshot" de 100% da tabela
-- todos os dias e gravar um novo arquivo no Object Storage.