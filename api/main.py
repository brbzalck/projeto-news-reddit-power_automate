# api/main.py

import sqlite3
import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ==========================================
# 1. Configurações e Instância da App
# ==========================================

app = FastAPI(
    title="Global AI Sentiment API",
    description="API Corporativa para análise de sentimento sobre IA (China vs EUA).",
    version="1.0.0",
)

# Permite conexões de qualquer lugar (Importante para evitar erros locais)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Caminho do Banco de Dados (saindo da pasta api/ para a raiz)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "database.db")

# ==========================================
# 2. Modelos de Dados (Schema)
# ==========================================
# Isso define o "contrato" da API. O Power BI vai receber dados assim.


class SentimentItem(BaseModel):
    id_origem: str
    plataforma: str
    tipo_fonte: str
    pais: str
    titulo_original: Optional[str]  # Renomeado
    titulo_pt: Optional[str]  # Novo
    conteudo_pt: Optional[str]
    data_publicacao: str
    data_raspagem: str
    engajamento: int
    ordem_coleta: int
    url: Optional[str]


# ==========================================
# 3. Helpers de Banco de Dados
# ==========================================


def get_db_connection():
    if not os.path.exists(DB_PATH):
        raise HTTPException(
            status_code=500,
            detail="Banco de dados não encontrado. Execute o ETL primeiro.",
        )

    conn = sqlite3.connect(DB_PATH)
    # Isso permite acessar colunas pelo nome (ex: row['titulo'])
    conn.row_factory = sqlite3.Row
    return conn


# ==========================================
# 4. Endpoints (Rotas)
# ==========================================


@app.get("/", tags=["Health"])
def read_root():
    """Verifica se a API está online."""
    return {"status": "online", "system": "AI Sentiment Analysis Core"}


@app.get("/v1/dados", response_model=List[SentimentItem], tags=["Analytics"])
def get_all_data(pais: Optional[str] = None, plataforma: Optional[str] = None):
    """
    Retorna os dados processados para consumo no Power BI.
    Permite filtragem opcional por País ou Plataforma via URL.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM sentiment_data WHERE 1=1"
    params = []

    # Aplica filtros dinâmicos se o usuário passar na URL
    if pais:
        query += " AND pais = ?"
        params.append(pais)

    if plataforma:
        query += " AND plataforma = ?"
        params.append(plataforma)

    # Ordena por data mais recente
    query += " ORDER BY data_publicacao DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    # Converte linhas do SQLite para dicionários Python
    results = [dict(row) for row in rows]
    return results
