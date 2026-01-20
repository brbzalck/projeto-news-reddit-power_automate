# pipeline/orchestrator.py

import json
import os
import sqlite3
import logging
import subprocess
import sys
from datetime import datetime  # Importante para a data do lote
from parsers import parse_peoples_daily, parse_wsj, parse_weibo, parse_twitter

# ========================
# Configurações de Caminho
# ========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_PATH = os.path.join(BASE_DIR, "database.db")

# Caminhos RELATIVOS dos scrapers
SCRAPER_SCRIPTS = [
    os.path.join("scrapers", "peoples_daily_scraper", "peoples_daily_scraper.py"),
    os.path.join("scrapers", "wsj_scraper", "wsj_scraper.py"),
    os.path.join("scrapers", "weibo_scraper", "weibo_scraper.py"),
    os.path.join("scrapers", "twitter_scraper", "twitter_scraper.py"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


# ========================
# 1. Execução dos Scrapers
# ========================
def run_scrapers():
    logging.info(">>> INICIANDO COLETA DE DADOS (SCRAPERS) <<<")
    python_executable = sys.executable

    for script_rel_path in SCRAPER_SCRIPTS:
        script_path = os.path.join(BASE_DIR, script_rel_path)
        work_dir = os.path.dirname(script_path)
        script_name = os.path.basename(script_path)

        logging.info(f"Executando scraper: {script_name}...")

        try:
            result = subprocess.run(
                [python_executable, script_path],
                cwd=work_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            logging.info(f"Sucesso: {script_name}")
        except subprocess.CalledProcessError as e:
            logging.error(f"ERRO ao rodar {script_name}:")
            logging.error(e.stderr)
        except Exception as e:
            logging.error(f"Erro inesperado ao chamar {script_name}: {e}")

    logging.info(">>> COLETA FINALIZADA. INICIANDO ETL... <<<")


# ========================
# 2. Banco de Dados e ETL
# ========================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sentiment_data (
            id_origem TEXT PRIMARY KEY,
            plataforma TEXT,
            tipo_fonte TEXT,
            pais TEXT,
            titulo_original TEXT,
            titulo_pt TEXT,
            conteudo_original TEXT,
            conteudo_pt TEXT,
            data_publicacao TEXT,
            data_raspagem TEXT,   -- A coluna unificada
            engajamento INTEGER,
            ordem_coleta INTEGER,
            url TEXT
        )
    """
    )
    conn.commit()
    return conn


def process_file(filename, parser_func, conn, batch_date):
    """
    Agora recebe 'batch_date' como argumento para garantir uniformidade.
    """
    filepath = os.path.join(OUTPUT_DIR, filename)

    if not os.path.exists(filepath):
        logging.warning(f"Arquivo não encontrado: {filename}")
        return

    logging.info(f"Processando: {filename} com data base {batch_date}...")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logging.error(f"Erro ao ler JSON: {filename}.")
        return

    c = conn.cursor()
    count = 0

    for index, item in enumerate(data):
        try:
            parsed = parser_func(item)

            # --- SOBRESCREVENDO COM A DATA DO LOTE ---
            # Ignora o que o parser calculou e força a data única da execução
            parsed["data_raspagem"] = batch_date

            parsed["ordem_coleta"] = index

            c.execute(
                """
                INSERT INTO sentiment_data VALUES (
                    :id_origem, :plataforma, :tipo_fonte, :pais,
                    :titulo_original, :titulo_pt,
                    :conteudo_original, :conteudo_pt,
                    :data_publicacao,
                    :data_raspagem,
                    :engajamento,
                    :ordem_coleta,
                    :url
                )
                ON CONFLICT(id_origem) DO UPDATE SET
                engajamento=excluded.engajamento,
                conteudo_pt=excluded.conteudo_pt,
                titulo_pt=excluded.titulo_pt,
                ordem_coleta=excluded.ordem_coleta,
                data_raspagem=excluded.data_raspagem -- Atualiza para a data do novo lote
            """,
                parsed,
            )
            count += 1
        except Exception as e:
            logging.error(f"Erro ao salvar item no DB ({filename}): {e}")

    conn.commit()
    logging.info(f"Salvos/Atualizados {count} itens de {filename}.")


# ========================
# 3. Fluxo Principal
# ========================
def main():
    # 1. Define a DATA DO LOTE (Batch Date)
    # Formato simples YYYY-MM-DD. Isso garante que todos tenham a mesma data.
    current_batch_date = datetime.now().strftime("%Y-%m-%d")
    logging.info(f">>> DATA DO LOTE DEFINIDA: {current_batch_date} <<<")

    # 2. Roda Scrapers
    run_scrapers()

    # 3. Inicializa Banco
    conn = init_db()

    # 4. Processa Arquivos (Passando a data do lote)
    process_file(
        "peoples_daily_raw.json", parse_peoples_daily, conn, current_batch_date
    )
    process_file("wsj_raw.json", parse_wsj, conn, current_batch_date)
    process_file("weibo_raw.json", parse_weibo, conn, current_batch_date)
    process_file("twitter_raw.json", parse_twitter, conn, current_batch_date)

    conn.close()
    logging.info(">>> FLUXO COMPLETO FINALIZADO COM SUCESSO <<<")


if __name__ == "__main__":
    main()
