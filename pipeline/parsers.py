# pipeline/parsers.py

import re
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator

# Configuração de Logs
logging.basicConfig(level=logging.INFO)


def traduzir_pt(texto):
    """Traduz qualquer texto para Português usando Google Translate (Free)."""
    if not texto or len(texto) < 3:
        return texto
    try:
        # source='auto' detecta se é Chinês ou Inglês automaticamente
        return GoogleTranslator(source="auto", target="pt").translate(texto)
    except Exception as e:
        logging.warning(f"Falha na tradução: {e}")
        return texto  # Retorna original se falhar


# ==========================================
# HELPERS (Funções Auxiliares)
# ==========================================

def get_scraped_date(item):
    """Tenta pegar a data de raspagem do item, ou usa Agora."""
    return item.get("scraped_at", datetime.now().isoformat())

def extrair_data_wsj(relative_str, scraped_at_str):
    """Converte '42 min ago' em datetime real baseado na hora do scrape."""
    try:
        scraped_dt = datetime.fromisoformat(scraped_at_str)

        if "min" in relative_str:
            mins = int(re.search(r"(\d+)", relative_str).group(1))
            return (scraped_dt - timedelta(minutes=mins)).isoformat()
        elif "hour" in relative_str:
            hours = int(re.search(r"(\d+)", relative_str).group(1))
            return (scraped_dt - timedelta(hours=hours)).isoformat()
        else:
            return scraped_dt.isoformat()
    except:
        return scraped_at_str


def extrair_data_chinesa(date_str):
    """Converte '2025年12月22日' para ISO format."""
    try:
        match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_str)
        if match:
            y, m, d = match.groups()
            return f"{y}-{int(m):02d}-{int(d):02d}T00:00:00"
        return datetime.now().isoformat()
    except:
        return datetime.now().isoformat()


def extrair_data_weibo(date_str):
    """Converte '12月21日 17:11' assumindo o ano atual."""
    try:
        match = re.search(r"(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{1,2})", date_str)
        if match:
            month, day, hour, minute = match.groups()
            year = datetime.now().year
            # Lógica de virada de ano (se estamos em Jan e post é de Dez, subtrai 1 ano)
            if datetime.now().month == 1 and int(month) == 12:
                year -= 1
            return f"{year}-{int(month):02d}-{int(day):02d}T{int(hour):02d}:{int(minute):02d}:00"
        return datetime.now().isoformat()
    except:
        return datetime.now().isoformat()


# ==========================================
# PARSERS ESPECÍFICOS
# ==========================================


def parse_peoples_daily(item):
    conteudo = item.get("summary", "") or item.get("title", "")
    return {
        "id_origem": item["url"],
        "plataforma": "Peoples Daily",
        "tipo_fonte": "Midia",
        "pais": "China",
        "titulo_original": item["title"],
        "titulo_pt": traduzir_pt(item["title"]),
        "conteudo_original": conteudo,
        "conteudo_pt": traduzir_pt(conteudo),
        "data_publicacao": extrair_data_chinesa(item["published_date"]),
        "data_raspagem": get_scraped_date(item),  # <--- NOVA COLUNA
        "engajamento": 0,
        "url": item["url"],
    }


def parse_wsj(item):
    conteudo = item.get("summary", "")
    return {
        "id_origem": item["url"],
        "plataforma": "WSJ",
        "tipo_fonte": "Midia",
        "pais": "USA",
        "titulo_original": item["title"],
        "titulo_pt": traduzir_pt(item["title"]),
        "conteudo_original": conteudo,
        "conteudo_pt": traduzir_pt(conteudo),
        "data_publicacao": extrair_data_wsj(item["published_date"], item.get("scraped_at", datetime.now().isoformat())),
        "data_raspagem": get_scraped_date(item), # <--- NOVA COLUNA
        "engajamento": 0,
        "url": item["url"],
    }


def parse_weibo(item):
    texto_pt = traduzir_pt(item["text"])
    return {
        "id_origem": item["mid"],
        "plataforma": "Weibo",
        "tipo_fonte": "Publico",
        "pais": "China",
        "titulo_original": None,
        "titulo_pt": None,
        "conteudo_original": item["text"],
        "conteudo_pt": texto_pt,
        "data_publicacao": extrair_data_weibo(item["timestamp"]),
        "data_raspagem": get_scraped_date(item), # <--- NOVA COLUNA
        "engajamento": item.get("likes", 0),
        "url": item["user_url"],
    }


def parse_twitter(item):
    soup = BeautifulSoup(item["raw_html"], "html.parser")
    tweet_text_div = soup.find("div", {"data-testid": "tweetText"})
    texto_limpo = (
        tweet_text_div.get_text(separator=" ") if tweet_text_div else item["text"]
    )
    time_tag = soup.find("time")
    data_iso = time_tag["datetime"] if time_tag else datetime.now().isoformat()

    likes = 0
    try:
        like_btn = soup.find("button", {"data-testid": "like"})
        if like_btn:
            aria = like_btn.get("aria-label", "")
            match = re.search(r"(\d+)", aria)
            if match:
                likes = int(match.group(1))
    except:
        pass

    # Tenta pegar scraped_at do item, se não tiver, pega agora
    scraped_at = item.get("scraped_at", datetime.now().isoformat())

    return {
        "id_origem": data_iso + str(likes),
        "plataforma": "X (Twitter)",
        "tipo_fonte": "Publico",
        "pais": "USA",
        "titulo_original": None,
        "titulo_pt": None,
        "conteudo_original": texto_limpo,
        "conteudo_pt": traduzir_pt(texto_limpo),
        "data_publicacao": data_iso,
        "data_raspagem": scraped_at, # <--- NOVA COLUNA
        "engajamento": likes,
        "url": "https://x.com",
    }
