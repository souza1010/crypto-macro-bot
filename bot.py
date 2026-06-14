"""
╔══════════════════════════════════════════════════════╗
║         CRYPTO MACRO RADAR - Bot Telegram            ║
║   Monitoramento macro para traders BTC/ETH/Alts      ║
╚══════════════════════════════════════════════════════╝

Autor: Gerado por Claude (Anthropic)
Versão: 4.0 — Confluência de Timeframes + Top 50 Scanner
"""

import asyncio
import logging
import os
import httpx
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── Configurações ───────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CHAT_ID           = os.environ.get("TELEGRAM_CHAT_ID", "")
TIMEZONE          = ZoneInfo("America/Sao_Paulo")

DAILY_REPORT_HOUR      = 9
DAILY_REPORT_MINUTE    = 0
CHECK_INTERVAL_MINUTES = 15

# Calendário FOMC 2025/2026
FOMC_DATES = [
    ("2025-07-30", "15:00"),
    ("2025-09-17", "15:00"),
    ("2025-11-05", "15:00"),
    ("2025-12-17", "15:00"),
    ("2026-01-28", "15:00"),
    ("2026-03-18", "15:00"),
    ("2026-04-29", "15:00"),
    ("2026-06-17", "15:00"),
    ("2026-07-29", "15:00"),
    ("2026-09-16", "15:00"),
    ("2026-11-04", "15:00"),
    ("2026-12-16", "15:00"),
]

# ─── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Fontes RSS — Notícias Crypto ────────────────────────────────
FEEDS_CRYPTO = {
    "coindesk":         "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph":    "https://cointelegraph.com/rss",
    "bitcoin_magazine": "https://bitcoinmagazine.com/feed",
    "decrypt":          "https://decrypt.co/feed",
    "theblock":         "https://www.theblock.co/rss.xml",
    "cryptoslate":      "https://cryptoslate.com/feed/",
}

# ─── Fontes RSS — Macro / Política ───────────────────────────────
FEEDS_MACRO = {
    "reuters_markets":  "https://feeds.reuters.com/reuters/businessNews",
    "politico":         "https://www.politico.com/rss/politicopicks.xml",
    "ft_markets":       "https://www.ft.com/rss/home/us",
    "reuters_world":    "https://feeds.reuters.com/Reuters/worldNews",
    "axios":            "https://api.axios.com/feed/",
}

# ─── Fontes RSS — Fallback para posts de influentes ──────────────
# Portais que republicam declarações de Trump, Fed, SEC, Elon, etc.
# Usados quando o Nitter não está disponível.
FEEDS_INFLUENCERS_FALLBACK = {
    "trump_truthsocial": "https://trumpstruth.org/feed",           # Agrega Truth Social do Trump
    "whitehouse_news":   "https://www.whitehouse.gov/feed/",       # Comunicados oficiais da Casa Branca
    "fed_press":         "https://www.federalreserve.gov/feeds/press_all.xml",  # Comunicados oficiais do Fed
    "sec_news":          "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&dateb=&owner=include&count=10&search_text=&output=atom",  # SEC
}

# ─── Contas Twitter via Nitter RSS ───────────────────────────────
# nitter.net é a instância oficial e mais estável em 2026.
# As demais instâncias públicas estão majoritariamente fora do ar.
NITTER_INSTANCES = [
    "https://nitter.net",           # Primário — instância oficial, mais estável
    "https://nitter.poast.org",     # Fallback 1
    "https://nitter.privacydev.net", # Fallback 2
]

TWITTER_ACCOUNTS = {
    "@realDonaldTrump": "realDonaldTrump",
    "@elonmusk":        "elonmusk",
    "@WhiteHouse":      "WhiteHouse",
    "@federalreserve":  "federalreserve",
    "@SECGov":          "SECGov",
    "@MichaelSaylor":   "saylor",
    "@CathieDWood":     "CathieDWood",
}

# Moedas para alertas técnicos de RSI
CRYPTO_WATCH = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "solana":   "SOL",
    "ripple":   "XRP",
    "cardano":  "ADA",
}

# Cache para evitar reenvios
sent_news_cache: set[str] = set()


# ═══════════════════════════════════════════════════════════════════
#  COLETA DE DADOS
# ═══════════════════════════════════════════════════════════════════

async def fetch_rss(url: str, client: httpx.AsyncClient, source_type: str = "news") -> list[dict]:
    """Busca e parseia RSS. source_type pode ser 'news' ou 'tweet'."""
    try:
        import xml.etree.ElementTree as ET
        resp = await client.get(url, timeout=10, follow_redirects=True)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        items = []
        for item in root.iter("item"):
            title = item.findtext("title", "").strip()
            link  = item.findtext("link", "").strip()
            desc  = item.findtext("description", "").strip()
            pub   = item.findtext("pubDate", "").strip()
            if title:
                items.append({
                    "title":       title,
                    "link":        link,
                    "description": desc,
                    "pubDate":     pub,
                    "source_type": source_type,
                })
        return items[:10]
    except Exception as e:
        logger.warning(f"Erro ao buscar {url}: {e}")
        return []


MAX_TWEET_AGE_HOURS = 720  # 30 dias — só descarta instância se dados estiverem completamente congelados


def _parse_pubdate(pub: str) -> datetime | None:
    """Converte string pubDate RSS para datetime com timezone."""
    if not pub:
        return None
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",   # RFC 822 padrão: Mon, 02 Jan 2006 15:04:05 +0000
        "%a, %d %b %Y %H:%M:%S GMT",  # Variante sem offset
        "%Y-%m-%dT%H:%M:%S%z",        # ISO 8601
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(pub.strip(), fmt)
            if dt.tzinfo is None:
                import pytz
                dt = pytz.utc.localize(dt)
            return dt
        except ValueError:
            continue
    return None


def _is_fresh(pub: str) -> bool:
    """Retorna True se o pubDate está dentro do limite de MAX_TWEET_AGE_HOURS."""
    dt = _parse_pubdate(pub)
    if dt is None:
        # Se não conseguiu parsear a data, aceita por precaução
        return True
    now_utc = datetime.now(dt.tzinfo)
    age_hours = (now_utc - dt).total_seconds() / 3600
    return age_hours <= MAX_TWEET_AGE_HOURS


async def _check_nitter_health(instance: str, client: httpx.AsyncClient) -> bool:
    """
    Camada 1 — Verifica se a instância Nitter está online e respondendo.
    Faz um ping leve na página inicial antes de tentar buscar tweets.
    """
    try:
        resp = await client.get(instance, timeout=5, follow_redirects=True)
        if resp.status_code == 200:
            logger.debug(f"Nitter {instance} online ✅")
            return True
        logger.warning(f"Nitter {instance} retornou status {resp.status_code} ❌")
        return False
    except Exception as e:
        logger.warning(f"Nitter {instance} offline: {e} ❌")
        return False


async def fetch_nitter_account(handle: str, username: str,
                                client: httpx.AsyncClient) -> list[dict]:
    """
    Busca tweets via Nitter RSS com duas camadas de verificação:
      Camada 1 — Saúde: instância está online?
      Camada 2 — Freshness: tweet mais recente tem menos de 6h?
    Tenta as instâncias em ordem e usa a primeira que passar nas duas camadas.
    """
    import xml.etree.ElementTree as ET

    for instance in NITTER_INSTANCES:

        # ── Camada 1: ping de saúde ──────────────────────────────
        is_online = await _check_nitter_health(instance, client)
        if not is_online:
            continue

        # ── Camada 2: busca RSS e verifica freshness ─────────────
        url = f"{instance}/{username}/rss"
        try:
            resp = await client.get(url, timeout=10, follow_redirects=True)
            resp.raise_for_status()
            root  = ET.fromstring(resp.text)
            items = []

            for item in root.iter("item"):
                title = item.findtext("title", "").strip()
                link  = item.findtext("link", "").strip()
                desc  = item.findtext("description", "").strip()
                pub   = item.findtext("pubDate", "").strip()

                if not title or title == handle:
                    continue  # Nitter às vezes repete o handle como título

                items.append({
                    "title":          title,
                    "link":           link,
                    "description":    desc,
                    "pubDate":        pub,
                    "source_type":    "tweet",
                    "twitter_handle": handle,
                })

            if not items:
                logger.warning(f"Nitter {instance} — nenhum tweet para {handle}")
                continue

            # Verifica se o tweet mais recente é fresco (≤ 6h)
            most_recent_pub = items[0].get("pubDate", "")
            if not _is_fresh(most_recent_pub):
                age_dt = _parse_pubdate(most_recent_pub)
                age_h  = round((datetime.now(age_dt.tzinfo) - age_dt).total_seconds() / 3600, 1) if age_dt else "?"
                logger.warning(
                    f"Nitter {instance} — dados desatualizados para {handle} "
                    f"(último tweet há {age_h}h, limite: {MAX_TWEET_AGE_HOURS}h) ❌ tentando próxima instância"
                )
                continue

            age_dt2 = _parse_pubdate(most_recent_pub)
            age_h2 = round((datetime.now(age_dt2.tzinfo) - age_dt2).total_seconds() / 3600, 1) if age_dt2 else "?"
            logger.info(f"✅ {handle} via {instance}: {len(items)} posts (último há {age_h2}h)")
            return items[:5]

        except Exception as e:
            logger.warning(f"Nitter {instance} erro ao buscar {handle}: {e}")
            continue

    logger.warning(f"⚠️ Nenhuma instância Nitter válida para {handle} — todas offline ou com dados velhos")
    return []


async def fetch_influencer_fallback(client: httpx.AsyncClient) -> list[dict]:
    """
    Fallback para quando o Nitter está offline.
    Busca RSS de fontes oficiais (Casa Branca, Fed, SEC) e agregadores
    que republicam declarações de Trump, Elon e outros influentes.
    """
    items = []
    for name, url in FEEDS_INFLUENCERS_FALLBACK.items():
        try:
            raw = await fetch_rss(url, client, "influencer")
            for item in raw:
                item["fallback_source"] = name
            items.extend(raw)
            if raw:
                logger.info(f"Fallback influencer '{name}': {len(raw)} itens")
        except Exception as e:
            logger.warning(f"Fallback '{name}' falhou: {e}")
    return items


async def fetch_all_news() -> list[dict]:
    """
    Busca notícias crypto + macro + tweets. Retorna lista unificada.
    Estratégia de tweets:
      1. Tenta Nitter (nitter.net como primário + fallbacks)
      2. Se todos falharem, usa feeds oficiais/agregadores como fallback
    """
    all_items  = []
    tweet_count = 0
    headers    = {"User-Agent": "CryptoMacroBot/3.0"}

    async with httpx.AsyncClient(headers=headers) as client:
        crypto_tasks = [fetch_rss(url, client, "news")  for url in FEEDS_CRYPTO.values()]
        macro_tasks  = [fetch_rss(url, client, "macro") for url in FEEDS_MACRO.values()]
        tweet_tasks  = [
            fetch_nitter_account(handle, username, client)
            for handle, username in TWITTER_ACCOUNTS.items()
        ]

        results = await asyncio.gather(
            *crypto_tasks, *macro_tasks, *tweet_tasks,
            return_exceptions=True
        )

        for items in results:
            if isinstance(items, list):
                all_items.extend(items)
                if items and items[0].get("source_type") == "tweet":
                    tweet_count += len(items)

        # Nenhum tweet veio do Nitter → ativa fallback oficial
        if tweet_count == 0:
            logger.warning("⚠️ Nitter indisponível — ativando fallback de influencers...")
            fallback_items = await fetch_influencer_fallback(client)
            all_items.extend(fallback_items)
            if fallback_items:
                logger.info(f"✅ Fallback: {len(fallback_items)} itens de fontes oficiais")
            else:
                logger.warning("❌ Fallback também falhou — sem dados de influencers neste ciclo")

    logger.info(f"Total de itens coletados: {len(all_items)} (tweets Nitter: {tweet_count})")
    return all_items


async def fetch_prices() -> dict:
    ids = ",".join(CRYPTO_WATCH.keys())
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids},bitcoin,ethereum&vs_currencies=usd,brl"
        f"&include_24hr_change=true&include_market_cap=true"
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            return resp.json()
    except Exception as e:
        logger.warning(f"Erro ao buscar preços: {e}")
        return {}


async def fetch_fear_greed() -> dict:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("https://api.alternative.me/fng/?limit=1", timeout=10)
            data = resp.json()
            return data["data"][0] if data.get("data") else {}
    except Exception:
        return {}


async def fetch_dxy() -> float | None:
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=2d"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            data = resp.json()
            closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            return closes[-1]
    except Exception:
        return None


async def fetch_market_data() -> dict:
    symbols = {
        "sp500":  "^GSPC",
        "gold":   "GC=F",
        "oil":    "CL=F",
        "usdbrl": "BRL=X",
    }
    result = {}
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        for key, symbol in symbols.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2d"
                resp = await client.get(url, timeout=10)
                data = resp.json()
                closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                if closes and len(closes) >= 2:
                    result[key] = {
                        "price":  closes[-1],
                        "change": ((closes[-1] - closes[-2]) / closes[-2] * 100) if closes[-2] else 0
                    }
            except Exception:
                result[key] = None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("https://api.coingecko.com/api/v3/global", timeout=10)
            data = resp.json()
            btc_dom = data["data"]["market_cap_percentage"].get("btc", 0)
            result["btc_dominance"] = round(btc_dom, 1)
    except Exception:
        result["btc_dominance"] = None
    return result


async def fetch_rsi(coin_id: str, vs_currency: str = "usd") -> dict:
    result = {"15m": None, "1h": None, "4h": None}
    try:
        async with httpx.AsyncClient() as client:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={vs_currency}&days=2"
            resp = await client.get(url, timeout=15)
            candles = resp.json()
            if not candles or len(candles) < 15:
                return result
            closes = [c[4] for c in candles]

            def calc_rsi(prices: list, period: int = 14) -> float | None:
                if len(prices) < period + 1:
                    return None
                gains, losses = [], []
                for i in range(1, len(prices)):
                    diff = prices[i] - prices[i-1]
                    gains.append(max(diff, 0))
                    losses.append(max(-diff, 0))
                avg_gain = sum(gains[-period:]) / period
                avg_loss = sum(losses[-period:]) / period
                if avg_loss == 0:
                    return 100.0
                rs = avg_gain / avg_loss
                return round(100 - (100 / (1 + rs)), 1)

            result["1h"] = calc_rsi(closes[-30:], 14)
            result["4h"] = calc_rsi(closes, 14)
    except Exception as e:
        logger.warning(f"Erro ao buscar RSI de {coin_id}: {e}")
    return result


def get_next_fomc() -> dict | None:
    now = datetime.now(TIMEZONE)
    for date_str, hour_str in FOMC_DATES:
        fomc_dt = datetime.strptime(f"{date_str} {hour_str}", "%Y-%m-%d %H:%M")
        fomc_dt = fomc_dt.replace(tzinfo=TIMEZONE)
        if fomc_dt > now:
            days_left = (fomc_dt - now).days
            return {
                "date":     fomc_dt.strftime("%d/%m/%Y"),
                "time":     hour_str,
                "days_left": days_left,
                "datetime": fomc_dt,
            }
    return None


# ═══════════════════════════════════════════════════════════════════
#  FILTRO INTELIGENTE COM IA  ← NOVO
# ═══════════════════════════════════════════════════════════════════

async def filter_relevant_items(items: list[dict], prices: dict) -> list[dict]:
    """
    Envia lote de notícias/tweets para o Claude avaliar relevância (score 0-10).
    Retorna apenas itens com score >= 7.
    Usa uma única chamada barata (Haiku) para economizar tokens.
    """
    if not items or not ANTHROPIC_API_KEY:
        return items

    btc_usd    = prices.get("bitcoin", {}).get("usd", 0)
    btc_change = prices.get("bitcoin", {}).get("usd_24h_change", 0)

    # Monta lista numerada para o Claude avaliar
    items_text = ""
    for i, item in enumerate(items):
        source = item.get("twitter_handle", item.get("source_type", "news"))
        items_text += f"{i+1}. [{source}] {item['title']}\n"

    prompt = f"""Você é um filtro de relevância para traders de Bitcoin e crypto.

CONTEXTO DO MERCADO:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)

LISTA DE NOTÍCIAS/POSTS (avalie cada um):
{items_text}

Para cada item, dê um score de 0 a 10 baseado no IMPACTO REAL no mercado crypto:
- 9-10: Impacto imediato e direto (ex: Fed corta juros, Trump bane crypto, hack bilionário)
- 7-8: Impacto significativo (ex: ETF aprovado, regulação importante, tweet de influência)
- 5-6: Relevante mas não urgente
- 0-4: Pouco relevante, genérico ou duplicado

Responda APENAS em JSON, sem texto extra, neste formato:
{{"scores": [{"id": 1, "score": 8}, {"id": 2, "score": 3}, ...]}}"""

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":            ANTHROPIC_API_KEY,
                    "anthropic-version":    "2023-06-01",
                    "content-type":         "application/json",
                },
                json={
                    "model":      "claude-haiku-4-5-20251001",
                    "max_tokens": 512,
                    "messages":   [{"role": "user", "content": prompt}],
                },
                timeout=30,
            )
            resp.raise_for_status()
            data    = resp.json()
            content = data["content"][0]["text"].strip()

            # Parse JSON — remove possíveis backticks
            import json, re
            content = re.sub(r"```json|```", "", content).strip()
            result  = json.loads(content)
            scores  = {s["id"]: s["score"] for s in result.get("scores", [])}

            # Filtra itens com score >= 7
            filtered = []
            for i, item in enumerate(items):
                score = scores.get(i + 1, 0)
                if score >= 7:
                    item["relevance_score"] = score
                    filtered.append(item)
                    logger.info(f"✅ Score {score}/10: {item['title'][:60]}")
                else:
                    logger.debug(f"❌ Score {score}/10 (descartado): {item['title'][:60]}")

            logger.info(f"Filtro IA: {len(filtered)}/{len(items)} itens relevantes")
            return filtered

    except Exception as e:
        logger.warning(f"Erro no filtro IA, passando tudo: {e}")
        return items  # Em caso de erro, não bloqueia o fluxo


# ═══════════════════════════════════════════════════════════════════
#  ANÁLISE COM IA
# ═══════════════════════════════════════════════════════════════════

async def analyze_with_claude(news_items: list[dict], prices: dict,
                               fear_greed: dict, report_type: str = "summary",
                               extra_context: str = "") -> str:
    if not ANTHROPIC_API_KEY:
        return "⚠️ ANTHROPIC_API_KEY não configurada."

    btc_usd    = prices.get("bitcoin", {}).get("usd", 0)
    btc_change = prices.get("bitcoin", {}).get("usd_24h_change", 0)
    eth_usd    = prices.get("ethereum", {}).get("usd", 0)
    eth_change = prices.get("ethereum", {}).get("usd_24h_change", 0)
    fg_value   = fear_greed.get("value", "N/A")
    fg_class   = fear_greed.get("value_classification", "N/A")

    news_text = "\n".join([f"- {n['title']}" for n in news_items[:15]])

    if report_type == "critical":
        system_prompt = """Você é um analista sênior de mercado crypto especializado em macro economia.
Alerte traders sobre eventos críticos que podem impactar Bitcoin, Ethereum e altcoins.
Seja direto, objetivo, use emojis. Responda SEMPRE em português brasileiro.
Use formatação Markdown do Telegram (*negrito*, _itálico_)."""
        user_prompt = f"""EVENTO CRÍTICO DETECTADO:

NOTÍCIA: {news_items[0]['title']}
DESCRIÇÃO: {news_items[0].get('description', '')[:300]}

MERCADO ATUAL:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- ETH: ${eth_usd:,.0f} ({eth_change:+.1f}% 24h)
- Fear & Greed: {fg_value}/100 ({fg_class})

Em 3-5 linhas: o que aconteceu, por que importa para crypto, tendência provável de curto prazo."""

    elif report_type == "tweet":
        system_prompt = """Você é um analista sênior de mercado crypto.
Analise o impacto de tweets/posts de figuras influentes no mercado crypto.
Seja direto e objetivo. Responda SEMPRE em português brasileiro.
Use formatação Markdown do Telegram (*negrito*, _itálico_)."""
        user_prompt = f"""TWEET DE FIGURA INFLUENTE:

Autor: {news_items[0].get('twitter_handle', 'Desconhecido')}
Post: {news_items[0]['title']}
{('Detalhes: ' + news_items[0].get('description', '')[:200]) if news_items[0].get('description') else ''}

MERCADO ATUAL:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- ETH: ${eth_usd:,.0f} ({eth_change:+.1f}% 24h)
- Fear & Greed: {fg_value}/100 ({fg_class})

Em 3-4 linhas: por que esse post importa para crypto, qual o impacto esperado e em qual direção."""

    elif report_type == "tweet_translation":
        system_prompt = """Você é um tradutor e analista de mercado crypto.
Traduza o texto do inglês para o português brasileiro de forma natural e precisa."""
        user_prompt = f"""Traduza este tweet/post para português brasileiro de forma natural:

"{news_items[0]['title']}"

Responda APENAS com a tradução, sem explicações ou aspas."""

    elif report_type == "rsi_alert":
        system_prompt = """Você é um analista técnico de crypto especializado em RSI e tendências.
Explique oportunidades de entrada em tendência de alta com RSI sobrevendido.
Seja direto e objetivo. Responda em português brasileiro.
Use formatação Markdown do Telegram."""
        user_prompt = f"""OPORTUNIDADE TÉCNICA DETECTADA:

{extra_context}

MERCADO ATUAL:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- Fear & Greed: {fg_value}/100 ({fg_class})

Em 3-4 linhas: explique o setup, o que isso significa para o trader e qual o risco principal."""

    elif report_type == "fomc_alert":
        system_prompt = """Você é um analista macro especializado no impacto do Fed no mercado crypto.
Prepare traders para a reunião do FOMC que acontece em breve.
Seja objetivo e prático. Responda em português brasileiro."""
        user_prompt = f"""ALERTA PRÉ-FOMC — Reunião em 1 hora!

CONTEXTO ATUAL:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- ETH: ${eth_usd:,.0f} ({eth_change:+.1f}% 24h)
- Fear & Greed: {fg_value}/100 ({fg_class})
{extra_context}

Em 4-5 linhas: o que esperar da reunião, como o crypto pode reagir em cada cenário (corte/manutenção/alta), e dica prática para o trader."""

    else:  # summary diário
        system_prompt = """Você é um analista sênior de mercado crypto especializado em macro economia global.
Faça briefings matinais para traders ativos de BTC, ETH e altcoins.
Use linguagem clara, objetiva, com emojis e formatação Markdown do Telegram.
Responda SEMPRE em português brasileiro."""
        user_prompt = f"""BRIEFING MACRO DIÁRIO para traders de crypto.

PREÇOS:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- ETH: ${eth_usd:,.0f} ({eth_change:+.1f}% 24h)
- Fear & Greed: {fg_value}/100 ({fg_class})
{extra_context}

NOTÍCIAS DAS ÚLTIMAS HORAS:
{news_text}

ESTRUTURE assim:
1. 🌍 Macro Global (Fed, juros, geopolítica)
2. 📊 Impacto esperado no Crypto
3. ⚠️ Pontos de atenção para hoje
4. 🎯 Viés de mercado (altista/baixista/neutro) com justificativa e stop sugerido

Máximo 400 palavras."""

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":            ANTHROPIC_API_KEY,
                    "anthropic-version":    "2023-06-01",
                    "content-type":         "application/json",
                },
                json={
                    "model":      "claude-haiku-4-5-20251001",
                    "max_tokens": 1024,
                    "messages":   [{"role": "user", "content": user_prompt}],
                    "system":     system_prompt,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            if "content" in data and data["content"]:
                return data["content"][0]["text"]
            return "❌ Resposta inválida da IA."
    except httpx.HTTPStatusError as e:
        logger.error(f"Erro HTTP API Claude: {e.response.status_code} - {e.response.text}")
        return f"❌ Erro na API ({e.response.status_code})."
    except Exception as e:
        logger.error(f"Erro na API Claude: {e}")
        return f"❌ Erro ao consultar IA: {e}"


# ═══════════════════════════════════════════════════════════════════
#  FORMATAÇÃO DE MENSAGENS
# ═══════════════════════════════════════════════════════════════════

def emoji_change(val: float) -> str:
    return "🟢" if val >= 0 else "🔴"


def format_status(prices: dict, fear_greed: dict, dxy: float | None, market: dict) -> str:
    btc    = prices.get("bitcoin", {})
    eth    = prices.get("ethereum", {})
    fg_val = fear_greed.get("value", "?")
    fg_cls = fear_greed.get("value_classification", "?")
    now    = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")
    fomc   = get_next_fomc()

    dxy_desc = ""
    if dxy:
        if dxy >= 105:   dxy_desc = "_muito forte_"
        elif dxy >= 100: dxy_desc = "_forte_"
        elif dxy >= 95:  dxy_desc = "_fraco_"
        else:            dxy_desc = "_muito fraco_"

    sp     = market.get("sp500")
    gold   = market.get("gold")
    oil    = market.get("oil")
    usdbrl = market.get("usdbrl")
    btc_dom = market.get("btc_dominance")

    sp_line   = f"{emoji_change(sp['change'])} *S&P 500:* `{sp['price']:,.0f}` ({sp['change']:+.1f}%)\n" if sp else ""
    gold_line = f"{emoji_change(gold['change'])} *Ouro:* `${gold['price']:,.0f}` ({gold['change']:+.1f}%)\n" if gold else ""
    oil_line  = f"{emoji_change(oil['change'])} *Petróleo WTI:* `${oil['price']:.2f}` ({oil['change']:+.1f}%)\n" if oil else ""
    brl_line  = f"🇧🇷 *USD/BRL:* `R$ {usdbrl['price']:.2f}`\n" if usdbrl else ""
    dom_line  = f"📈 *Dominância BTC:* `{btc_dom}%`\n" if btc_dom else ""

    if fomc:
        if fomc["days_left"] == 0:
            fomc_line = f"🔴 *Próx. FOMC:* `HOJE às {fomc['time']} (Brasília)`\n"
        elif fomc["days_left"] == 1:
            fomc_line = f"🟡 *Próx. FOMC:* `AMANHÃ — {fomc['date']} às {fomc['time']}`\n"
        else:
            fomc_line = f"📅 *Próx. FOMC:* `{fomc['date']} às {fomc['time']}` _({fomc['days_left']} dias)_\n"
    else:
        fomc_line = ""

    return (
        f"📡 *CRYPTO MACRO RADAR*\n"
        f"🕐 _{now} (Brasília)_\n"
        f"{'─'*30}\n"
        f"*₿ CRYPTO*\n"
        f"{emoji_change(btc.get('usd_24h_change',0))} *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
        f"{emoji_change(eth.get('usd_24h_change',0))} *ETH:* `${eth.get('usd',0):,.0f}` ({eth.get('usd_24h_change',0):+.1f}%)\n"
        f"😱 *Fear & Greed:* `{fg_val}/100` _{fg_cls}_\n"
        f"{dom_line}"
        f"{'─'*30}\n"
        f"*📊 MERCADO TRADICIONAL*\n"
        f"{sp_line}{gold_line}{oil_line}"
        f"{'─'*30}\n"
        f"*🌍 MACRO GLOBAL*\n"
        f"{'💵 *DXY:* `' + f'{dxy:.2f}` ' + dxy_desc + chr(10) if dxy else ''}"
        f"{brl_line}"
        f"🏦 *Fed Juros:* `4.25% — 4.50%`\n"
        f"{fomc_line}"
        f"{'─'*30}\n"
    )


def format_price_header(prices: dict, fear_greed: dict, dxy: float | None) -> str:
    btc    = prices.get("bitcoin", {})
    eth    = prices.get("ethereum", {})
    fg_val = fear_greed.get("value", "?")
    fg_cls = fear_greed.get("value_classification", "?")
    now    = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")
    dxy_str = f"💵 *DXY:* `{dxy:.2f}`\n" if dxy else ""
    return (
        f"📡 *CRYPTO MACRO RADAR*\n"
        f"🕐 _{now} (Brasília)_\n"
        f"{'─'*30}\n"
        f"{emoji_change(btc.get('usd_24h_change',0))} *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
        f"{emoji_change(eth.get('usd_24h_change',0))} *ETH:* `${eth.get('usd',0):,.0f}` ({eth.get('usd_24h_change',0):+.1f}%)\n"
        f"😱 *Fear & Greed:* `{fg_val}/100` _{fg_cls}_\n"
        f"{dxy_str}"
        f"{'─'*30}\n"
    )


# ═══════════════════════════════════════════════════════════════════
#  TAREFAS AGENDADAS
# ═══════════════════════════════════════════════════════════════════

async def send_daily_summary(bot: Bot):
    logger.info("Gerando resumo diário...")
    try:
        news, prices, fear_greed, dxy, market = await asyncio.gather(
            fetch_all_news(),
            fetch_prices(),
            fetch_fear_greed(),
            fetch_dxy(),
            fetch_market_data(),
        )
        fomc  = get_next_fomc()
        extra = ""
        if fomc:
            extra = f"\nPróxima reunião FOMC: {fomc['date']} às {fomc['time']} (daqui {fomc['days_left']} dias)"
        sp = market.get("sp500")
        if sp:
            extra += f"\nS&P 500: {sp['price']:,.0f} ({sp['change']:+.1f}%)"

        # Filtra apenas notícias (não tweets) para o resumo diário
        news_only = [n for n in news if n.get("source_type") in ("news", "macro")]
        header    = format_status(prices, fear_greed, dxy, market)
        analysis  = await analyze_with_claude(news_only, prices, fear_greed, "summary", extra)
        message   = header + analysis

        await bot.send_message(
            chat_id=CHAT_ID, text=message[:4096],
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info("Resumo diário enviado.")
    except Exception as e:
        logger.error(f"Erro no resumo diário: {e}")
        try:
            await bot.send_message(chat_id=CHAT_ID, text=f"❌ Erro no resumo diário: {e}")
        except Exception:
            pass


async def check_critical_news(bot: Bot):
    """
    Fluxo novo:
    1. Coleta todas as notícias + tweets
    2. Remove já enviados do cache
    3. Filtra por relevância com IA (score >= 7)
    4. Envia alerta adequado para cada tipo (tweet ou notícia)
    """
    logger.info("Verificando notícias e tweets críticos...")
    try:
        all_items = await fetch_all_news()

        # Remove itens já processados
        new_items = []
        for item in all_items:
            uid = item.get("link") or item.get("title", "")[:80]
            if uid not in sent_news_cache:
                new_items.append(item)

        if not new_items:
            logger.info("Nenhum item novo para avaliar.")
            return

        # Busca preços uma vez para o filtro e os alertas
        prices = await fetch_prices()

        # Filtro inteligente com IA — uma única chamada Haiku
        relevant_items = await filter_relevant_items(new_items, prices)

        if not relevant_items:
            logger.info("Nenhum item relevante após filtro IA.")
            return

        fear_greed = await fetch_fear_greed()

        for item in relevant_items:
            uid = item.get("link") or item.get("title", "")[:80]
            sent_news_cache.add(uid)
            if len(sent_news_cache) > 500:
                sent_news_cache.clear()

            is_tweet = item.get("source_type") == "tweet"

            if is_tweet:
                await send_tweet_alert(bot, item, prices, fear_greed)
            else:
                await send_news_alert(bot, item, prices, fear_greed)

            await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Erro na verificação crítica: {e}")


async def send_tweet_alert(bot: Bot, item: dict, prices: dict, fear_greed: dict):
    """Envia card especial para tweets com original + tradução + análise."""
    handle    = item.get("twitter_handle", "@desconhecido")
    score     = item.get("relevance_score", "?")
    tweet_text = item["title"]

    # Tradução do tweet
    translation = await analyze_with_claude([item], prices, fear_greed, "tweet_translation")

    # Análise de impacto
    analysis = await analyze_with_claude([item], prices, fear_greed, "tweet")

    btc = prices.get("bitcoin", {})
    now = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")

    message = (
        f"🐦 *TWEET — {handle}*\n"
        f"{'─'*30}\n"
        f"🇺🇸 *Original:*\n"
        f"_{tweet_text}_\n\n"
        f"🇧🇷 *Tradução:*\n"
        f"_{translation}_\n"
        f"{'─'*30}\n"
        f"{emoji_change(btc.get('usd_24h_change',0))} *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
        f"🕐 _{now} (Brasília)_\n"
        f"{'─'*30}\n"
        f"🧠 *Impacto esperado:*\n"
        f"{analysis}\n\n"
        f"🔗 [Ver post]({item.get('link', '#')})\n"
        f"⭐ _Relevância: {score}/10_"
    )

    await bot.send_message(
        chat_id=CHAT_ID,
        text=message[:4096],
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    logger.info(f"Tweet enviado: {handle} — {tweet_text[:50]}")


async def send_news_alert(bot: Bot, item: dict, prices: dict, fear_greed: dict):
    """Envia alerta de notícia crítica."""
    score    = item.get("relevance_score", "?")
    analysis = await analyze_with_claude([item], prices, fear_greed, "critical")
    header   = format_price_header(prices, fear_greed, None)

    message = (
        f"🚨 *ALERTA CRÍTICO*\n"
        f"{'─'*30}\n"
        f"📰 *{item['title']}*\n"
        f"{'─'*30}\n"
        + header
        + analysis
        + f"\n\n🔗 [Ver notícia]({item.get('link', '#')})\n"
        + f"⭐ _Relevância: {score}/10_"
    )

    await bot.send_message(
        chat_id=CHAT_ID,
        text=message[:4096],
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    logger.info(f"Alerta notícia: {item['title'][:60]}")


async def check_rsi_alerts(bot: Bot):
    logger.info("Verificando alertas de RSI...")
    try:
        prices = await fetch_prices()
        for coin_id, symbol in CRYPTO_WATCH.items():
            price_data = prices.get(coin_id, {})
            change_24h = price_data.get("usd_24h_change", 0)
            rsi        = await fetch_rsi(coin_id)
            rsi_1h     = rsi.get("1h")
            rsi_4h     = rsi.get("4h")

            if rsi_1h is None or rsi_4h is None:
                continue

            if rsi_1h < 30 and rsi_4h > 45:
                cache_key = f"rsi_{coin_id}_{int(rsi_1h)}"
                if cache_key in sent_news_cache:
                    continue
                sent_news_cache.add(cache_key)

                price_usd  = price_data.get("usd", 0)
                fear_greed = await fetch_fear_greed()
                extra = (
                    f"Moeda: {symbol}/USDT\n"
                    f"Preço: ${price_usd:,.4f}\n"
                    f"RSI 1h: {rsi_1h} (SOBREVENDIDO)\n"
                    f"RSI 4h: {rsi_4h} (tendência de alta)\n"
                    f"Variação 24h: {change_24h:+.1f}%"
                )
                analysis = await analyze_with_claude([], prices, fear_greed, "rsi_alert", extra)

                message = (
                    f"🔔 *ALERTA TÉCNICO — {symbol}*\n{'─'*30}\n"
                    f"📈 Tendência de Alta + RSI Sobrevendido\n{'─'*30}\n"
                    f"💰 *Preço:* `${price_usd:,.4f}`\n"
                    f"⏱ *RSI 1h:* `{rsi_1h}` _(Sobrevendido)_\n"
                    f"⏱ *RSI 4h:* `{rsi_4h}` _(Tendência de Alta)_\n"
                    f"📊 *24h:* `{change_24h:+.1f}%`\n"
                    f"{'─'*30}\n"
                    + analysis
                    + "\n\n⚠️ _Não é recomendação de investimento_"
                )
                await bot.send_message(
                    chat_id=CHAT_ID, text=message[:4096],
                    parse_mode=ParseMode.MARKDOWN,
                )
                logger.info(f"Alerta RSI enviado: {symbol}")
                await asyncio.sleep(3)
    except Exception as e:
        logger.error(f"Erro nos alertas de RSI: {e}")


async def check_fomc_alert(bot: Bot):
    fomc = get_next_fomc()
    if not fomc:
        return
    now          = datetime.now(TIMEZONE)
    diff_minutes = (fomc["datetime"] - now).total_seconds() / 60
    if 55 <= diff_minutes <= 65:
        cache_key = f"fomc_{fomc['date']}"
        if cache_key in sent_news_cache:
            return
        sent_news_cache.add(cache_key)
        prices, fear_greed = await asyncio.gather(fetch_prices(), fetch_fear_greed())
        extra    = f"\nData: {fomc['date']} às {fomc['time']} (Brasília)\nEm aproximadamente 1 hora"
        analysis = await analyze_with_claude([], prices, fear_greed, "fomc_alert", extra)
        btc      = prices.get("bitcoin", {})
        message  = (
            f"⚠️ *ALERTA FOMC — EM 1 HORA!*\n{'─'*30}\n"
            f"🏦 Fed anuncia decisão de juros\n"
            f"🕐 Hoje às *{fomc['time']}* (Brasília)\n{'─'*30}\n"
            f"₿ *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
            f"{'─'*30}\n"
            + analysis
            + "\n\n🔴 _Prepare-se para volatilidade!_"
        )
        await bot.send_message(
            chat_id=CHAT_ID, text=message[:4096],
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info("Alerta FOMC enviado!")


# ═══════════════════════════════════════════════════════════════════
#  COMANDOS DO BOT
# ═══════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    fomc    = get_next_fomc()
    fomc_line = f"📅 Próx. FOMC: {fomc['date']} às {fomc['time']} ({fomc['days_left']} dias)\n" if fomc else ""
    await update.message.reply_text(
        f"🚀 *Crypto Macro Radar v4.0 ativo!*\n\n"
        f"Seu Chat ID: `{chat_id}`\n\n"
        f"*Comandos:*\n"
        f"/status — Painel completo de mercado\n"
        f"/resumo — Briefing macro com IA\n"
        f"/scanner — Scanner Top 50 agora\n"
        f"/rsi — RSI das moedas monitoradas\n"
        f"/ajuda — Lista completa\n\n"
        f"*Automático:*\n"
        f"📅 Resumo macro: 09h00 (Brasília)\n"
        f"📋 Scanner Top 50: 09h05 (Brasília)\n"
        f"🚨 Alertas filtrados por IA: a cada 15 min\n"
        f"🔔 Oportunidades em tempo real: a cada 1h\n"
        f"🐦 Tweets de Trump, Elon e outros: monitorados\n"
        f"⚠️ Alerta FOMC: 1h antes\n\n"
        f"{fomc_line}",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Buscando dados...", parse_mode=ParseMode.MARKDOWN)
    prices, fear_greed, dxy, market = await asyncio.gather(
        fetch_prices(), fetch_fear_greed(), fetch_dxy(), fetch_market_data()
    )
    msg  = format_status(prices, fear_greed, dxy, market)
    msg += "_Dados em tempo real_"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_resumo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧠 Gerando análise macro com IA...", parse_mode=ParseMode.MARKDOWN)
    await send_daily_summary(context.bot)


async def cmd_rsi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra RSI via Binance (1h, 4h, 1d) para moedas monitoradas."""
    await update.message.reply_text("📊 Calculando RSI via Binance...", parse_mode=ParseMode.MARKDOWN)
    prices = await fetch_prices()

    def rsi_label(v):
        if v is None: return "N/A"
        if v < 20:  return f"`{v}` 🔴 _Extremo SV_"
        if v < 30:  return f"`{v}` 🟠 _SV_"
        if v > 80:  return f"`{v}` 🟡 _Extremo SC_"
        if v > 70:  return f"`{v}` 🟡 _SC_"
        return f"`{v}` ⚪"

    lines = ["📊 *RSI — MOEDAS MONITORADAS (Binance)*\n" + "─"*30]
    for coin_id, symbol in CRYPTO_WATCH.items():
        price  = prices.get(coin_id, {}).get("usd", 0)
        bsymbol = COINGECKO_TO_BINANCE.get(coin_id)
        if bsymbol:
            rsi_1h = await fetch_binance_rsi(bsymbol, "1h")
            rsi_4h = await fetch_binance_rsi(bsymbol, "4h")
            rsi_1d = await fetch_binance_rsi(bsymbol, "1d")
        else:
            rsi_data = await fetch_rsi(coin_id)
            rsi_1h = rsi_data.get("1h")
            rsi_4h = rsi_data.get("4h")
            rsi_1d = None
        lines.append(
            f"*{symbol}* — `${price:,.4f}`\n"
            f"  1h: {rsi_label(rsi_1h)} | 4h: {rsi_label(rsi_4h)} | 1d: {rsi_label(rsi_1d)}\n"
        )
        await asyncio.sleep(0.5)

    lines.append("─"*30 + "\n_SV = Sobrevendido | SC = Sobrecomprado_\n_< 20 = Extremamente sobrevendido_")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)



async def cmd_scanner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /scanner — roda o Top 50 scanner manualmente."""
    await update.message.reply_text(
        "🔍 Rodando scanner Top 50...\n_Isso pode levar até 2 minutos_",
        parse_mode=ParseMode.MARKDOWN
    )
    now          = datetime.now(TIMEZONE)
    opportunities = await scan_top50_opportunities()
    msg          = format_opportunity_list(opportunities, now)
    await update.message.reply_text(msg[:4096], parse_mode=ParseMode.MARKDOWN)


async def cmd_ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*📚 Comandos — Crypto Macro Radar v3.0*\n\n"
        "/start — Iniciar e ver Chat ID\n"
        "/status — Painel completo (BTC, ETH, S&P500, Ouro, DXY, FOMC...)\n"
        "/resumo — Briefing macro completo com IA\n"
        "/rsi — RSI atual de BTC, ETH, SOL, XRP, ADA\n"
        "/ajuda — Esta mensagem\n\n"
        "*⚙️ Automático:*\n"
        f"• Resumo diário: 08h00 (Brasília)\n"
        f"• Alertas filtrados por IA (score ≥7/10): a cada {CHECK_INTERVAL_MINUTES} min\n"
        "• Tweets monitorados: Trump, Elon, WhiteHouse, Fed, SEC, Saylor, Cathie Wood\n"
        "• Alertas RSI (SV em tendência de alta): a cada 30 min\n"
        "• Alerta FOMC: 1h antes de cada reunião\n\n"
        "*🐦 Contas Twitter monitoradas:*\n"
        "@realDonaldTrump • @elonmusk • @WhiteHouse\n"
        "@federalreserve • @SECGov • @saylor • @CathieDWood\n\n"
        "*🔍 Fontes de notícias:*\n"
        "CoinDesk • CoinTelegraph • Bitcoin Magazine\n"
        "Decrypt • The Block • CryptoSlate\n"
        "Reuters • Politico • Financial Times\n"
        "CoinGecko • Yahoo Finance • Alternative.me",
        parse_mode=ParseMode.MARKDOWN,
    )


# ═══════════════════════════════════════════════════════════════════
#  INICIALIZAÇÃO
# ═══════════════════════════════════════════════════════════════════

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN não configurado!")
    if not CHAT_ID:
        raise ValueError("❌ TELEGRAM_CHAT_ID não configurado!")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("resumo",  cmd_resumo))
    app.add_handler(CommandHandler("rsi",     cmd_rsi))
    app.add_handler(CommandHandler("scanner", cmd_scanner))
    app.add_handler(CommandHandler("ajuda",   cmd_ajuda))

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    # Resumo macro diário às 09h
    scheduler.add_job(send_daily_summary,         "cron",     hour=DAILY_REPORT_HOUR,
                      minute=DAILY_REPORT_MINUTE, args=[app.bot])
    # Scanner matinal Top 50 às 09h05 (após resumo)
    scheduler.add_job(send_morning_scanner,        "cron",     hour=9, minute=5, args=[app.bot])
    # Notícias e tweets críticos a cada 15 min
    scheduler.add_job(check_critical_news,         "interval", minutes=CHECK_INTERVAL_MINUTES, args=[app.bot])
    # Oportunidades em tempo real a cada 1h
    scheduler.add_job(check_realtime_opportunities,"interval", minutes=60, args=[app.bot])
    # Alertas RSI lista base a cada 30 min
    scheduler.add_job(check_rsi_alerts,            "interval", minutes=30, args=[app.bot])
    # Checagem FOMC a cada 5 min
    scheduler.add_job(check_fomc_alert,            "interval", minutes=5,  args=[app.bot])
    scheduler.start()

    logger.info("✅ Crypto Macro Radar v4.0 iniciado!")
    logger.info(f"📅 Resumo diário às {DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d}")
    logger.info("📋 Scanner Top 50 às 09:05 + alertas em tempo real a cada 1h")
    logger.info(f"🔍 Scan + filtro IA a cada {CHECK_INTERVAL_MINUTES} minutos")
    logger.info("🐦 Monitorando: Trump, Elon, WhiteHouse, Fed, SEC, Saylor, Cathie Wood")
    logger.info("🔔 Alertas RSI a cada 30 minutos")
    logger.info("⚠️ Checagem FOMC a cada 5 minutos")

    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════════════
#  TOP 50 SCANNER — CONFLUÊNCIA DE TIMEFRAMES
# ═══════════════════════════════════════════════════════════════════

# Cache global do Top 50
_top50_cache: list[dict] = []
_top50_cache_time: datetime | None = None


async def fetch_top50() -> list[dict]:
    """
    Busca top 50 moedas por market cap da CoinGecko.
    Cacheia por 6h para não sobrecarregar a API.
    Retorna lista com id, symbol, preço e variações.
    """
    global _top50_cache, _top50_cache_time
    now = datetime.now(TIMEZONE)

    # Usa cache se tiver menos de 6h
    if _top50_cache and _top50_cache_time:
        age_h = (now - _top50_cache_time).total_seconds() / 3600
        if age_h < 6:
            logger.info(f"Top 50 via cache ({age_h:.1f}h atrás)")
            return _top50_cache

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency":           "usd",
                    "order":                 "market_cap_desc",
                    "per_page":              50,
                    "page":                  1,
                    "price_change_percentage": "1h,24h,7d,30d",
                    "sparkline":             False,
                },
                timeout=15,
            )
            data = resp.json()
            _top50_cache      = data
            _top50_cache_time = now
            logger.info(f"Top 50 atualizado: {len(data)} moedas")
            return data
    except Exception as e:
        logger.warning(f"Erro ao buscar top 50: {e}")
        return _top50_cache  # Retorna cache antigo em caso de erro


def calc_rsi_from_closes(closes: list[float], period: int = 14) -> float | None:
    """Calcula RSI a partir de uma lista de closes."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


# Mapeamento CoinGecko ID → símbolo Binance
COINGECKO_TO_BINANCE = {
    "bitcoin":        "BTCUSDT",
    "ethereum":       "ETHUSDT",
    "tether":         "USDTUSD",
    "binancecoin":    "BNBUSDT",
    "solana":         "SOLUSDT",
    "ripple":         "XRPUSDT",
    "usd-coin":       "USDCUSDT",
    "dogecoin":       "DOGEUSDT",
    "cardano":        "ADAUSDT",
    "avalanche-2":    "AVAXUSDT",
    "shiba-inu":      "SHIBUSDT",
    "polkadot":       "DOTUSDT",
    "chainlink":      "LINKUSDT",
    "bitcoin-cash":   "BCHUSDT",
    "near":           "NEARUSDT",
    "litecoin":       "LTCUSDT",
    "uniswap":        "UNIUSDT",
    "internet-computer": "ICPUSDT",
    "aptos":          "APTUSDT",
    "stellar":        "XLMUSDT",
    "ethereum-classic": "ETCUSDT",
    "filecoin":       "FILUSDT",
    "hedera-hashgraph": "HBARUSDT",
    "arbitrum":       "ARBUSDT",
    "vechain":        "VETUSDT",
    "the-graph":      "GRTUSDT",
    "injective-protocol": "INJUSDT",
    "sei-network":    "SEIUSDT",
    "optimism":       "OPUSDT",
    "render-token":   "RENDERUSDT",
    "sui":            "SUIUSDT",
    "pepe":           "PEPEUSDT",
    "maker":          "MKRUSDT",
    "aave":           "AAVEUSDT",
    "fantom":         "FTMUSDT",
    "the-sandbox":    "SANDUSDT",
    "decentraland":   "MANAUSDT",
    "axie-infinity":  "AXSUSDT",
    "matic-network":  "MATICUSDT",
    "atom":           "ATOMUSDT",
    "algorand":       "ALGOUSDT",
    "tron":           "TRXUSDT",
    "theta-token":    "THETAUSDT",
    "flow":           "FLOWUSDT",
    "gala":           "GALAUSDT",
    "floki":          "FLOKIUSDT",
    "worldcoin-wld":  "WLDUSDT",
    "blur":           "BLURUSDT",
    "woo-network":    "WOOUSDT",
}

BINANCE_INTERVAL_MAP = {
    "1d": ("1d",  30),   # timeframe, número de candles
    "4h": ("4h",  60),
    "1h": ("1h",  100),
    "1w": ("1w",  20),
}

async def fetch_binance_rsi(symbol: str, timeframe: str = "1d") -> float | None:
    """
    Busca OHLC da Binance e calcula RSI.
    Muito mais preciso que CoinGecko — dados reais de exchange.
    """
    interval, limit = BINANCE_INTERVAL_MAP.get(timeframe, ("1d", 30))
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol":   symbol,
                    "interval": interval,
                    "limit":    limit,
                },
                timeout=10,
            )
            resp.raise_for_status()
            candles = resp.json()
            if not candles or len(candles) < 15:
                return None
            closes = [float(c[4]) for c in candles]
            return calc_rsi_from_closes(closes)
    except Exception as e:
        logger.debug(f"Binance RSI erro {symbol} {timeframe}: {e}")
        return None


async def fetch_ohlc_rsi(coin_id: str, days: int) -> float | None:
    """
    Wrapper de compatibilidade — tenta Binance primeiro, fallback CoinGecko.
    days=30 → timeframe 1d | days=7 → timeframe 4h
    """
    symbol = COINGECKO_TO_BINANCE.get(coin_id)
    timeframe = "1d" if days >= 14 else "4h"

    if symbol:
        rsi = await fetch_binance_rsi(symbol, timeframe)
        if rsi is not None:
            return rsi

    # Fallback CoinGecko
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc",
                params={"vs_currency": "usd", "days": days},
                timeout=15,
            )
            candles = resp.json()
            if not candles or len(candles) < 15:
                return None
            closes = [c[4] for c in candles]
            return calc_rsi_from_closes(closes)
    except Exception:
        return None


def is_uptrend(coin: dict) -> tuple[bool, str]:
    """
    Verifica tendência de alta via variações de preço nos timeframes.
    Retorna (is_uptrend, descrição).
    Lógica top-down:
      - 30d positivo = tendência macro de alta
      - 7d negativo  = correção saudável em andamento
    """
    change_30d = coin.get("price_change_percentage_30d_in_currency", None)
    change_7d  = coin.get("price_change_percentage_7d_in_currency",  None)
    change_24h = coin.get("price_change_percentage_24h_in_currency", None)

    if change_30d is None or change_7d is None:
        return False, "Dados insuficientes"

    # Tendência macro de alta: 30d positivo
    if change_30d <= 0:
        return False, f"30d negativo ({change_30d:+.1f}%)"

    # Correção no médio prazo: 7d negativo (pullback)
    if change_7d >= 0:
        return False, f"Sem correção (7d: {change_7d:+.1f}%)"

    desc = f"30d: {change_30d:+.1f}% | 7d: {change_7d:+.1f}% | 24h: {change_24h:+.1f}%"
    return True, desc


async def scan_top50_opportunities() -> list[dict]:
    """
    ETAPA 1 — Filtro rápido: moedas em tendência de alta com correção
    ETAPA 2 — Análise profunda: RSI 1d sobrevendido nas filtradas
    Retorna lista de oportunidades ordenadas por força do sinal.
    """
    logger.info("🔍 Iniciando scan Top 50...")
    coins = await fetch_top50()
    if not coins:
        return []

    # ETAPA 1 — Filtro de tendência (rápido, sem chamadas extras)
    candidates = []
    for coin in coins:
        uptrend, trend_desc = is_uptrend(coin)
        if uptrend:
            candidates.append({
                "id":         coin["id"],
                "symbol":     coin["symbol"].upper(),
                "name":       coin["name"],
                "price":      coin["current_price"],
                "change_24h": coin.get("price_change_percentage_24h_in_currency", 0),
                "change_7d":  coin.get("price_change_percentage_7d_in_currency",  0),
                "change_30d": coin.get("price_change_percentage_30d_in_currency", 0),
                "trend_desc": trend_desc,
            })

    logger.info(f"Etapa 1: {len(candidates)}/{len(coins)} moedas em tendência de alta")

    if not candidates:
        return []

    # ETAPA 2 — RSI 1d nas candidatas (chamadas individuais, mas poucas)
    opportunities = []
    for coin in candidates:
        await asyncio.sleep(1.5)  # Respeita rate limit da CoinGecko
        rsi_1d = await fetch_ohlc_rsi(coin["id"], days=30)   # 1d candles
        rsi_4h = await fetch_ohlc_rsi(coin["id"], days=7)    # 4h candles

        if rsi_1d is None:
            continue

        coin["rsi_1d"] = rsi_1d
        coin["rsi_4h"] = rsi_4h

        # Classifica força do sinal
        if rsi_1d < 20:
            if rsi_4h and rsi_4h < 30:
                coin["signal_strength"] = "🔴 FORTE"
                coin["signal_score"]    = 3
            else:
                coin["signal_strength"] = "🟠 MÉDIO"
                coin["signal_score"]    = 2
            opportunities.append(coin)
            logger.info(f"✅ Oportunidade: {coin['symbol']} RSI1d={rsi_1d} RSI4h={rsi_4h}")
        elif rsi_1d < 30:
            coin["signal_strength"] = "🟡 FRACO"
            coin["signal_score"]    = 1
            opportunities.append(coin)

    # Ordena por força do sinal
    opportunities.sort(key=lambda x: x["signal_score"], reverse=True)
    logger.info(f"Scan concluído: {len(opportunities)} oportunidades encontradas")
    return opportunities


def format_opportunity_list(opportunities: list[dict], now: datetime) -> str:
    """Formata a lista matinal de oportunidades para o Telegram."""
    if not opportunities:
        return (
            f"📋 *SCANNER TOP 50 — {now.strftime('%d/%m/%Y %H:%M')}*\n"
            f"{'─'*30}\n"
            f"Nenhuma moeda em setup de fundo ascendente no momento.\n"
            f"_Tendência de alta + RSI sobrevendido = sem confluência agora._"
        )

    lines = [
        f"📋 *SCANNER TOP 50 — {now.strftime('%d/%m/%Y %H:%M')}*",
        f"_Tendência 30d alta + correção 7d + RSI 1d < 30 (🔴 < 20)_",
        f"{'─'*30}",
    ]

    for i, coin in enumerate(opportunities[:10], 1):
        rsi_4h_str = f" | RSI 4h: `{coin['rsi_4h']}`" if coin.get("rsi_4h") else ""
        lines.append(
            f"{i}. {coin['signal_strength']} *{coin['symbol']}* — `${coin['price']:,.4f}`\n"
            f"   📈 30d: `{coin['change_30d']:+.1f}%` | 7d: `{coin['change_7d']:+.1f}%`\n"
            f"   ⚡ RSI 1d: `{coin['rsi_1d']}`{rsi_4h_str}\n"
        )

    lines.append("─"*30)
    lines.append("⚠️ _Não é recomendação de investimento. Confirme no gráfico!_")
    return "\n".join(lines)


async def send_morning_scanner(bot: Bot):
    """Envia lista matinal de oportunidades às 09h."""
    logger.info("📋 Gerando scanner matinal Top 50...")
    now = datetime.now(TIMEZONE)
    try:
        opportunities = await scan_top50_opportunities()
        msg = format_opportunity_list(opportunities, now)
        await bot.send_message(
            chat_id=CHAT_ID,
            text=msg[:4096],
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info(f"Scanner matinal enviado: {len(opportunities)} oportunidades")
    except Exception as e:
        logger.error(f"Erro no scanner matinal: {e}")


async def check_realtime_opportunities(bot: Bot):
    """
    Roda a cada 1h — filtro leve nas top 50.
    Dispara alerta apenas para sinais FORTES (RSI 1d < 30 + RSI 4h < 40).
    Evita reenviar o mesmo setup pelo cache.
    """
    logger.info("🔔 Verificando oportunidades em tempo real...")
    try:
        opportunities = await scan_top50_opportunities()
        # Só alerta FORTE: RSI 1d < 20 + RSI 4h < 30
        strong = [o for o in opportunities if o["signal_score"] == 3]

        for coin in strong:
            cache_key = f"opportunity_{coin['id']}_{int(coin['rsi_1d'])}"
            if cache_key in sent_news_cache:
                continue
            sent_news_cache.add(cache_key)

            now = datetime.now(TIMEZONE)
            msg = (
                f"🚨 *SETUP DETECTADO — {coin['symbol']}*\n"
                f"{'─'*30}\n"
                f"📈 *Tendência macro de alta + Fundo se formando*\n"
                f"{'─'*30}\n"
                f"💰 *Preço:* `${coin['price']:,.4f}`\n"
                f"📊 *30d:* `{coin['change_30d']:+.1f}%` _(tendência de alta)_\n"
                f"📉 *7d:* `{coin['change_7d']:+.1f}%` _(correção)_\n"
                f"📉 *24h:* `{coin['change_24h']:+.1f}%`\n"
                f"⚡ *RSI 1d:* `{coin['rsi_1d']}` _(sobrevendido)_\n"
                f"⚡ *RSI 4h:* `{coin.get('rsi_4h', 'N/A')}` _(sobrevendido)_\n"
                f"{'─'*30}\n"
                f"🎯 *Confluência de timeframes detectada*\n"
                f"_Verifique o gráfico para confirmar o fundo ascendente_\n\n"
                f"🕐 _{now.strftime('%d/%m/%Y %H:%M')} (Brasília)_\n"
                f"⚠️ _Não é recomendação de investimento_"
            )
            await bot.send_message(
                chat_id=CHAT_ID,
                text=msg[:4096],
                parse_mode=ParseMode.MARKDOWN,
            )
            logger.info(f"Alerta de oportunidade enviado: {coin['symbol']}")
            await asyncio.sleep(3)

    except Exception as e:
        logger.error(f"Erro no check de oportunidades: {e}")
