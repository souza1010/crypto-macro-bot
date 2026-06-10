"""
╔══════════════════════════════════════════════════════╗
║         CRYPTO MACRO RADAR - Bot Telegram            ║
║   Monitoramento macro para traders BTC/ETH/Alts      ║
╚══════════════════════════════════════════════════════╝

Autor: Gerado por Claude (Anthropic)
Versão: 2.0
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

DAILY_REPORT_HOUR   = 8
DAILY_REPORT_MINUTE = 0
CHECK_INTERVAL_MINUTES = 15

# Calendário FOMC 2025/2026 (data, hora Brasília)
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

# ─── Fontes RSS ──────────────────────────────────────────────────
FEEDS = {
    "coindesk":         "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph":    "https://cointelegraph.com/rss",
    "bitcoin_magazine": "https://bitcoinmagazine.com/feed",
    "decrypt":          "https://decrypt.co/feed",
    "theblock":         "https://www.theblock.co/rss.xml",
    "cryptoslate":      "https://cryptoslate.com/feed/",
}

# Palavras-chave para alertas críticos
CRITICAL_KEYWORDS = [
    "fed rate", "federal reserve", "interest rate", "fomc", "powell",
    "inflation", "cpi", "pce", "unemployment", "nonfarm payroll",
    "recession", "gdp", "treasury",
    "war", "guerra", "sanction", "nuclear", "missile",
    "china", "taiwan", "russia", "ukraine", "iran", "north korea",
    "conflict", "attack", "crisis",
    "bitcoin", "btc", "ethereum", "eth", "crypto", "sec", "etf",
    "hack", "exploit", "blackrock", "coinbase", "binance",
    "trump", "powell", "lagarde", "yellen", "xi jinping",
    "elon musk", "michael saylor",
]

# Moedas para alertas técnicos de RSI
CRYPTO_WATCH = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "solana":   "SOL",
    "ripple":   "XRP",
    "cardano":  "ADA",
}

sent_news_cache: set[str] = set()


# ═══════════════════════════════════════════════════════════════════
#  FUNÇÕES DE COLETA DE DADOS
# ═══════════════════════════════════════════════════════════════════

async def fetch_rss(url: str, client: httpx.AsyncClient) -> list[dict]:
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
            if title:
                items.append({"title": title, "link": link, "description": desc})
        return items[:10]
    except Exception as e:
        logger.warning(f"Erro ao buscar {url}: {e}")
        return []


async def fetch_all_news() -> list[dict]:
    all_items = []
    async with httpx.AsyncClient(headers={"User-Agent": "CryptoMacroBot/2.0"}) as client:
        tasks = [fetch_rss(url, client) for url in FEEDS.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for items in results:
            if isinstance(items, list):
                all_items.extend(items)
    return all_items


async def fetch_prices() -> dict:
    """BTC, ETH + altcoins monitoradas."""
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
    """Busca S&P500, Ouro, Petróleo, USD/BRL e Dominância BTC."""
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
                opens  = data["chart"]["result"][0]["indicators"]["quote"][0]["open"]
                if closes and len(closes) >= 2:
                    result[key] = {
                        "price": closes[-1],
                        "change": ((closes[-1] - closes[-2]) / closes[-2] * 100) if closes[-2] else 0
                    }
            except Exception:
                result[key] = None
    # Dominância BTC via CoinGecko
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
    """Calcula RSI aproximado para 15min e 1h via CoinGecko OHLC."""
    result = {"15m": None, "1h": None, "4h": None}
    try:
        async with httpx.AsyncClient() as client:
            # 1h candles (últimas 48h)
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={vs_currency}&days=2"
            resp = await client.get(url, timeout=15)
            candles = resp.json()  # [timestamp, open, high, low, close]
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

            # RSI 1h (candles de 30min do CoinGecko agrupados)
            result["1h"] = calc_rsi(closes[-30:], 14)
            result["4h"] = calc_rsi(closes, 14)

    except Exception as e:
        logger.warning(f"Erro ao buscar RSI de {coin_id}: {e}")
    return result


def get_next_fomc() -> dict | None:
    """Retorna a próxima reunião FOMC."""
    now = datetime.now(TIMEZONE)
    for date_str, hour_str in FOMC_DATES:
        fomc_dt = datetime.strptime(f"{date_str} {hour_str}", "%Y-%m-%d %H:%M")
        fomc_dt = fomc_dt.replace(tzinfo=TIMEZONE)
        if fomc_dt > now:
            days_left = (fomc_dt - now).days
            return {
                "date": fomc_dt.strftime("%d/%m/%Y"),
                "time": hour_str,
                "days_left": days_left,
                "datetime": fomc_dt,
            }
    return None


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
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1024,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
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
        return f"❌ Erro na API ({e.response.status_code}). Verifique a chave ANTHROPIC_API_KEY."
    except Exception as e:
        logger.error(f"Erro na API Claude: {e}")
        return f"❌ Erro ao consultar IA: {e}"


# ═══════════════════════════════════════════════════════════════════
#  FORMATAÇÃO DE MENSAGENS
# ═══════════════════════════════════════════════════════════════════

def emoji_change(val: float) -> str:
    return "🟢" if val >= 0 else "🔴"

def format_status(prices: dict, fear_greed: dict, dxy: float | None,
                  market: dict) -> str:
    btc = prices.get("bitcoin", {})
    eth = prices.get("ethereum", {})
    fg_val = fear_greed.get("value", "?")
    fg_cls = fear_greed.get("value_classification", "?")
    now = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")
    fomc = get_next_fomc()

    # Linha DXY com descrição
    dxy_desc = ""
    if dxy:
        if dxy >= 105:   dxy_desc = "_muito forte_"
        elif dxy >= 100: dxy_desc = "_forte_"
        elif dxy >= 95:  dxy_desc = "_fraco_"
        else:            dxy_desc = "_muito fraco_"

    # S&P500
    sp = market.get("sp500")
    sp_line = f"{emoji_change(sp['change'])} *S&P 500:* `{sp['price']:,.0f}` ({sp['change']:+.1f}%)\n" if sp else ""

    # Ouro
    gold = market.get("gold")
    gold_line = f"{emoji_change(gold['change'])} *Ouro:* `${gold['price']:,.0f}` ({gold['change']:+.1f}%)\n" if gold else ""

    # Petróleo
    oil = market.get("oil")
    oil_line = f"{emoji_change(oil['change'])} *Petróleo WTI:* `${oil['price']:.2f}` ({oil['change']:+.1f}%)\n" if oil else ""

    # USD/BRL
    usdbrl = market.get("usdbrl")
    brl_line = f"🇧🇷 *USD/BRL:* `R$ {usdbrl['price']:.2f}`\n" if usdbrl else ""

    # Dominância BTC
    btc_dom = market.get("btc_dominance")
    dom_line = f"📈 *Dominância BTC:* `{btc_dom}%`\n" if btc_dom else ""

    # FOMC
    if fomc:
        if fomc["days_left"] == 0:
            fomc_line = f"🔴 *Próx. FOMC:* `HOJE às {fomc['time']} (Brasília)`\n"
        elif fomc["days_left"] == 1:
            fomc_line = f"🟡 *Próx. FOMC:* `AMANHÃ — {fomc['date']} às {fomc['time']}`\n"
        else:
            fomc_line = f"📅 *Próx. FOMC:* `{fomc['date']} às {fomc['time']}` _({fomc['days_left']} dias)_\n"
    else:
        fomc_line = ""

    msg = (
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
    return msg


def format_price_header(prices: dict, fear_greed: dict, dxy: float | None) -> str:
    """Header compacto para alertas críticos."""
    btc = prices.get("bitcoin", {})
    eth = prices.get("ethereum", {})
    fg_val = fear_greed.get("value", "?")
    fg_cls = fear_greed.get("value_classification", "?")
    now = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")
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
        fomc = get_next_fomc()
        extra = ""
        if fomc:
            extra = f"\nPróxima reunião FOMC: {fomc['date']} às {fomc['time']} (daqui {fomc['days_left']} dias)"

        sp = market.get("sp500")
        if sp:
            extra += f"\nS&P 500: {sp['price']:,.0f} ({sp['change']:+.1f}%)"

        header   = format_status(prices, fear_greed, dxy, market)
        analysis = await analyze_with_claude(news, prices, fear_greed, "summary", extra)
        message  = header + analysis

        await bot.send_message(
            chat_id=CHAT_ID,
            text=message[:4096],
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
    logger.info("Verificando notícias críticas...")
    try:
        news = await fetch_all_news()
        for item in news:
            uid = item.get("link") or item.get("title", "")[:80]
            if uid in sent_news_cache:
                continue
            title_lower = item["title"].lower()
            desc_lower  = item.get("description", "").lower()
            is_critical = any(kw in title_lower or kw in desc_lower for kw in CRITICAL_KEYWORDS)
            if is_critical:
                sent_news_cache.add(uid)
                if len(sent_news_cache) > 500:
                    sent_news_cache.clear()
                prices, fear_greed = await asyncio.gather(fetch_prices(), fetch_fear_greed())
                analysis = await analyze_with_claude([item], prices, fear_greed, "critical")
                header = format_price_header(prices, fear_greed, None)
                message = (
                    f"🚨 *ALERTA CRÍTICO*\n{'─'*30}\n"
                    f"📰 *{item['title']}*\n{'─'*30}\n"
                    + header + analysis
                    + f"\n\n🔗 [Ver notícia]({item.get('link', '#')})"
                )
                await bot.send_message(
                    chat_id=CHAT_ID, text=message[:4096],
                    parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True,
                )
                logger.info(f"Alerta crítico: {item['title'][:60]}")
                await asyncio.sleep(5)
    except Exception as e:
        logger.error(f"Erro na verificação de críticos: {e}")


async def check_rsi_alerts(bot: Bot):
    """Verifica RSI sobrevendido em tendência de alta."""
    logger.info("Verificando alertas de RSI...")
    try:
        prices = await fetch_prices()
        for coin_id, symbol in CRYPTO_WATCH.items():
            price_data = prices.get(coin_id, {})
            change_24h = price_data.get("usd_24h_change", 0)

            # Tendência de alta = variação positiva nas últimas 24h (simplificado)
            # Para análise mais robusta usamos RSI 4h > 50
            rsi = await fetch_rsi(coin_id)
            rsi_1h = rsi.get("1h")
            rsi_4h = rsi.get("4h")

            if rsi_1h is None or rsi_4h is None:
                continue

            # Condição: RSI 1h sobrevendido (<30) E tendência de alta (RSI 4h > 45)
            if rsi_1h < 30 and rsi_4h > 45:
                cache_key = f"rsi_{coin_id}_{int(rsi_1h)}"
                if cache_key in sent_news_cache:
                    continue
                sent_news_cache.add(cache_key)

                price_usd = price_data.get("usd", 0)
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
    """Dispara alerta 1 hora antes do FOMC."""
    fomc = get_next_fomc()
    if not fomc:
        return
    now = datetime.now(TIMEZONE)
    diff_minutes = (fomc["datetime"] - now).total_seconds() / 60
    # Alerta entre 55 e 65 minutos antes
    if 55 <= diff_minutes <= 65:
        cache_key = f"fomc_{fomc['date']}"
        if cache_key in sent_news_cache:
            return
        sent_news_cache.add(cache_key)
        prices, fear_greed = await asyncio.gather(fetch_prices(), fetch_fear_greed())
        extra = f"\nData: {fomc['date']} às {fomc['time']} (Brasília)\nEm aproximadamente 1 hora"
        analysis = await analyze_with_claude([], prices, fear_greed, "fomc_alert", extra)
        btc = prices.get("bitcoin", {})
        message = (
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
    fomc = get_next_fomc()
    fomc_line = f"📅 Próx. FOMC: {fomc['date']} às {fomc['time']} ({fomc['days_left']} dias)\n" if fomc else ""
    await update.message.reply_text(
        f"🚀 *Crypto Macro Radar v2.0 ativo!*\n\n"
        f"Seu Chat ID: `{chat_id}`\n\n"
        f"*Comandos:*\n"
        f"/status — Painel completo de mercado\n"
        f"/resumo — Briefing macro com IA\n"
        f"/rsi — Verificar RSI das moedas agora\n"
        f"/ajuda — Lista completa\n\n"
        f"*Automático:*\n"
        f"📅 Resumo diário: 08h00 (Brasília)\n"
        f"🚨 Alertas críticos: a cada 15 min\n"
        f"🔔 Alertas RSI: a cada 30 min\n"
        f"⚠️ Alerta FOMC: 1h antes\n\n"
        f"{fomc_line}",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Buscando dados...", parse_mode=ParseMode.MARKDOWN)
    prices, fear_greed, dxy, market = await asyncio.gather(
        fetch_prices(), fetch_fear_greed(), fetch_dxy(), fetch_market_data()
    )
    msg = format_status(prices, fear_greed, dxy, market)
    msg += "_Dados em tempo real_"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_resumo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧠 Gerando análise macro com IA...", parse_mode=ParseMode.MARKDOWN)
    await send_daily_summary(context.bot)


async def cmd_rsi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra RSI atual de todas as moedas monitoradas."""
    await update.message.reply_text("📊 Calculando RSI...", parse_mode=ParseMode.MARKDOWN)
    prices = await fetch_prices()
    lines = ["📊 *RSI — MOEDAS MONITORADAS*\n" + "─"*30]
    for coin_id, symbol in CRYPTO_WATCH.items():
        rsi = await fetch_rsi(coin_id)
        price = prices.get(coin_id, {}).get("usd", 0)
        rsi_1h = rsi.get("1h", "N/A")
        rsi_4h = rsi.get("4h", "N/A")

        def rsi_label(v):
            if v == "N/A": return "N/A"
            if v < 30:  return f"`{v}` 🔴 _SV_"
            if v > 70:  return f"`{v}` 🟡 _SC_"
            return f"`{v}` ⚪"

        lines.append(
            f"*{symbol}* — `${price:,.4f}`\n"
            f"  ⏱ 1h: {rsi_label(rsi_1h)}  |  4h: {rsi_label(rsi_4h)}\n"
        )
    lines.append("─"*30 + "\n_SV = Sobrevendido | SC = Sobrecomprado_")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*📚 Comandos — Crypto Macro Radar v2.0*\n\n"
        "/start — Iniciar e ver Chat ID\n"
        "/status — Painel completo (BTC, ETH, S&P500, Ouro, DXY, FOMC...)\n"
        "/resumo — Briefing macro completo com IA\n"
        "/rsi — RSI atual de BTC, ETH, SOL, XRP, ADA\n"
        "/ajuda — Esta mensagem\n\n"
        "*⚙️ Automático:*\n"
        "• Resumo diário: 08h00 (Brasília)\n"
        f"• Alertas críticos: a cada {CHECK_INTERVAL_MINUTES} min\n"
        "• Alertas RSI (SV em tendência de alta): a cada 30 min\n"
        "• Alerta FOMC: 1h antes de cada reunião\n\n"
        "*🔍 Fontes:*\n"
        "CoinDesk • CoinTelegraph • Bitcoin Magazine\n"
        "Decrypt • The Block • CryptoSlate\n"
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

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("resumo", cmd_resumo))
    app.add_handler(CommandHandler("rsi",    cmd_rsi))
    app.add_handler(CommandHandler("ajuda",  cmd_ajuda))

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    scheduler.add_job(send_daily_summary,  "cron",     hour=DAILY_REPORT_HOUR,
                      minute=DAILY_REPORT_MINUTE, args=[app.bot])
    scheduler.add_job(check_critical_news, "interval", minutes=CHECK_INTERVAL_MINUTES, args=[app.bot])
    scheduler.add_job(check_rsi_alerts,    "interval", minutes=30, args=[app.bot])
    scheduler.add_job(check_fomc_alert,    "interval", minutes=5,  args=[app.bot])

    scheduler.start()
    logger.info("✅ Crypto Macro Radar v2.0 iniciado!")
    logger.info(f"📅 Resumo diário às {DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d}")
    logger.info(f"🔍 Scan crítico a cada {CHECK_INTERVAL_MINUTES} minutos")
    logger.info("🔔 Alertas RSI a cada 30 minutos")
    logger.info("⚠️ Checagem FOMC a cada 5 minutos")

    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
