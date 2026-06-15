# CRYPTO MACRO RADAR - Bot Telegram v4.0
# Monitoramento macro para traders BTC/ETH/Alts
# Versao: 4.0 - Confluencia de Timeframes + Top 50 Scanner

import asyncio
import logging
import os
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── Configuracoes ───────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CHAT_ID           = os.environ.get("TELEGRAM_CHAT_ID", "")
TIMEZONE          = ZoneInfo("America/Sao_Paulo")

DAILY_REPORT_HOUR      = 9
DAILY_REPORT_MINUTE    = 0
CHECK_INTERVAL_MINUTES = 15

FOMC_DATES = [
    ("2025-07-30", "15:00"), ("2025-09-17", "15:00"),
    ("2025-11-05", "15:00"), ("2025-12-17", "15:00"),
    ("2026-01-28", "15:00"), ("2026-03-18", "15:00"),
    ("2026-04-29", "15:00"), ("2026-06-17", "15:00"),
    ("2026-07-29", "15:00"), ("2026-09-16", "15:00"),
    ("2026-11-04", "15:00"), ("2026-12-16", "15:00"),
]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Fontes RSS ──────────────────────────────────────────────────
FEEDS_CRYPTO = {
    "coindesk":         "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph":    "https://cointelegraph.com/rss",
    "bitcoin_magazine": "https://bitcoinmagazine.com/feed",
    "decrypt":          "https://decrypt.co/feed",
    "theblock":         "https://www.theblock.co/rss.xml",
    "cryptoslate":      "https://cryptoslate.com/feed/",
}

FEEDS_MACRO = {
    "reuters_markets": "https://feeds.reuters.com/reuters/businessNews",
    "axios":           "https://api.axios.com/feed/",
}

FEEDS_INFLUENCERS_FALLBACK = {
    "whitehouse_news": "https://www.whitehouse.gov/feed/",
    "fed_press":       "https://www.federalreserve.gov/feeds/press_all.xml",
}

NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
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

CRYPTO_WATCH = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "solana":   "SOL",
    "ripple":   "XRP",
    "cardano":  "ADA",
}

# Mapeamento CoinGecko -> Binance
COINGECKO_TO_BINANCE = {
    "bitcoin": "BTCUSDT", "ethereum": "ETHUSDT", "tether": "USDTUSD",
    "binancecoin": "BNBUSDT", "solana": "SOLUSDT", "ripple": "XRPUSDT",
    "usd-coin": "USDCUSDT", "dogecoin": "DOGEUSDT", "cardano": "ADAUSDT",
    "avalanche-2": "AVAXUSDT", "shiba-inu": "SHIBUSDT", "polkadot": "DOTUSDT",
    "chainlink": "LINKUSDT", "bitcoin-cash": "BCHUSDT", "near": "NEARUSDT",
    "litecoin": "LTCUSDT", "uniswap": "UNIUSDT", "aptos": "APTUSDT",
    "stellar": "XLMUSDT", "ethereum-classic": "ETCUSDT", "filecoin": "FILUSDT",
    "hedera-hashgraph": "HBARUSDT", "arbitrum": "ARBUSDT", "vechain": "VETUSDT",
    "injective-protocol": "INJUSDT", "sui": "SUIUSDT", "pepe": "PEPEUSDT",
    "maker": "MKRUSDT", "aave": "AAVEUSDT", "matic-network": "MATICUSDT",
    "atom": "ATOMUSDT", "tron": "TRXUSDT", "algorand": "ALGOUSDT",
    "render-token": "RENDERUSDT", "optimism": "OPUSDT", "the-graph": "GRTUSDT",
    "fantom": "FTMUSDT", "the-sandbox": "SANDUSDT", "decentraland": "MANAUSDT",
    "axie-infinity": "AXSUSDT", "flow": "FLOWUSDT", "gala": "GALAUSDT",
}

MAX_TWEET_AGE_HOURS = 720
sent_news_cache: set = set()
_top50_cache = []
_top50_cache_time = None


# ===================================================================
#  BINANCE - CALCULO DE RSI
# ===================================================================

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


async def fetch_binance_klines(symbol, interval, limit=100):
    """Busca candles da Binance."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.debug(f"Binance klines erro {symbol} {interval}: {e}")
        return []


async def fetch_binance_rsi(symbol, interval, limit=100):
    """Calcula RSI via Binance para qualquer timeframe."""
    candles = await fetch_binance_klines(symbol, interval, limit)
    if not candles or len(candles) < 15:
        return None
    closes = [float(c[4]) for c in candles]
    return calc_rsi(closes)


async def fetch_binance_trend(symbol, interval, limit=50):
    """
    Determina tendencia de alta via MM20 e MM50.
    Retorna True se MM20 > MM50 (tendencia de alta).
    """
    candles = await fetch_binance_klines(symbol, interval, limit)
    if not candles or len(candles) < 50:
        return None
    closes = [float(c[4]) for c in candles]
    mm20 = sum(closes[-20:]) / 20
    mm50 = sum(closes[-50:]) / 50
    return mm20 > mm50


async def fetch_binance_pair_trend(symbol_base, interval="1d", limit=50):
    """
    Verifica se a moeda esta em tendencia de alta contra BTC.
    Ex: SOLUSDT e SOLBTC - se SOLBTC subindo, SOL mais forte que BTC.
    """
    btc_pair = symbol_base.replace("USDT", "BTC")
    candles = await fetch_binance_klines(btc_pair, interval, limit)
    if not candles or len(candles) < 20:
        return None
    closes = [float(c[4]) for c in candles]
    mm20 = sum(closes[-20:]) / 20
    mm50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else sum(closes) / len(closes)
    return mm20 > mm50


# ===================================================================
#  TOP 50 SCANNER - MULTI-TIMEFRAME
# ===================================================================

async def fetch_top50():
    """Busca top 50 moedas por market cap. Cache de 6h."""
    global _top50_cache, _top50_cache_time
    now = datetime.now(TIMEZONE)
    if _top50_cache and _top50_cache_time:
        age_h = (now - _top50_cache_time).total_seconds() / 3600
        if age_h < 6:
            return _top50_cache
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 50,
                    "page": 1,
                    "price_change_percentage": "1h,24h,7d,30d",
                    "sparkline": False,
                },
                timeout=15,
            )
            data = resp.json()
            _top50_cache = data
            _top50_cache_time = now
            logger.info(f"Top 50 atualizado: {len(data)} moedas")
            return data
    except Exception as e:
        logger.warning(f"Erro ao buscar top 50: {e}")
        return _top50_cache


async def analyze_coin_multiframe(symbol_usdt):
    """
    Analise completa multi-timeframe de uma moeda.

    FILTRO 1 - Tendencia de alta macro (MM20 > MM50):
      - 1 semana (1w)
      - 1 dia   (1d)
      - 4 horas (4h)

    FILTRO 2 - RSI sobrevendido no curto prazo:
      - 1 hora  (1h) RSI < 30 OBRIGATORIO
      - 15 min  (15m) RSI < 30
      - 5 min   (5m)  RSI < 30
      Regra: 1h obrigatorio + pelo menos 1 dos outros 2

    FILTRO 3 - Forca relativa contra BTC:
      - Par MOEDA/BTC em tendencia de alta (MM20 > MM50 no 1d)

    Retorna dict com resultado ou None se nao passou nos filtros.
    """
    result = {
        "symbol": symbol_usdt,
        "trend_1w": None, "trend_1d": None, "trend_4h": None,
        "rsi_1h": None, "rsi_15m": None, "rsi_5m": None,
        "btc_pair_uptrend": None,
        "passed": False,
        "signal_score": 0,
    }

    # FILTRO 1 - Tendencia macro (busca em paralelo)
    trend_1w, trend_1d, trend_4h = await asyncio.gather(
        fetch_binance_trend(symbol_usdt, "1w", 50),
        fetch_binance_trend(symbol_usdt, "1d", 50),
        fetch_binance_trend(symbol_usdt, "4h", 50),
    )
    result["trend_1w"] = trend_1w
    result["trend_1d"] = trend_1d
    result["trend_4h"] = trend_4h

    # Todos os 3 timeframes macro devem estar em alta
    if not (trend_1w and trend_1d and trend_4h):
        return result

    # FILTRO 2 - RSI curto prazo (busca em paralelo)
    rsi_1h, rsi_15m, rsi_5m = await asyncio.gather(
        fetch_binance_rsi(symbol_usdt, "1h", 100),
        fetch_binance_rsi(symbol_usdt, "15m", 100),
        fetch_binance_rsi(symbol_usdt, "5m", 100),
    )
    result["rsi_1h"]  = rsi_1h
    result["rsi_15m"] = rsi_15m
    result["rsi_5m"]  = rsi_5m

    # 1h obrigatorio em SV + pelo menos 1 dos outros 2
    if rsi_1h is None or rsi_1h >= 30:
        return result

    short_sv_count = sum([
        1 if (rsi_15m and rsi_15m < 30) else 0,
        1 if (rsi_5m and rsi_5m < 30) else 0,
    ])
    if short_sv_count < 1:
        return result

    # FILTRO 3 - Forca relativa contra BTC
    btc_uptrend = await fetch_binance_pair_trend(symbol_usdt, "1d", 50)
    result["btc_pair_uptrend"] = btc_uptrend

    # Calcula score
    score = 3  # Base: passou todos os filtros obrigatorios
    if rsi_15m and rsi_15m < 30:
        score += 1
    if rsi_5m and rsi_5m < 30:
        score += 1
    if btc_uptrend:
        score += 2  # Bonus grande: mais forte que BTC
    if rsi_1h < 20:
        score += 1  # Bonus: RSI extremo

    result["passed"]       = True
    result["signal_score"] = score
    return result


async def scan_top50_opportunities():
    """
    ETAPA 1: Filtra top 50 com dados de mercado (1 chamada rapida)
    ETAPA 2: Analise multi-timeframe completa nas candidatas
    """
    logger.info("Iniciando scan Top 50 multi-timeframe...")
    coins = await fetch_top50()
    if not coins:
        return []

    # ETAPA 1: Pre-filtro rapido por variacao de preco
    # 30d positivo = tendencia macro de alta (proxy rapido)
    candidates = []
    for coin in coins:
        change_30d = coin.get("price_change_percentage_30d_in_currency", 0) or 0
        change_7d  = coin.get("price_change_percentage_7d_in_currency", 0) or 0
        symbol = COINGECKO_TO_BINANCE.get(coin["id"])
        if symbol and change_30d > 0:
            candidates.append({
                "id":         coin["id"],
                "symbol":     coin["symbol"].upper(),
                "name":       coin["name"],
                "price":      coin["current_price"],
                "change_24h": coin.get("price_change_percentage_24h_in_currency", 0) or 0,
                "change_7d":  change_7d,
                "change_30d": change_30d,
                "binance":    symbol,
            })

    logger.info(f"Pre-filtro: {len(candidates)}/{len(coins)} candidatas")

    # ETAPA 2: Analise multi-timeframe completa
    opportunities = []
    for coin in candidates:
        await asyncio.sleep(0.5)  # Rate limit Binance
        analysis = await analyze_coin_multiframe(coin["binance"])

        if not analysis["passed"]:
            continue

        coin.update({
            "trend_1w":        analysis["trend_1w"],
            "trend_1d":        analysis["trend_1d"],
            "trend_4h":        analysis["trend_4h"],
            "rsi_1h":          analysis["rsi_1h"],
            "rsi_15m":         analysis["rsi_15m"],
            "rsi_5m":          analysis["rsi_5m"],
            "btc_pair_uptrend": analysis["btc_pair_uptrend"],
            "signal_score":    analysis["signal_score"],
        })

        # Define forca do sinal
        score = analysis["signal_score"]
        if score >= 7:
            coin["signal_strength"] = "🔴 FORTE"
        elif score >= 5:
            coin["signal_strength"] = "🟠 MEDIO"
        else:
            coin["signal_strength"] = "🟡 FRACO"

        opportunities.append(coin)
        logger.info(f"Setup: {coin['symbol']} score={score} RSI1h={analysis['rsi_1h']} BTC_par={'alta' if analysis['btc_pair_uptrend'] else 'baixa'}")

    opportunities.sort(key=lambda x: x["signal_score"], reverse=True)
    logger.info(f"Scan concluido: {len(opportunities)} oportunidades")
    return opportunities


def format_opportunity_list(opportunities, now):
    """Formata lista de oportunidades para o Telegram."""
    if not opportunities:
        return (
            f"📋 *SCANNER TOP 50 — {now.strftime('%d/%m/%Y %H:%M')}*\n"
            f"{'─'*30}\n"
            f"Nenhum setup encontrado no momento.\n"
            f"_Aguardando confluencia: tendencia macro + RSI SV + forca vs BTC_"
        )

    lines = [
        f"📋 *SCANNER TOP 50 — {now.strftime('%d/%m/%Y %H:%M')}*",
        f"_1w+1d+4h em alta | 1h+15m/5m SV | Forca vs BTC_",
        f"{'─'*30}",
    ]

    for i, coin in enumerate(opportunities[:10], 1):
        trend_btc = "✅ Alta vs BTC" if coin.get("btc_pair_uptrend") else "⚠️ Fraca vs BTC"
        rsi_15m_str = f"`{coin['rsi_15m']}`" if coin.get("rsi_15m") else "N/A"
        rsi_5m_str  = f"`{coin['rsi_5m']}`"  if coin.get("rsi_5m")  else "N/A"

        lines.append(
            f"{i}. {coin['signal_strength']} *{coin['symbol']}* — `${coin['price']:,.4f}`\n"
            f"   📈 Tendencia: 1w✅ 1d✅ 4h✅\n"
            f"   ⚡ RSI: 1h:`{coin['rsi_1h']}` | 15m:{rsi_15m_str} | 5m:{rsi_5m_str}\n"
            f"   {trend_btc} | Score: `{coin['signal_score']}`\n"
        )

    lines.append("─"*30)
    lines.append(f"_Total: {len(opportunities)} setups | Score max: {opportunities[0]['signal_score']}_")
    lines.append("⚠️ _Confirme no grafico antes de entrar!_")
    return "\n".join(lines)


async def send_morning_scanner(bot):
    """Envia lista matinal de oportunidades."""
    logger.info("Gerando scanner matinal Top 50...")
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


async def check_realtime_opportunities(bot):
    """Roda a cada 1h e alerta sobre setups FORTES."""
    logger.info("Verificando oportunidades em tempo real...")
    try:
        opportunities = await scan_top50_opportunities()
        strong = [o for o in opportunities if o["signal_score"] >= 7]

        for coin in strong:
            cache_key = f"opp_{coin['id']}_{int(coin['rsi_1h'])}"
            if cache_key in sent_news_cache:
                continue
            sent_news_cache.add(cache_key)

            now = datetime.now(TIMEZONE)
            trend_btc = "✅ Mais forte que BTC" if coin.get("btc_pair_uptrend") else "⚠️ Fraca vs BTC"
            rsi_15m_str = f"`{coin['rsi_15m']}`" if coin.get("rsi_15m") else "N/A"
            rsi_5m_str  = f"`{coin['rsi_5m']}`"  if coin.get("rsi_5m")  else "N/A"

            msg = (
                f"🚨 *SETUP DETECTADO — {coin['symbol']}*\n"
                f"{'─'*30}\n"
                f"*Confluencia multi-timeframe confirmada*\n"
                f"{'─'*30}\n"
                f"💰 *Preco:* `${coin['price']:,.4f}`\n"
                f"📈 *Tendencia macro:* 1w ✅ | 1d ✅ | 4h ✅\n"
                f"⚡ *RSI curto prazo:*\n"
                f"   1h: `{coin['rsi_1h']}` _(SV obrigatorio)_\n"
                f"   15m: {rsi_15m_str} | 5m: {rsi_5m_str}\n"
                f"📊 *Forca relativa:* {trend_btc}\n"
                f"⭐ *Score:* `{coin['signal_score']}/9`\n"
                f"{'─'*30}\n"
                f"🎯 _Verifique o grafico para confirmar entrada_\n"
                f"🕐 _{now.strftime('%d/%m/%Y %H:%M')} (Brasilia)_\n"
                f"⚠️ _Nao e recomendacao de investimento_"
            )
            await bot.send_message(
                chat_id=CHAT_ID,
                text=msg[:4096],
                parse_mode=ParseMode.MARKDOWN,
            )
            logger.info(f"Alerta enviado: {coin['symbol']} score={coin['signal_score']}")
            await asyncio.sleep(3)
    except Exception as e:
        logger.error(f"Erro no check de oportunidades: {e}")


# ===================================================================
#  NITTER RSS
# ===================================================================

def _parse_pubdate(pub):
    if not pub:
        return None
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y-%m-%dT%H:%M:%S%z",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(pub.strip(), fmt)
            return dt
        except ValueError:
            continue
    return None


def _is_fresh(pub):
    dt = _parse_pubdate(pub)
    if dt is None:
        return True
    try:
        now_utc = datetime.now(dt.tzinfo)
        age_hours = (now_utc - dt).total_seconds() / 3600
        return age_hours <= MAX_TWEET_AGE_HOURS
    except Exception:
        return True


async def _check_nitter_health(instance, client):
    try:
        resp = await client.get(instance, timeout=5, follow_redirects=True)
        return resp.status_code == 200
    except Exception:
        return False


async def fetch_rss(url, client, source_type="news"):
    try:
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
                    "title": title, "link": link,
                    "description": desc, "pubDate": pub,
                    "source_type": source_type,
                })
        return items[:10]
    except Exception as e:
        logger.warning(f"Erro ao buscar {url}: {e}")
        return []


async def fetch_nitter_account(handle, username, client):
    for instance in NITTER_INSTANCES:
        is_online = await _check_nitter_health(instance, client)
        if not is_online:
            continue
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
                if title and title != handle:
                    items.append({
                        "title": title, "link": link,
                        "description": desc, "pubDate": pub,
                        "source_type": "tweet",
                        "twitter_handle": handle,
                    })
            if not items:
                continue
            most_recent_pub = items[0].get("pubDate", "")
            if not _is_fresh(most_recent_pub):
                continue
            dt = _parse_pubdate(most_recent_pub)
            age_h = round((datetime.now(dt.tzinfo) - dt).total_seconds() / 3600, 1) if dt else "?"
            logger.info(f"✅ {handle} via {instance}: {len(items)} posts (ultimo ha {age_h}h)")
            return items[:5]
        except Exception as e:
            logger.warning(f"Nitter {instance} erro para {handle}: {e}")
            continue
    return []


async def fetch_influencer_fallback(client):
    items = []
    for name, url in FEEDS_INFLUENCERS_FALLBACK.items():
        try:
            raw = await fetch_rss(url, client, "influencer")
            for item in raw:
                item["fallback_source"] = name
            items.extend(raw)
        except Exception as e:
            logger.warning(f"Fallback {name} falhou: {e}")
    return items


async def fetch_all_news():
    all_items  = []
    tweet_count = 0
    headers    = {"User-Agent": "CryptoMacroBot/4.0"}
    async with httpx.AsyncClient(headers=headers) as client:
        crypto_tasks = [fetch_rss(url, client, "news")  for url in FEEDS_CRYPTO.values()]
        macro_tasks  = [fetch_rss(url, client, "macro") for url in FEEDS_MACRO.values()]
        tweet_tasks  = [
            fetch_nitter_account(handle, username, client)
            for handle, username in TWITTER_ACCOUNTS.items()
        ]
        results = await asyncio.gather(*crypto_tasks, *macro_tasks, *tweet_tasks, return_exceptions=True)
        for items in results:
            if isinstance(items, list):
                all_items.extend(items)
                if items and items[0].get("source_type") == "tweet":
                    tweet_count += len(items)
        if tweet_count == 0:
            logger.warning("Nitter indisponivel — ativando fallback...")
            fallback = await fetch_influencer_fallback(client)
            all_items.extend(fallback)
    logger.info(f"Total itens coletados: {len(all_items)} (tweets: {tweet_count})")
    return all_items


# ===================================================================
#  DADOS DE MERCADO
# ===================================================================

async def fetch_prices():
    ids = ",".join(list(CRYPTO_WATCH.keys()) + ["bitcoin", "ethereum"])
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=usd,brl"
        f"&include_24hr_change=true&include_market_cap=true"
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            return resp.json()
    except Exception as e:
        logger.warning(f"Erro ao buscar precos: {e}")
        return {}


async def fetch_fear_greed():
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("https://api.alternative.me/fng/?limit=1", timeout=10)
            data = resp.json()
            return data["data"][0] if data.get("data") else {}
    except Exception:
        return {}


async def fetch_dxy():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=2d"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            data = resp.json()
            closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            return closes[-1]
    except Exception:
        return None


async def fetch_market_data():
    symbols = {"sp500": "^GSPC", "gold": "GC=F", "oil": "CL=F", "usdbrl": "BRL=X"}
    result = {}
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        for key, symbol in symbols.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2d"
                resp  = await client.get(url, timeout=10)
                data  = resp.json()
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
            result["btc_dominance"] = round(data["data"]["market_cap_percentage"].get("btc", 0), 1)
    except Exception:
        result["btc_dominance"] = None
    return result


# ===================================================================
#  FILTRO IA
# ===================================================================

async def filter_relevant_items(items, prices):
    if not items or not ANTHROPIC_API_KEY:
        return items
    btc_usd    = prices.get("bitcoin", {}).get("usd", 0)
    btc_change = prices.get("bitcoin", {}).get("usd_24h_change", 0)
    items_text = ""
    for i, item in enumerate(items):
        source = item.get("twitter_handle", item.get("source_type", "news"))
        items_text += f"{i+1}. [{source}] {item['title']}\n"
    prompt = f"""Voce e um filtro de relevancia para traders de Bitcoin e crypto.
CONTEXTO: BTC ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
LISTA:
{items_text}
Score 0-10 por impacto real no mercado crypto. 9-10=impacto imediato, 7-8=significativo, 0-6=descartar.
Responda APENAS JSON: {{"scores": [{{"id": 1, "score": 8}}]}}"""
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
                    "max_tokens": 512,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=30,
            )
            resp.raise_for_status()
            data    = resp.json()
            content = data["content"][0]["text"].strip()
            content = re.sub(r"```json|```", "", content).strip()
            result  = json.loads(content)
            scores  = {s["id"]: s["score"] for s in result.get("scores", [])}
            filtered = []
            for i, item in enumerate(items):
                score = scores.get(i + 1, 0)
                if score >= 7:
                    item["relevance_score"] = score
                    filtered.append(item)
            logger.info(f"Filtro IA: {len(filtered)}/{len(items)} relevantes")
            return filtered
    except Exception as e:
        logger.warning(f"Erro no filtro IA: {e}")
        return items


# ===================================================================
#  ANALISE COM IA
# ===================================================================

async def analyze_with_claude(news_items, prices, fear_greed, report_type="summary", extra_context=""):
    if not ANTHROPIC_API_KEY:
        return "ANTHROPIC_API_KEY nao configurada."
    btc_usd    = prices.get("bitcoin", {}).get("usd", 0)
    btc_change = prices.get("bitcoin", {}).get("usd_24h_change", 0)
    eth_usd    = prices.get("ethereum", {}).get("usd", 0)
    eth_change = prices.get("ethereum", {}).get("usd_24h_change", 0)
    fg_value   = fear_greed.get("value", "N/A")
    fg_class   = fear_greed.get("value_classification", "N/A")
    news_text  = "\n".join([f"- {n['title']}" for n in news_items[:15]])

    if report_type == "critical":
        system_prompt = "Voce e um analista senior de mercado crypto. Alerte traders sobre eventos criticos. Seja direto. Responda em portugues brasileiro. Use Markdown do Telegram."
        user_prompt = f"""EVENTO CRITICO:
NOTICIA: {news_items[0]['title']}
BTC: ${btc_usd:,.0f} ({btc_change:+.1f}%) | ETH: ${eth_usd:,.0f} ({eth_change:+.1f}%) | F&G: {fg_value}/100
Em 3-5 linhas: o que aconteceu, por que importa para crypto, tendencia provavel."""

    elif report_type == "tweet":
        system_prompt = "Voce e um analista senior de mercado crypto. Analise impacto de tweets de figuras influentes. Responda em portugues brasileiro. Use Markdown do Telegram."
        user_prompt = f"""TWEET DE FIGURA INFLUENTE:
Autor: {news_items[0].get('twitter_handle', 'Desconhecido')}
Post: {news_items[0]['title']}
BTC: ${btc_usd:,.0f} ({btc_change:+.1f}%) | F&G: {fg_value}/100
Em 3-4 linhas: por que importa para crypto e impacto esperado."""

    elif report_type == "tweet_translation":
        system_prompt = "Voce e um tradutor. Traduza para portugues brasileiro de forma natural."
        user_prompt = f'Traduza: "{news_items[0]["title"]}"\nResposta APENAS com a traducao.'

    elif report_type == "fomc_alert":
        system_prompt = "Voce e um analista macro especializado no Fed. Prepare traders para o FOMC. Responda em portugues brasileiro."
        user_prompt = f"""ALERTA PRE-FOMC — em 1 hora!
BTC: ${btc_usd:,.0f} ({btc_change:+.1f}%) | ETH: ${eth_usd:,.0f} ({eth_change:+.1f}%) | F&G: {fg_value}/100
{extra_context}
Em 4-5 linhas: o que esperar, como crypto pode reagir, dica pratica."""

    else:
        system_prompt = "Voce e um analista senior de crypto especializado em macro global. Faca briefings matinais para traders. Responda em portugues brasileiro. Use Markdown do Telegram."
        user_prompt = f"""BRIEFING MACRO DIARIO:
BTC: ${btc_usd:,.0f} ({btc_change:+.1f}%) | ETH: ${eth_usd:,.0f} ({eth_change:+.1f}%) | F&G: {fg_value}/100 ({fg_class})
{extra_context}
NOTICIAS:
{news_text}
Estruture:
1. Macro Global (Fed, juros, geopolitica)
2. Impacto esperado no Crypto
3. Pontos de atencao para hoje
4. Vies de mercado (altista/baixista/neutro) com stop sugerido
Maximo 400 palavras."""

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
            return "Resposta invalida da IA."
    except httpx.HTTPStatusError as e:
        logger.error(f"Erro HTTP API Claude: {e.response.status_code}")
        return f"Erro na API ({e.response.status_code}). Verifique ANTHROPIC_API_KEY."
    except Exception as e:
        logger.error(f"Erro na API Claude: {e}")
        return f"Erro ao consultar IA: {e}"


# ===================================================================
#  FORMATACAO
# ===================================================================

def emoji_change(val):
    return "🟢" if val >= 0 else "🔴"


def get_next_fomc():
    now = datetime.now(TIMEZONE)
    for date_str, hour_str in FOMC_DATES:
        fomc_dt = datetime.strptime(f"{date_str} {hour_str}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
        if fomc_dt > now:
            return {
                "date": fomc_dt.strftime("%d/%m/%Y"),
                "time": hour_str,
                "days_left": (fomc_dt - now).days,
                "datetime": fomc_dt,
            }
    return None


def format_status(prices, fear_greed, dxy, market):
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
    oil_line  = f"{emoji_change(oil['change'])} *Petroleo WTI:* `${oil['price']:.2f}` ({oil['change']:+.1f}%)\n" if oil else ""
    brl_line  = f"🇧🇷 *USD/BRL:* `R$ {usdbrl['price']:.2f}`\n" if usdbrl else ""
    dom_line  = f"📈 *Dominancia BTC:* `{btc_dom}%`\n" if btc_dom else ""
    if fomc:
        if fomc["days_left"] == 0:
            fomc_line = f"🔴 *Prox. FOMC:* `HOJE as {fomc['time']} (Brasilia)`\n"
        elif fomc["days_left"] == 1:
            fomc_line = f"🟡 *Prox. FOMC:* `AMANHA — {fomc['date']} as {fomc['time']}`\n"
        else:
            fomc_line = f"📅 *Prox. FOMC:* `{fomc['date']} as {fomc['time']}` _({fomc['days_left']} dias)_\n"
    else:
        fomc_line = ""
    return (
        f"📡 *CRYPTO MACRO RADAR*\n"
        f"🕐 _{now} (Brasilia)_\n"
        f"{'─'*30}\n"
        f"*BTC CRYPTO*\n"
        f"{emoji_change(btc.get('usd_24h_change',0))} *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
        f"{emoji_change(eth.get('usd_24h_change',0))} *ETH:* `${eth.get('usd',0):,.0f}` ({eth.get('usd_24h_change',0):+.1f}%)\n"
        f"😱 *Fear & Greed:* `{fg_val}/100` _{fg_cls}_\n"
        f"{dom_line}"
        f"{'─'*30}\n"
        f"*MERCADO TRADICIONAL*\n"
        f"{sp_line}{gold_line}{oil_line}"
        f"{'─'*30}\n"
        f"*MACRO GLOBAL*\n"
        f"{'💵 *DXY:* `' + f'{dxy:.2f}` ' + dxy_desc + chr(10) if dxy else ''}"
        f"{brl_line}"
        f"🏦 *Fed Juros:* `4.25% — 4.50%`\n"
        f"{fomc_line}"
        f"{'─'*30}\n"
    )


def format_price_header(prices, fear_greed, dxy):
    btc    = prices.get("bitcoin", {})
    eth    = prices.get("ethereum", {})
    fg_val = fear_greed.get("value", "?")
    fg_cls = fear_greed.get("value_classification", "?")
    now    = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")
    dxy_str = f"💵 *DXY:* `{dxy:.2f}`\n" if dxy else ""
    return (
        f"📡 *CRYPTO MACRO RADAR*\n"
        f"🕐 _{now} (Brasilia)_\n"
        f"{'─'*30}\n"
        f"{emoji_change(btc.get('usd_24h_change',0))} *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
        f"{emoji_change(eth.get('usd_24h_change',0))} *ETH:* `${eth.get('usd',0):,.0f}` ({eth.get('usd_24h_change',0):+.1f}%)\n"
        f"😱 *Fear & Greed:* `{fg_val}/100` _{fg_cls}_\n"
        f"{dxy_str}"
        f"{'─'*30}\n"
    )


# ===================================================================
#  TAREFAS AGENDADAS
# ===================================================================

async def send_daily_summary(bot):
    logger.info("Gerando resumo diario...")
    try:
        news, prices, fear_greed, dxy, market = await asyncio.gather(
            fetch_all_news(), fetch_prices(), fetch_fear_greed(), fetch_dxy(), fetch_market_data(),
        )
        fomc  = get_next_fomc()
        extra = ""
        if fomc:
            extra = f"\nFOMC: {fomc['date']} as {fomc['time']} (daqui {fomc['days_left']} dias)"
        sp = market.get("sp500")
        if sp:
            extra += f"\nS&P 500: {sp['price']:,.0f} ({sp['change']:+.1f}%)"
        news_only = [n for n in news if n.get("source_type") in ("news", "macro")]
        header    = format_status(prices, fear_greed, dxy, market)
        analysis  = await analyze_with_claude(news_only, prices, fear_greed, "summary", extra)
        await bot.send_message(chat_id=CHAT_ID, text=(header + analysis)[:4096], parse_mode=ParseMode.MARKDOWN)
        logger.info("Resumo diario enviado.")
    except Exception as e:
        logger.error(f"Erro no resumo diario: {e}")


async def check_critical_news(bot):
    logger.info("Verificando noticias criticas...")
    try:
        all_items = await fetch_all_news()
        new_items = []
        for item in all_items:
            uid = item.get("link") or item.get("title", "")[:80]
            if uid not in sent_news_cache:
                new_items.append(item)
        if not new_items:
            return
        prices    = await fetch_prices()
        relevant  = await filter_relevant_items(new_items, prices)
        if not relevant:
            return
        fear_greed = await fetch_fear_greed()
        for item in relevant:
            uid = item.get("link") or item.get("title", "")[:80]
            sent_news_cache.add(uid)
            if len(sent_news_cache) > 500:
                sent_news_cache.clear()
            is_tweet = item.get("source_type") == "tweet"
            if is_tweet:
                handle     = item.get("twitter_handle", "@desconhecido")
                tweet_text = item["title"]
                translation = await analyze_with_claude([item], prices, fear_greed, "tweet_translation")
                analysis    = await analyze_with_claude([item], prices, fear_greed, "tweet")
                btc = prices.get("bitcoin", {})
                now = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")
                score = item.get("relevance_score", "?")
                msg = (
                    f"🐦 *TWEET — {handle}*\n{'─'*30}\n"
                    f"🇺🇸 *Original:*\n_{tweet_text}_\n\n"
                    f"🇧🇷 *Traducao:*\n_{translation}_\n"
                    f"{'─'*30}\n"
                    f"{emoji_change(btc.get('usd_24h_change',0))} *BTC:* `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n"
                    f"🕐 _{now} (Brasilia)_\n{'─'*30}\n"
                    f"🧠 *Impacto esperado:*\n{analysis}\n\n"
                    f"🔗 [Ver post]({item.get('link','#')})\n"
                    f"⭐ _Relevancia: {score}/10_"
                )
            else:
                score    = item.get("relevance_score", "?")
                analysis = await analyze_with_claude([item], prices, fear_greed, "critical")
                header   = format_price_header(prices, fear_greed, None)
                msg = (
                    f"🚨 *ALERTA CRITICO*\n{'─'*30}\n"
                    f"📰 *{item['title']}*\n{'─'*30}\n"
                    + header + analysis
                    + f"\n\n🔗 [Ver noticia]({item.get('link','#')})\n"
                    + f"⭐ _Relevancia: {score}/10_"
                )
            await bot.send_message(chat_id=CHAT_ID, text=msg[:4096], parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
            await asyncio.sleep(5)
    except Exception as e:
        logger.error(f"Erro na verificacao critica: {e}")


async def check_rsi_alerts(bot):
    logger.info("Verificando alertas de RSI...")
    try:
        prices = await fetch_prices()
        for coin_id, symbol in CRYPTO_WATCH.items():
            price_data = prices.get(coin_id, {})
            change_24h = price_data.get("usd_24h_change", 0)
            bsymbol    = COINGECKO_TO_BINANCE.get(coin_id)
            if bsymbol:
                rsi_1h = await fetch_binance_rsi(bsymbol, "1h")
                rsi_4h = await fetch_binance_rsi(bsymbol, "4h")
            else:
                continue
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
                    f"Moeda: {symbol}/USDT\nPreco: ${price_usd:,.4f}\n"
                    f"RSI 1h: {rsi_1h} (SOBREVENDIDO)\nRSI 4h: {rsi_4h}\nVariacao 24h: {change_24h:+.1f}%"
                )
                analysis = await analyze_with_claude([], prices, fear_greed, "critical", extra)
                msg = (
                    f"🔔 *ALERTA TECNICO — {symbol}*\n{'─'*30}\n"
                    f"📈 Tendencia de Alta + RSI Sobrevendido\n{'─'*30}\n"
                    f"💰 *Preco:* `${price_usd:,.4f}`\n"
                    f"⏱ *RSI 1h:* `{rsi_1h}` _(Sobrevendido)_\n"
                    f"⏱ *RSI 4h:* `{rsi_4h}` _(Tendencia de Alta)_\n"
                    f"📊 *24h:* `{change_24h:+.1f}%`\n{'─'*30}\n"
                    + analysis + "\n\n⚠️ _Nao e recomendacao de investimento_"
                )
                await bot.send_message(chat_id=CHAT_ID, text=msg[:4096], parse_mode=ParseMode.MARKDOWN)
                await asyncio.sleep(3)
    except Exception as e:
        logger.error(f"Erro nos alertas de RSI: {e}")


async def check_fomc_alert(bot):
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
        extra    = f"\nData: {fomc['date']} as {fomc['time']} (Brasilia)\nEm aproximadamente 1 hora"
        analysis = await analyze_with_claude([], prices, fear_greed, "fomc_alert", extra)
        btc      = prices.get("bitcoin", {})
        msg = (
            f"⚠️ *ALERTA FOMC — EM 1 HORA!*\n{'─'*30}\n"
            f"🏦 Fed anuncia decisao de juros\n"
            f"🕐 Hoje as *{fomc['time']}* (Brasilia)\n{'─'*30}\n"
            f"BTC: `${btc.get('usd',0):,.0f}` ({btc.get('usd_24h_change',0):+.1f}%)\n{'─'*30}\n"
            + analysis + "\n\n🔴 _Prepare-se para volatilidade!_"
        )
        await bot.send_message(chat_id=CHAT_ID, text=msg[:4096], parse_mode=ParseMode.MARKDOWN)


# ===================================================================
#  COMANDOS DO BOT
# ===================================================================

async def cmd_start(update, context):
    chat_id = update.effective_chat.id
    fomc    = get_next_fomc()
    fomc_line = f"📅 Prox. FOMC: {fomc['date']} as {fomc['time']} ({fomc['days_left']} dias)\n" if fomc else ""
    await update.message.reply_text(
        f"🚀 *Crypto Macro Radar v4.0 ativo!*\n\n"
        f"Chat ID: `{chat_id}`\n\n"
        f"*Comandos:*\n"
        f"/status — Painel completo\n"
        f"/resumo — Briefing macro com IA\n"
        f"/scanner — Scanner Top 50 agora\n"
        f"/rsi — RSI das moedas monitoradas\n"
        f"/ajuda — Lista completa\n\n"
        f"*Automatico:*\n"
        f"📅 Resumo macro: 09h00\n"
        f"📋 Scanner Top 50: 09h05\n"
        f"🚨 Alertas filtrados por IA: a cada 15 min\n"
        f"🔔 Oportunidades multi-timeframe: a cada 1h\n"
        f"⚠️ Alerta FOMC: 1h antes\n\n"
        f"{fomc_line}",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update, context):
    await update.message.reply_text("⏳ Buscando dados...", parse_mode=ParseMode.MARKDOWN)
    prices, fear_greed, dxy, market = await asyncio.gather(
        fetch_prices(), fetch_fear_greed(), fetch_dxy(), fetch_market_data()
    )
    msg = format_status(prices, fear_greed, dxy, market) + "_Dados em tempo real_"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_resumo(update, context):
    await update.message.reply_text("🧠 Gerando analise macro com IA...", parse_mode=ParseMode.MARKDOWN)
    await send_daily_summary(context.bot)


async def cmd_scanner(update, context):
    await update.message.reply_text(
        "🔍 *Rodando scanner Top 50...*\n_Analise multi-timeframe em andamento. Aguarde ate 3 minutos._",
        parse_mode=ParseMode.MARKDOWN
    )
    now           = datetime.now(TIMEZONE)
    opportunities = await scan_top50_opportunities()
    msg           = format_opportunity_list(opportunities, now)
    await update.message.reply_text(msg[:4096], parse_mode=ParseMode.MARKDOWN)


async def cmd_rsi(update, context):
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
        price   = prices.get(coin_id, {}).get("usd", 0)
        bsymbol = COINGECKO_TO_BINANCE.get(coin_id)
        if bsymbol:
            rsi_1h, rsi_4h, rsi_1d = await asyncio.gather(
                fetch_binance_rsi(bsymbol, "1h"),
                fetch_binance_rsi(bsymbol, "4h"),
                fetch_binance_rsi(bsymbol, "1d"),
            )
        else:
            rsi_1h = rsi_4h = rsi_1d = None
        lines.append(
            f"*{symbol}* — `${price:,.4f}`\n"
            f"  1h: {rsi_label(rsi_1h)} | 4h: {rsi_label(rsi_4h)} | 1d: {rsi_label(rsi_1d)}\n"
        )
    lines.append("─"*30 + "\n_SV=Sobrevendido | SC=Sobrecomprado | <20=Extremo_")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_ajuda(update, context):
    await update.message.reply_text(
        "*Comandos — Crypto Macro Radar v4.0*\n\n"
        "/start — Iniciar\n"
        "/status — Painel completo de mercado\n"
        "/resumo — Briefing macro com IA\n"
        "/scanner — Scanner Top 50 (multi-timeframe)\n"
        "/rsi — RSI atual via Binance (1h, 4h, 1d)\n"
        "/ajuda — Esta mensagem\n\n"
        "*Automatico:*\n"
        "• Resumo macro: 09h00\n"
        "• Scanner Top 50: 09h05\n"
        "• Alertas IA: a cada 15 min\n"
        "• Oportunidades multi-timeframe: a cada 1h\n"
        "• FOMC: 1h antes\n\n"
        "*Scanner busca:*\n"
        "✅ Tendencia 1w + 1d + 4h em alta (MM20>MM50)\n"
        "✅ RSI 1h sobrevendido (<30) + 15m ou 5m SV\n"
        "✅ Moeda mais forte que BTC (par MOEDA/BTC em alta)\n\n"
        "*Twitter monitorado:*\n"
        "Trump | Elon | WhiteHouse | Fed | SEC | Saylor | Cathie Wood",
        parse_mode=ParseMode.MARKDOWN,
    )


# ===================================================================
#  INICIALIZACAO
# ===================================================================

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN nao configurado!")
    if not CHAT_ID:
        raise ValueError("TELEGRAM_CHAT_ID nao configurado!")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("resumo",  cmd_resumo))
    app.add_handler(CommandHandler("scanner", cmd_scanner))
    app.add_handler(CommandHandler("rsi",     cmd_rsi))
    app.add_handler(CommandHandler("ajuda",   cmd_ajuda))

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(send_daily_summary,          "cron",     hour=9,  minute=0,  args=[app.bot])
    scheduler.add_job(send_morning_scanner,         "cron",     hour=9,  minute=5,  args=[app.bot])
    scheduler.add_job(check_critical_news,          "interval", minutes=CHECK_INTERVAL_MINUTES, args=[app.bot])
    scheduler.add_job(check_realtime_opportunities, "interval", minutes=60, args=[app.bot])
    scheduler.add_job(check_rsi_alerts,             "interval", minutes=30, args=[app.bot])
    scheduler.add_job(check_fomc_alert,             "interval", minutes=5,  args=[app.bot])
    scheduler.start()

    logger.info("✅ Crypto Macro Radar v4.0 iniciado!")
    logger.info("📅 Resumo diario as 09:00")
    logger.info("📋 Scanner Top 50 as 09:05 + alertas a cada 1h")
    logger.info(f"🔍 Scan + filtro IA a cada {CHECK_INTERVAL_MINUTES} minutos")
    logger.info("🐦 Monitorando: Trump, Elon, WhiteHouse, Fed, SEC, Saylor, Cathie Wood")

    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
