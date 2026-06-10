"""
╔══════════════════════════════════════════════════════╗
║         CRYPTO MACRO RADAR - Bot Telegram            ║
║   Monitoramento macro para traders BTC/ETH/Alts      ║
╚══════════════════════════════════════════════════════╝

Autor: Gerado por Claude (Anthropic)
Versão: 1.0
"""

import asyncio
import logging
import os
import json
import httpx
from datetime import datetime, time
from zoneinfo import ZoneInfo

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── Configurações ───────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CHAT_ID           = os.environ.get("TELEGRAM_CHAT_ID", "")   # seu chat_id pessoal
TIMEZONE          = ZoneInfo("America/Sao_Paulo")

# Horário do resumo diário (horário de Brasília)
DAILY_REPORT_HOUR   = 8   # 08:00 AM
DAILY_REPORT_MINUTE = 0

# Intervalo de checagem de eventos críticos (em minutos)
CHECK_INTERVAL_MINUTES = 15

# ─── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Fontes de dados (APIs gratuitas) ────────────────────────────
FEEDS = {
    "coindesk":        "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph":   "https://cointelegraph.com/rss",
    "bitcoin_magazine":"https://bitcoinmagazine.com/feed",
    "decrypt":         "https://decrypt.co/feed",
    "theblock":        "https://www.theblock.co/rss.xml",
    "cryptoslate":     "https://cryptoslate.com/feed/",
}

# Palavras-chave que disparam alerta CRÍTICO imediato
CRITICAL_KEYWORDS = [
    # Macro EUA
    "fed rate", "federal reserve", "interest rate", "fomc", "powell",
    "inflation", "cpi", "pce", "unemployment", "nonfarm payroll",
    "recession", "gdp", "treasury",
    # Geopolítica
    "war", "guerra", "sanction", "sanção", "nuclear", "missile",
    "china", "taiwan", "russia", "ukraine", "iran", "north korea",
    "conflict", "attack", "crisis",
    # Crypto direto
    "bitcoin", "btc", "ethereum", "eth", "crypto", "sec", "etf",
    "hack", "exploit", "blackrock", "coinbase", "binance",
    # Nomes de peso
    "trump", "biden", "powell", "lagarde", "yellen", "xi jinping",
    "elon musk", "michael saylor",
]

# Cache simples para não reenviar a mesma notícia
sent_news_cache: set[str] = set()


# ═══════════════════════════════════════════════════════════════════
#  FUNÇÕES DE COLETA DE DADOS
# ═══════════════════════════════════════════════════════════════════

async def fetch_rss(url: str, client: httpx.AsyncClient) -> list[dict]:
    """Busca e parseia um feed RSS retornando lista de itens."""
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
                items.append({"title": title, "link": link,
                               "description": desc, "pubDate": pub})
        return items[:10]  # últimas 10
    except Exception as e:
        logger.warning(f"Erro ao buscar {url}: {e}")
        return []


async def fetch_all_news() -> list[dict]:
    """Agrega notícias de todas as fontes."""
    all_items = []
    async with httpx.AsyncClient(headers={"User-Agent": "CryptoMacroBot/1.0"}) as client:
        tasks = [fetch_rss(url, client) for url in FEEDS.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for items in results:
            if isinstance(items, list):
                all_items.extend(items)
    return all_items


async def fetch_prices() -> dict:
    """Busca preços atuais via CoinGecko (gratuito, sem API key)."""
    url = (
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin,ethereum&vs_currencies=usd,brl"
        "&include_24hr_change=true&include_market_cap=true"
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            return resp.json()
    except Exception as e:
        logger.warning(f"Erro ao buscar preços: {e}")
        return {}


async def fetch_fear_greed() -> dict:
    """Busca índice Fear & Greed do mercado crypto."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("https://api.alternative.me/fng/?limit=1", timeout=10)
            data = resp.json()
            return data["data"][0] if data.get("data") else {}
    except Exception:
        return {}


async def fetch_dxy() -> float | None:
    """Tenta buscar DXY (índice do dólar) via Yahoo Finance."""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=2d"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10,
                                     headers={"User-Agent": "Mozilla/5.0"})
            data = resp.json()
            closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            return closes[-1]
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
#  ANÁLISE COM IA (Claude via Anthropic API)
# ═══════════════════════════════════════════════════════════════════

async def analyze_with_claude(news_items: list[dict], prices: dict,
                               fear_greed: dict, report_type: str = "summary") -> str:
    """
    Envia contexto para o Claude e recebe análise em português.
    report_type: 'summary' (diário) | 'critical' (alerta pontual)
    """
    if not ANTHROPIC_API_KEY:
        return "⚠️ ANTHROPIC_API_KEY não configurada. Configure para ativar análise com IA."

    # Monta contexto de preços
    btc_usd = prices.get("bitcoin", {}).get("usd", "N/A")
    btc_change = prices.get("bitcoin", {}).get("usd_24h_change", 0)
    eth_usd = prices.get("ethereum", {}).get("usd", "N/A")
    eth_change = prices.get("ethereum", {}).get("usd_24h_change", 0)
    fg_value = fear_greed.get("value", "N/A")
    fg_class = fear_greed.get("value_classification", "N/A")

    # Monta lista de notícias
    news_text = "\n".join(
        [f"- [{i+1}] {n['title']}" for i, n in enumerate(news_items[:15])]
    )

    if report_type == "critical":
        system_prompt = """Você é um analista sênior de mercado crypto especializado em macro economia.
Seu papel é alertar traders sobre eventos críticos que podem impactar Bitcoin, Ethereum e altcoins.
Seja direto, objetivo e use emojis para facilitar leitura rápida.
Responda SEMPRE em português brasileiro.
Formate com Markdown do Telegram (negrito com *texto*, itálico com _texto_)."""
        user_prompt = f"""EVENTO CRÍTICO DETECTADO. Analise e explique o impacto provável no mercado crypto:

NOTÍCIA: {news_items[0]['title']}
DESCRIÇÃO: {news_items[0].get('description', '')[:300]}

CONTEXTO DE MERCADO ATUAL:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- ETH: ${eth_usd:,.0f} ({eth_change:+.1f}% 24h)
- Fear & Greed: {fg_value}/100 ({fg_class})

Explique em 3-5 linhas: o que aconteceu, por que importa para crypto, e qual a tendência provável de curto prazo."""

    else:  # summary diário
        system_prompt = """Você é um analista sênior de mercado crypto especializado em macro economia global.
Seu papel é fazer o briefing matinal para traders ativos de BTC, ETH e altcoins.
Use linguagem clara, objetiva, com emojis e formatação Markdown do Telegram.
Responda SEMPRE em português brasileiro."""
        user_prompt = f"""Faça o BRIEFING MACRO DIÁRIO para traders de crypto com base nas notícias abaixo.

PREÇOS ATUAIS:
- BTC: ${btc_usd:,.0f} ({btc_change:+.1f}% 24h)
- ETH: ${eth_usd:,.0f} ({eth_change:+.1f}% 24h)
- Fear & Greed Index: {fg_value}/100 ({fg_class})

PRINCIPAIS NOTÍCIAS DAS ÚLTIMAS HORAS:
{news_text}

ESTRUTURE o briefing assim:
1. 🌍 Macro Global (Fed, juros, geopolítica)
2. 📊 Impacto esperado no Crypto
3. ⚠️ Pontos de atenção para hoje
4. 🎯 Viés de mercado (altista/baixista/neutro) com justificativa

Seja conciso mas completo. Máximo 400 palavras."""

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
            if "content" in data and len(data["content"]) > 0:
                return data["content"][0]["text"]
            else:
                logger.error(f"Resposta inesperada da API: {data}")
                return "❌ Resposta inválida da IA. Tente novamente."
    except httpx.HTTPStatusError as e:
        logger.error(f"Erro HTTP na API Claude: {e.response.status_code} - {e.response.text}")
        return f"❌ Erro na API ({e.response.status_code}). Verifique a chave ANTHROPIC_API_KEY."
    except Exception as e:
        logger.error(f"Erro na API Claude: {e}")
        return f"❌ Erro ao consultar IA: {e}"


# ═══════════════════════════════════════════════════════════════════
#  FORMATAÇÃO DE MENSAGENS
# ═══════════════════════════════════════════════════════════════════

def format_price_header(prices: dict, fear_greed: dict, dxy: float | None) -> str:
    """Formata cabeçalho com preços atual."""
    btc = prices.get("bitcoin", {})
    eth = prices.get("ethereum", {})
    fg_val = fear_greed.get("value", "?")
    fg_cls = fear_greed.get("value_classification", "?")

    btc_emoji = "🟢" if btc.get("usd_24h_change", 0) >= 0 else "🔴"
    eth_emoji = "🟢" if eth.get("usd_24h_change", 0) >= 0 else "🔴"

    dxy_str = f"\n💵 *DXY:* `{dxy:.2f}`" if dxy else ""
    now = datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M")

    return (
        f"📡 *CRYPTO MACRO RADAR*\n"
        f"🕐 _{now} (Brasília)_\n"
        f"{'─'*30}\n"
        f"{btc_emoji} *BTC:* `${btc.get('usd', 0):,.0f}` "
        f"({btc.get('usd_24h_change', 0):+.1f}%)\n"
        f"{eth_emoji} *ETH:* `${eth.get('usd', 0):,.0f}` "
        f"({eth.get('usd_24h_change', 0):+.1f}%)\n"
        f"😱 *Fear & Greed:* `{fg_val}/100` _{fg_cls}_"
        f"{dxy_str}\n"
        f"{'─'*30}\n"
    )


# ═══════════════════════════════════════════════════════════════════
#  TAREFAS AGENDADAS
# ═══════════════════════════════════════════════════════════════════

async def send_daily_summary(bot: Bot):
    """Envia o resumo macro diário das 08h."""
    logger.info("Gerando resumo diário...")
    try:
        news, prices, fear_greed, dxy = await asyncio.gather(
            fetch_all_news(),
            fetch_prices(),
            fetch_fear_greed(),
            fetch_dxy(),
        )

        header = format_price_header(prices, fear_greed, dxy)
        analysis = await analyze_with_claude(news, prices, fear_greed, "summary")

        message = header + analysis

        await bot.send_message(
            chat_id=CHAT_ID,
            text=message[:4096],  # limite Telegram
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info("Resumo diário enviado.")
    except Exception as e:
        logger.error(f"Erro no resumo diário: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Erro no resumo diário: {e}")


async def check_critical_news(bot: Bot):
    """Verifica notícias críticas a cada 15 minutos."""
    logger.info("Verificando notícias críticas...")
    try:
        news = await fetch_all_news()

        for item in news:
            uid = item.get("link") or item.get("title", "")[:80]
            if uid in sent_news_cache:
                continue

            title_lower = item["title"].lower()
            desc_lower  = item.get("description", "").lower()

            is_critical = any(
                kw in title_lower or kw in desc_lower
                for kw in CRITICAL_KEYWORDS
            )

            if is_critical:
                sent_news_cache.add(uid)
                # Evita cache infinito
                if len(sent_news_cache) > 500:
                    sent_news_cache.clear()

                prices, fear_greed = await asyncio.gather(
                    fetch_prices(), fetch_fear_greed()
                )

                analysis = await analyze_with_claude(
                    [item], prices, fear_greed, "critical"
                )

                header = format_price_header(prices, fear_greed, None)
                message = (
                    f"🚨 *ALERTA CRÍTICO*\n"
                    f"{'─'*30}\n"
                    f"📰 *{item['title']}*\n"
                    f"{'─'*30}\n"
                    + header
                    + analysis
                    + f"\n\n🔗 [Ver notícia completa]({item.get('link', '#')})"
                )

                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=message[:4096],
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True,
                )
                logger.info(f"Alerta crítico enviado: {item['title'][:60]}")
                # Pausa entre alertas para não spammar
                await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Erro na verificação de críticos: {e}")


# ═══════════════════════════════════════════════════════════════════
#  COMANDOS DO BOT
# ═══════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"🚀 *Crypto Macro Radar ativo!*\n\n"
        f"Seu Chat ID: `{chat_id}`\n\n"
        f"Comandos disponíveis:\n"
        f"/status - Preços e Fear & Greed agora\n"
        f"/resumo - Gerar resumo macro agora\n"
        f"/ajuda - Lista de comandos\n\n"
        f"📅 Resumo diário: todos os dias às 08h (Brasília)\n"
        f"🚨 Alertas críticos: monitoramento contínuo",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra preços atuais sem análise de IA."""
    await update.message.reply_text("⏳ Buscando dados...", parse_mode=ParseMode.MARKDOWN)
    prices, fear_greed, dxy = await asyncio.gather(
        fetch_prices(), fetch_fear_greed(), fetch_dxy()
    )
    msg = format_price_header(prices, fear_greed, dxy)
    msg += "_Dados em tempo real via CoinGecko_"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_resumo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gera resumo macro sob demanda."""
    await update.message.reply_text("🧠 Gerando análise macro com IA...", parse_mode=ParseMode.MARKDOWN)
    bot = context.bot
    await send_daily_summary(bot)


async def cmd_ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*📚 Comandos disponíveis:*\n\n"
        "/start - Iniciar o bot\n"
        "/status - Ver preços agora (BTC, ETH, Fear & Greed)\n"
        "/resumo - Gerar resumo macro com IA agora\n"
        "/ajuda - Esta mensagem\n\n"
        "*⚙️ Funcionamento automático:*\n"
        "• Resumo diário: 08h00 (Brasília)\n"
        f"• Scan de notícias críticas: a cada {CHECK_INTERVAL_MINUTES} min\n\n"
        "*🔍 Fontes monitoradas:*\n"
        "Federal Reserve • Reuters • CoinDesk\n"
        "CoinTelegraph • Investing.com",
        parse_mode=ParseMode.MARKDOWN,
    )


# ═══════════════════════════════════════════════════════════════════
#  INICIALIZAÇÃO
# ═══════════════════════════════════════════════════════════════════

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN não configurado! Veja o arquivo .env")
    if not CHAT_ID:
        raise ValueError("❌ TELEGRAM_CHAT_ID não configurado! Veja o arquivo .env")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Registra comandos
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("resumo", cmd_resumo))
    app.add_handler(CommandHandler("ajuda",  cmd_ajuda))

    # Agendador
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    # Resumo diário às 08h
    scheduler.add_job(
        send_daily_summary,
        trigger="cron",
        hour=DAILY_REPORT_HOUR,
        minute=DAILY_REPORT_MINUTE,
        args=[app.bot],
    )

    # Checagem de críticos a cada 15 min
    scheduler.add_job(
        check_critical_news,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        args=[app.bot],
    )

    scheduler.start()
    logger.info("✅ Crypto Macro Radar iniciado!")
    logger.info(f"📅 Resumo diário às {DAILY_REPORT_HOUR:02d}:{DAILY_REPORT_MINUTE:02d}")
    logger.info(f"🔍 Scan crítico a cada {CHECK_INTERVAL_MINUTES} minutos")

    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
