import asyncio
import logging
import os
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    ContextTypes, MessageHandler, filters
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, PlainTextResponse
from starlette.routing import Route

from src.config import (
    TELEGRAM_BOT_TOKEN, load_settings, save_settings,
    load_muted_coins, save_muted_coins
)
from src.exchanges import exchange_manager

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

settings = load_settings()
muted_coins = load_muted_coins()
scheduler = AsyncIOScheduler()
application = None

PORT = int(os.environ.get("PORT", 5000))
WEBHOOK_PATH = "/webhook"
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")

def get_main_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Текущие отклонения", callback_data="show_deviations")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
        [InlineKeyboardButton("🔕 Замьюченные монеты", callback_data="show_muted")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_settings_keyboard():
    global settings
    binance_status = "✅" if settings.get("binance_enabled", True) else "❌"
    bybit_status = "✅" if settings.get("bybit_enabled", True) else "❌"
    gate_status = "✅" if settings.get("gate_enabled", True) else "❌"
    
    keyboard = [
        [InlineKeyboardButton(f"Binance: {binance_status}", callback_data="toggle_binance")],
        [InlineKeyboardButton(f"Bybit: {bybit_status}", callback_data="toggle_bybit")],
        [InlineKeyboardButton(f"Gate: {gate_status}", callback_data="toggle_gate")],
        [InlineKeyboardButton(f"Порог: {settings['threshold_percent']}%", callback_data="set_threshold")],
        [InlineKeyboardButton(f"Интервал: {settings['check_interval_seconds']}с", callback_data="set_interval")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global settings
    chat_id = update.effective_chat.id
    
    if chat_id not in settings["chat_ids"]:
        settings["chat_ids"].append(chat_id)
        save_settings(settings)
    
    await update.message.reply_text(
        "🚀 *Futures-Spot Deviation Scanner*\n\n"
        "Бот для мониторинга отклонений цены фьючерсов от спота на биржах Binance и Bybit.\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global settings, muted_coins
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    
    if query.data == "show_deviations":
        await show_deviations_handler(query)
    
    elif query.data == "settings":
        await query.edit_message_text(
            "⚙️ *Настройки*\n\n"
            f"Порог отклонения: {settings['threshold_percent']}%\n"
            f"Интервал проверки: {settings['check_interval_seconds']} сек\n",
            parse_mode="Markdown",
            reply_markup=get_settings_keyboard()
        )
    
    elif query.data == "toggle_binance":
        settings["binance_enabled"] = not settings.get("binance_enabled", True)
        save_settings(settings)
        await query.edit_message_text(
            "⚙️ *Настройки*\n\n"
            f"Binance {'включен' if settings['binance_enabled'] else 'отключен'}",
            parse_mode="Markdown",
            reply_markup=get_settings_keyboard()
        )
    
    elif query.data == "toggle_bybit":
        settings["bybit_enabled"] = not settings.get("bybit_enabled", True)
        save_settings(settings)
        await query.edit_message_text(
            "⚙️ *Настройки*\n\n"
            f"Bybit {'включен' if settings['bybit_enabled'] else 'отключен'}",
            parse_mode="Markdown",
            reply_markup=get_settings_keyboard()
        )
    
    elif query.data == "toggle_gate":
        settings["gate_enabled"] = not settings.get("gate_enabled", True)
        save_settings(settings)
        await query.edit_message_text(
            "⚙️ *Настройки*\n\n"
            f"Gate {'включен' if settings['gate_enabled'] else 'отключен'}",
            parse_mode="Markdown",
            reply_markup=get_settings_keyboard()
        )
    
    elif query.data == "set_threshold":
        context.user_data["waiting_for"] = "threshold"
        await query.edit_message_text(
            "Введите новый порог отклонения в процентах (например: 0.5):",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Отмена", callback_data="settings")
            ]])
        )
    
    elif query.data == "set_interval":
        context.user_data["waiting_for"] = "interval"
        await query.edit_message_text(
            "Введите интервал проверки в секундах (минимум 30):",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Отмена", callback_data="settings")
            ]])
        )
    
    elif query.data == "show_muted":
        await show_muted_handler(query)
    
    elif query.data.startswith("unmute_"):
        coin = query.data.replace("unmute_", "")
        if coin in muted_coins:
            del muted_coins[coin]
            save_muted_coins(muted_coins)
        await show_muted_handler(query)
    
    elif query.data == "back_to_main":
        await query.edit_message_text(
            "🚀 *Futures-Spot Deviation Scanner*\n\nВыберите действие:",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard()
        )

async def show_deviations_handler(query):
    global settings
    await query.edit_message_text("⏳ Загрузка данных...")
    
    try:
        # Теперь данные берутся из кэша WebSocket - моментально
        deviations = await exchange_manager.get_all_deviations(
            binance_enabled=settings.get("binance_enabled", True),
            bybit_enabled=settings.get("bybit_enabled", True),
            gate_enabled=settings.get("gate_enabled", True)
        )
        
        if not deviations or all(len(d) == 0 for d in deviations.values()):
            await query.edit_message_text(
                "❌ Нет данных. WebSocket соединения могут ещё загружаться.",
                reply_markup=get_main_keyboard()
            )
            return
        
        message = "📊 *Текущие отклонения (Топ-20)*\n\n"
        
        for exchange, data in deviations.items():
            message += f"*{exchange.upper()}*:\n"
            if data:
                for coin, spot, futures, dev in data[:10]:
                    sign = "+" if dev > 0 else ""
                    emoji = "🔴" if abs(dev) >= settings["threshold_percent"] else "⚪"
                    message += f"{emoji} {coin}: {sign}{dev:.2f}%\n"
            else:
                message += "⚠️ Нет данных\n"
            message += "\n"
        
        await query.edit_message_text(
            message,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="show_deviations")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
            ])
        )
    except Exception as e:
        logger.error(f"Error showing deviations: {e}")
        await query.edit_message_text(
            f"❌ Ошибка при получении данных: {str(e)}",
            reply_markup=get_main_keyboard()
        )

async def show_muted_handler(query):
    global muted_coins
    clean_expired_mutes()
    
    if not muted_coins:
        await query.edit_message_text(
            "🔕 *Замьюченные монеты*\n\nСписок пуст.\n\n"
            "Чтобы замьютить монету, используйте команду:\n"
            "`/mute COIN HOURS`\n"
            "Например: `/mute BTC 2`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")
            ]])
        )
        return
    
    keyboard = []
    message = "🔕 *Замьюченные монеты*\n\n"
    
    for coin, until_str in muted_coins.items():
        until = datetime.fromisoformat(until_str)
        remaining = until - datetime.now()
        hours = int(remaining.total_seconds() / 3600)
        mins = int((remaining.total_seconds() % 3600) / 60)
        message += f"• {coin}: {hours}ч {mins}м\n"
        keyboard.append([InlineKeyboardButton(f"🔔 Включить {coin}", callback_data=f"unmute_{coin}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
    
    await query.edit_message_text(
        message,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global settings
    
    if "waiting_for" not in context.user_data:
        return
    
    text = update.message.text
    waiting_for = context.user_data.get("waiting_for")
    
    if waiting_for == "threshold":
        try:
            value = float(text)
            if value <= 0:
                raise ValueError("Must be positive")
            settings["threshold_percent"] = value
            save_settings(settings)
            del context.user_data["waiting_for"]
            await update.message.reply_text(
                f"✅ Порог установлен: {value}%",
                reply_markup=get_settings_keyboard()
            )
        except ValueError:
            await update.message.reply_text("❌ Введите корректное число (например: 0.5)")
    
    elif waiting_for == "interval":
        try:
            value = int(text)
            if value < 30:
                raise ValueError("Must be at least 30")
            settings["check_interval_seconds"] = value
            save_settings(settings)
            reschedule_checker()
            del context.user_data["waiting_for"]
            await update.message.reply_text(
                f"✅ Интервал установлен: {value} секунд",
                reply_markup=get_settings_keyboard()
            )
        except ValueError:
            await update.message.reply_text("❌ Введите число не менее 30")

async def mute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global muted_coins
    
    if len(context.args) != 2:
        await update.message.reply_text(
            "Использование: `/mute COIN HOURS`\n"
            "Например: `/mute BTC 2`",
            parse_mode="Markdown"
        )
        return
    
    coin = context.args[0].upper()
    try:
        hours = float(context.args[1])
        if hours <= 0:
            raise ValueError()
    except ValueError:
        await update.message.reply_text("❌ Укажите корректное количество часов")
        return
    
    until = datetime.now() + timedelta(hours=hours)
    muted_coins[coin] = until.isoformat()
    save_muted_coins(muted_coins)
    
    await update.message.reply_text(
        f"🔕 Сигналы по {coin} отключены на {hours} часов",
        reply_markup=get_main_keyboard()
    )

def clean_expired_mutes():
    global muted_coins
    now = datetime.now()
    expired = [coin for coin, until_str in muted_coins.items() 
               if datetime.fromisoformat(until_str) < now]
    for coin in expired:
        del muted_coins[coin]
    if expired:
        save_muted_coins(muted_coins)

def is_muted(coin: str) -> bool:
    global muted_coins
    if coin not in muted_coins:
        return False
    until = datetime.fromisoformat(muted_coins[coin])
    if datetime.now() >= until:
        del muted_coins[coin]
        save_muted_coins(muted_coins)
        return False
    return True

async def check_and_alert():
    global settings, application
    
    logger.info(f"check_and_alert started. chat_ids: {settings['chat_ids']}, threshold: {settings['threshold_percent']}%")
    
    if not settings["chat_ids"]:
        logger.info("No chat_ids registered, skipping alerts")
        return
    
    if not settings.get("binance_enabled", True) and not settings.get("bybit_enabled", True) and not settings.get("gate_enabled", True):
        logger.info("All exchanges disabled, skipping")
        return
    
    try:
        # Данные берутся из кэша WebSocket - никаких HTTP запросов
        deviations = await exchange_manager.get_all_deviations(
            binance_enabled=settings.get("binance_enabled", True),
            bybit_enabled=settings.get("bybit_enabled", True),
            gate_enabled=settings.get("gate_enabled", True)
        )
        
        threshold = settings["threshold_percent"]
        alerts = []
        
        total_pairs = sum(len(data) for data in deviations.values())
        logger.info(f"Fetched {total_pairs} pairs total from cache")
        
        for exchange, data in deviations.items():
            for coin, spot, futures, dev in data:
                if abs(dev) >= threshold and not is_muted(coin):
                    sign = "+" if dev > 0 else ""
                    alerts.append(f"🚨 *{exchange.upper()}* | {coin}: {sign}{dev:.2f}%")
        
        logger.info(f"Found {len(alerts)} alerts above threshold {threshold}%")
        
        if alerts and application:
            message = "⚠️ *Сигнал отклонения!*\n\n" + "\n".join(alerts[:15])
            for chat_id in settings["chat_ids"]:
                try:
                    await application.bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode="Markdown"
                    )
                    logger.info(f"Alert sent to {chat_id}")
                except Exception as e:
                    logger.error(f"Error sending alert to {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Error in check_and_alert: {e}")

def reschedule_checker():
    global scheduler, settings
    try:
        scheduler.remove_job("price_checker")
    except:
        pass
    scheduler.add_job(
        check_and_alert,
        'interval',
        seconds=settings["check_interval_seconds"],
        id="price_checker"
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global settings
    binance = "✅" if settings.get("binance_enabled", True) else "❌"
    bybit = "✅" if settings.get("bybit_enabled", True) else "❌"
    gate = "✅" if settings.get("gate_enabled", True) else "❌"
    
    # Показываем количество пар в кэше
    binance_count = len(exchange_manager.spot_prices.get('binance', {}))
    bybit_count = len(exchange_manager.spot_prices.get('bybit', {}))
    gate_count = len(exchange_manager.spot_prices.get('gate', {}))
    
    await update.message.reply_text(
        f"📊 *Статус бота*\n\n"
        f"Binance: {binance} ({binance_count} пар)\n"
        f"Bybit: {bybit} ({bybit_count} пар)\n"
        f"Gate: {gate} ({gate_count} пар)\n"
        f"Порог: {settings['threshold_percent']}%\n"
        f"Интервал: {settings['check_interval_seconds']} сек\n"
        f"Замьючено монет: {len(muted_coins)}",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )

async def telegram_webhook(request: Request) -> Response:
    global application
    try:
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return Response(status_code=500)

async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")

async def home(request: Request) -> PlainTextResponse:
    return PlainTextResponse("Futures-Spot Deviation Scanner Bot is running!")

@asynccontextmanager
async def lifespan(app: Starlette):
    global application, scheduler
    
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set!")
        yield
        return
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("mute", mute_command))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    await application.initialize()
    await application.start()
    
    # НОВОЕ: Запуск WebSocket соединений
    logger.info("Starting WebSocket connections...")
    await exchange_manager.start_websockets()
    
    # Даём время для загрузки данных в кэш
    logger.info("Waiting for initial data load...")
    await asyncio.sleep(15)
    
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}"
        await application.bot.set_webhook(webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")
    else:
        logger.warning("RENDER_EXTERNAL_URL not set, webhook not configured")
    
    scheduler.start()
    reschedule_checker()
    logger.info("Bot started with WebSocket mode!")
    
    yield
    
    # НОВОЕ: Остановка WebSocket соединений
    logger.info("Stopping WebSocket connections...")
    await exchange_manager.stop_websockets()
    
    scheduler.shutdown()
    await application.stop()
    await application.shutdown()
    logger.info("Bot stopped")

routes = [
    Route("/", home),
    Route("/health", health_check),
    Route(WEBHOOK_PATH, telegram_webhook, methods=["POST"]),
]

app = Starlette(debug=False, routes=routes, lifespan=lifespan)

def create_app():
    return app
