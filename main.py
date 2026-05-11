from __future__ import annotations

import asyncio
import importlib
import logging
import os
import re
import time
from typing import TYPE_CHECKING

try:
    from keep_alive import keep_alive, set_health_state
except ImportError:
    from .keep_alive import keep_alive, set_health_state

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "8756688085"))
DEFAULT_INSTAGRAM_URL = "https://www.instagram.com/kinotop.bot/"
INSTAGRAM_CHANNEL_URL = os.environ.get("INSTAGRAM_CHANNEL_URL", "https://www.instagram.com/movie.hub.star?igsh=MTduMjZlc2hyazB6cg==").strip() or DEFAULT_INSTAGRAM_URL

VERIFICATION_BOT_URL = os.environ.get("VERIFICATION_BOT_URL", "https://t.me/gram_prbot?start=7657019165").strip()
VERIFICATION_WAIT_SECONDS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from telegram import Update
    from telegram.ext import ContextTypes

ApplicationBuilder = None
CallbackQueryHandler = None
CommandHandler = None
ContextTypes = None
ConversationHandler = None
MessageHandler = None
filters = None
InlineKeyboardButton = None
InlineKeyboardMarkup = None
ReplyKeyboardMarkup = None
ReplyKeyboardRemove = None
MongoClient = None
PyMongoError = Exception

# ConversationHandler.END qiymati — lazy import muammosini hal qilish uchun
CONV_END = -1


def ensure_telegram_imports():
    global ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes
    global ConversationHandler, MessageHandler, filters
    global InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
    if ApplicationBuilder is not None:
        return

    telegram = importlib.import_module("telegram")
    telegram_ext = importlib.import_module("telegram.ext")
    InlineKeyboardButton = telegram.InlineKeyboardButton
    InlineKeyboardMarkup = telegram.InlineKeyboardMarkup
    ReplyKeyboardMarkup = telegram.ReplyKeyboardMarkup
    ReplyKeyboardRemove = telegram.ReplyKeyboardRemove
    ApplicationBuilder = telegram_ext.ApplicationBuilder
    CallbackQueryHandler = telegram_ext.CallbackQueryHandler
    CommandHandler = telegram_ext.CommandHandler
    ContextTypes = telegram_ext.ContextTypes
    ConversationHandler = telegram_ext.ConversationHandler
    MessageHandler = telegram_ext.MessageHandler
    filters = telegram_ext.filters


def ensure_pymongo_imports():
    global MongoClient, PyMongoError
    if MongoClient is not None and PyMongoError is not Exception:
        return
    pymongo = importlib.import_module("pymongo")
    pymongo_errors = importlib.import_module("pymongo.errors")
    MongoClient = pymongo.MongoClient
    PyMongoError = pymongo_errors.PyMongoError


MONGO_URL = os.environ.get("MONGO_URL", "").strip()

client = None
db = None
movies_col = None
series_col = None
folders_col = None
users_col = None

_last_reconnect_attempt = 0
_RECONNECT_COOLDOWN = 10

SERVICE_UNAVAILABLE_TEXT = "Serverda vaqtincha muammo bor. Keyinroq yana urinib ko'ring."
DEFAULT_SIFAT = os.environ.get("DEFAULT_SIFAT", "720p").strip() or "720p"
DEFAULT_TIL = os.environ.get("DEFAULT_TIL", "O'zbek").strip() or "O'zbek"
DEFAULT_VAQT = os.environ.get("DEFAULT_VAQT", "-").strip() or "-"
KEEP_PREVIOUS_TEXT = "♻️ Oldingisini qoldirish"
CONFIRM_SAVE_TEXT = "✅ Saqlash"
CONFIRM_CANCEL_TEXT = "❌ Bekor qilish"
FOLDER_SKIP_TEXT = "❌ Yo'q, oddiy saqlash"
FOLDER_CREATE_TEXT = "🆕 Yangi jild yaratish"
FOLDER_ADD_EXISTING_TEXT = "📂 Mavjud jildga qo'shish"
FOLDER_BACK_TEXT = "🔙 Orqaga"
JILD_FINISH_TEXT = "✅ Tugatish"
JILD_CLEAR_TEXT = "🧹 Tozalash"
SERIES_CALLBACK_PREFIX = "series_part:"

_broadcast_active = False


# ===================== DB ULANISH =====================

def _init_db_client():
    global client, db, movies_col, series_col, folders_col, users_col
    ensure_pymongo_imports()
    if not MONGO_URL:
        raise RuntimeError("MONGO_URL topilmadi.")
    client = MongoClient(
        MONGO_URL,
        maxPoolSize=10,
        minPoolSize=1,
        maxIdleTimeMS=30000,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        socketTimeoutMS=15000,
        retryWrites=True,
        retryReads=True,
    )
    db = client["moviebot"]
    movies_col = db["movies"]
    series_col = db["series_groups"]
    folders_col = db["movie_folders"]
    users_col = db["users"]


def _ensure_connected():
    global client, _last_reconnect_attempt
    if client is not None:
        try:
            client.admin.command("ping")
            return
        except Exception as e:
            logger.warning(f"MongoDB ping xatosi: {e}. Qayta ulanish...")

    now = time.time()
    if now - _last_reconnect_attempt < _RECONNECT_COOLDOWN:
        raise RuntimeError("MongoDB vaqtinchalik mavjud emas.")

    _last_reconnect_attempt = now
    try:
        _init_db_client()
        client.admin.command("ping")
        set_health_state(db="connected", last_error="")
        logger.info("MongoDB qayta ulandi.")
    except Exception as exc:
        client = None
        set_health_state(db="error", last_error=str(exc))
        raise RuntimeError(f"MongoDB ulanmadi: {exc}") from exc


def run_db(operation):
    _ensure_connected()
    try:
        return operation(movies_col)
    except PyMongoError as exc:
        logger.error(f"MongoDB xatosi: {exc}")
        raise


def run_users_db(operation):
    _ensure_connected()
    try:
        return operation(users_col)
    except PyMongoError as exc:
        logger.error(f"MongoDB users xatosi: {exc}")
        raise


def run_series_db(operation):
    _ensure_connected()
    try:
        return operation(series_col)
    except PyMongoError as exc:
        logger.error(f"MongoDB series xatosi: {exc}")
        raise


def run_folders_db(operation):
    _ensure_connected()
    try:
        return operation(folders_col)
    except PyMongoError as exc:
        logger.error(f"MongoDB folders xatosi: {exc}")
        raise


# ===================== DB FUNKSIYALAR =====================

def save_movie(code, data):
    run_db(lambda col: col.update_one({"code": code}, {"$set": {**data, "code": code}}, upsert=True))


def delete_movie_db(code):
    run_db(lambda col: col.delete_one({"code": code}))


def movie_exists(code):
    return run_db(lambda col: col.find_one({"code": code}, {"_id": 1}) is not None)


def get_movie_by_file_id(file_id):
    doc = run_db(lambda col: col.find_one({"file_id": file_id}, {"code": 1, "nom": 1, "_id": 0}))
    if not doc:
        return None
    return {"code": doc.get("code", "-"), "nom": doc.get("nom", "-")}


def get_last_and_next_movie_code():
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}}}},
            {"$match": {"code_num": {"$ne": None, "$lt": 1000000}}},
            {"$sort": {"code_num": -1}},
            {"$limit": 1},
        ]
        latest = next(col.aggregate(pipeline), None)
        if latest:
            n = int(latest["code_num"])
            return str(n), str(n + 1)
        return "yo'q", "1"
    return run_db(operation)


def get_movie(code):
    doc = run_db(lambda col: col.find_one({"code": code}))
    if not doc:
        return None
    return {
        "type": doc["type"],
        "file_id": doc["file_id"],
        "nom": doc.get("nom", "-"),
        "sifat": doc.get("sifat", "-"),
        "til": doc.get("til", "-"),
        "vaqt": doc.get("vaqt", "-"),
    }


def parse_numeric_code(value):
    if not value or not value.isdigit():
        return None
    return int(value)


def get_series_range_by_code(code):
    code_num = parse_numeric_code(code)
    if code_num is None:
        return None
    def operation(col):
        cursor = col.find(
            {"start_code_num": {"$lte": code_num}, "end_code_num": {"$gte": code_num}},
            {"_id": 0},
        ).sort("start_code_num", 1).limit(1)
        return next(cursor, None)
    return run_series_db(operation)


def get_all_series_ranges():
    return run_series_db(lambda col: list(col.find({}, {"_id": 0}).sort("start_code_num", 1)))


def get_movies_in_range(start_code_num, end_code_num):
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}}}},
            {"$match": {"code_num": {"$ne": None, "$gte": start_code_num, "$lte": end_code_num}}},
            {"$sort": {"code_num": 1}},
            {"$project": {"_id": 0, "code": 1, "nom": 1, "code_num": 1}},
        ]
        return list(col.aggregate(pipeline))
    return run_db(operation)


def get_all_folder_names():
    return run_folders_db(lambda col: [item["name"] for item in col.find({}, {"_id": 0, "name": 1}).sort("name", 1)])


def folder_exists_by_name(name):
    return run_folders_db(lambda col: col.find_one({"name": name}, {"_id": 1}) is not None)


def get_folder_by_code(code):
    return run_folders_db(lambda col: col.find_one({"codes": code}, {"_id": 0, "name": 1, "codes": 1}))


def add_movie_to_folder(folder_name, code):
    run_folders_db(lambda col: col.update_one(
        {"name": folder_name},
        {"$set": {"name": folder_name, "name_lower": folder_name.lower()}, "$addToSet": {"codes": code}, "$setOnInsert": {"created_at": int(time.time())}},
        upsert=True,
    ))


def add_movies_to_folder(folder_name, codes):
    unique_codes = sort_codes_for_folder(list(set(codes)))
    if not unique_codes:
        return
    run_folders_db(lambda col: col.update_one(
        {"name": folder_name},
        {"$set": {"name": folder_name, "name_lower": folder_name.lower()}, "$addToSet": {"codes": {"$each": unique_codes}}, "$setOnInsert": {"created_at": int(time.time())}},
        upsert=True,
    ))


def get_existing_movie_codes(codes):
    if not codes:
        return []
    return run_db(lambda col: [item["code"] for item in col.find({"code": {"$in": codes}}, {"_id": 0, "code": 1})])


def sort_codes_for_folder(codes):
    numeric, non_numeric = [], []
    for code in codes:
        parsed = parse_numeric_code(code)
        if parsed is None:
            non_numeric.append(code)
        else:
            numeric.append((parsed, code))
    numeric.sort(key=lambda item: item[0])
    non_numeric.sort()
    return [item[1] for item in numeric] + non_numeric


def get_movies_for_folder(folder_name):
    folder = run_folders_db(lambda col: col.find_one({"name": folder_name}, {"_id": 0, "codes": 1}))
    if not folder:
        return []
    codes = sort_codes_for_folder(folder.get("codes", []))
    if not codes:
        return []
    code_to_movie = {
        movie["code"]: movie
        for movie in run_db(lambda col: list(col.find({"code": {"$in": codes}}, {"_id": 0, "code": 1, "nom": 1})))
    }
    return [code_to_movie[code] for code in codes if code in code_to_movie]


def get_all_user_ids():
    raw = run_users_db(lambda col: [item["user_id"] for item in col.find({"is_admin": {"$ne": True}}, {"_id": 0, "user_id": 1})])
    result = []
    for uid in raw:
        try:
            result.append(int(uid))
        except Exception:
            pass
    return result


def add_to_favorites(user_id, code):
    run_users_db(lambda col: col.update_one({"user_id": user_id}, {"$addToSet": {"favorites": code}}, upsert=True))


def remove_from_favorites(user_id, code):
    run_users_db(lambda col: col.update_one({"user_id": user_id}, {"$pull": {"favorites": code}}))


def get_favorites(user_id):
    try:
        doc = run_users_db(lambda col: col.find_one({"user_id": user_id}, {"favorites": 1, "_id": 0}))
        return doc.get("favorites", []) if doc else []
    except Exception:
        return []


def is_favorite(user_id, code):
    try:
        return run_users_db(lambda col: col.find_one({"user_id": user_id, "favorites": code}, {"_id": 1})) is not None
    except Exception:
        return False


# ===================== VERIFICATION =====================

def mark_user_started(user_id):
    run_users_db(lambda col: col.update_one(
        {"user_id": user_id},
        {
            "$setOnInsert": {"started_at": int(time.time())},
            "$set": {"user_id": user_id},
        },
        upsert=True,
    ))


def get_user_started_at(user_id):
    try:
        doc = run_users_db(lambda col: col.find_one({"user_id": user_id}, {"started_at": 1, "_id": 0}))
        if doc and "started_at" in doc:
            return doc["started_at"]
        return None
    except Exception:
        logger.warning(f"get_user_started_at xatosi (user_id={user_id}) — o'tkazildi")
        return None


def is_user_verified(user_id):
    started_at = get_user_started_at(user_id)
    if started_at is None:
        return False, "not_started"
    elapsed = int(time.time()) - started_at
    if elapsed < VERIFICATION_WAIT_SECONDS:
        return False, elapsed
    return True, None


def get_verification_keyboard():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Obuna bo'lish", url=VERIFICATION_BOT_URL)
    ]])


# ===================== FOYDALANUVCHI KUZATUV =====================

def track_user(user):
    if user is None:
        return
    run_users_db(lambda col: col.update_one(
        {"user_id": user.id},
        {"$set": {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "is_admin": user.id == ADMIN_ID,
            "last_seen_at": int(time.time()),
        }},
        upsert=True,
    ))


def get_tracked_user_count():
    return run_users_db(lambda col: col.count_documents({"is_admin": {"$ne": True}}))


def remember_user(update):
    user = update.effective_user
    if user is None:
        return
    try:
        track_user(user)
    except Exception:
        logger.warning("Foydalanuvchini saqlashda xato (o'tkazildi)")


# ===================== QIDIRUV =====================

def search_movies_by_name(query: str, limit: int = 15):
    if not query or len(query.strip()) < 2:
        return []
    query = query.strip()
    regex_pattern = ".*".join(re.escape(ch) for ch in query)
    try:
        return run_db(lambda col: list(col.find(
            {"nom": {"$regex": regex_pattern, "$options": "i"}},
            {"_id": 0, "code": 1, "nom": 1, "sifat": 1, "til": 1},
        ).limit(limit)))
    except Exception:
        return []


# ===================== MOVIE HELPERS =====================

def get_movie_reply_markup(code, user_id=None):
    rows = []
    if user_id is not None and user_id != ADMIN_ID:
        try:
            in_fav = is_favorite(user_id, code)
        except Exception:
            in_fav = False
        fav_text = "❤️ Sevimlilardan chiqarish" if in_fav else "🤍 Sevimlilarga qo'shish"
        rows.append([InlineKeyboardButton(fav_text, callback_data=f"fav:{code}")])
    if INSTAGRAM_CHANNEL_URL:
        rows.append([InlineKeyboardButton("Qolgan kino kodlarini ko'rish uchun bosing", url=INSTAGRAM_CHANNEL_URL)])
    if not rows:
        return None
    return InlineKeyboardMarkup(rows)


def build_movie_caption(code, data):
    return (
        f"🎬 {data.get('nom', '-')}\n"
        f"🎥 Sifat: {data.get('sifat', '-')}\n"
        f"🌐 Til: {data.get('til', '-')}\n"
        f"⏱️ Davomiylik: {data.get('vaqt', '-')}\n"
        f"🆔 Kod: {code}"
    )


def get_series_parts_keyboard(series_data, movies):
    rows, row = [], []
    start_code_num = series_data["start_code_num"]
    for movie in movies:
        part_number = (movie["code_num"] - start_code_num) + 1
        row.append(InlineKeyboardButton(f"{part_number}-qism", callback_data=f"{SERIES_CALLBACK_PREFIX}{movie['code']}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def get_folder_parts_keyboard(movies):
    rows, row = [], []
    for index, movie in enumerate(movies, start=1):
        row.append(InlineKeyboardButton(f"{index}-qism", callback_data=f"{SERIES_CALLBACK_PREFIX}{movie['code']}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


async def send_movie_to_chat(target_message, code, data, user_id=None):
    caption = build_movie_caption(code, data)
    reply_markup = get_movie_reply_markup(code, user_id=user_id)
    file_id = data["file_id"]
    if data["type"] == "video":
        await target_message.reply_video(video=file_id, caption=caption, reply_markup=reply_markup)
    else:
        await target_message.reply_document(document=file_id, caption=caption, reply_markup=reply_markup)


async def send_series_parts_prompt(target_message, series_data, movies):
    await target_message.reply_text(
        f"🎞️ {series_data['title']}\n"
        f"🧾 Kodlar oralig'i: {series_data['start_code_num']} - {series_data['end_code_num']}\n"
        "Kerakli qismni tanlang:",
        reply_markup=get_series_parts_keyboard(series_data, movies),
    )


async def send_folder_parts_prompt(target_message, folder_data, movies):
    await target_message.reply_text(
        f"🎞️ {folder_data['name']}\n"
        f"📚 Qismlar soni: {len(movies)}\n"
        "Kerakli qismni tanlang:",
        reply_markup=get_folder_parts_keyboard(movies),
    )


# ===================== KLAVIATURALAR =====================

def get_sifat_keyboard():
    return ReplyKeyboardMarkup([["480p", "720p", "1080p"], ["1080p Full HD"], [KEEP_PREVIOUS_TEXT]], resize_keyboard=True, one_time_keyboard=True)


def get_til_keyboard():
    return ReplyKeyboardMarkup([["🇺🇿 O'zbek", "🇷🇺 Rus", "🇬🇧 Ingliz"], [KEEP_PREVIOUS_TEXT]], resize_keyboard=True, one_time_keyboard=True)


def get_confirm_keyboard():
    return ReplyKeyboardMarkup([[CONFIRM_SAVE_TEXT, CONFIRM_CANCEL_TEXT]], resize_keyboard=True, one_time_keyboard=True)


def get_folder_choice_keyboard():
    return ReplyKeyboardMarkup([[FOLDER_SKIP_TEXT], [FOLDER_CREATE_TEXT, FOLDER_ADD_EXISTING_TEXT]], resize_keyboard=True, one_time_keyboard=True)


def build_folder_list_keyboard(folder_names):
    rows, row = [], []
    for name in folder_names:
        row.append(name)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([FOLDER_BACK_TEXT, FOLDER_SKIP_TEXT])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def get_kod_suggestion_keyboard(next_code):
    return ReplyKeyboardMarkup([[next_code, KEEP_PREVIOUS_TEXT]], resize_keyboard=True, one_time_keyboard=True)


def get_admin_menu_keyboard():
    return ReplyKeyboardMarkup(
        [["/edit", "/delete <kod>"], ["/seriallist", "/foydalanuvchi 777"], ["/sevimli"]],
        resize_keyboard=True, one_time_keyboard=False,
    )


def parse_codes_input(raw_value):
    normalized = raw_value.replace(",", " ").replace(";", " ").replace("\n", " ").strip()
    if not normalized:
        return [], ["bo'sh qiymat"]
    parsed_codes, invalid_tokens = set(), []
    for token in normalized.split():
        value = token.strip()
        if not value:
            continue
        if "-" in value:
            parts = value.split("-", 1)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                s, e = int(parts[0]), int(parts[1])
                if s <= e:
                    for n in range(s, e + 1):
                        parsed_codes.add(str(n))
                    continue
        elif value.isdigit():
            parsed_codes.add(value)
            continue
        invalid_tokens.append(value)
    return sort_codes_for_folder(list(parsed_codes)), invalid_tokens


def format_codes_for_text(codes, limit=20):
    if not codes:
        return "-"
    if len(codes) <= limit:
        return ", ".join(codes)
    return f"{', '.join(codes[:limit])} ... (jami {len(codes)} ta)"


async def send_confirm_prompt(update, data):
    await update.message.reply_text(
        f"📋 Tekshirib chiqing:\n\n"
        f"🆔 Kod: {data['kod']}\n"
        f"🎬 Nom: {data['nom']}\n"
        f"🎥 Sifat: {data['sifat']}\n"
        f"🌐 Til: {data['til']}\n"
        f"⏱️ Davomiylik: {data['vaqt']}\n\n"
        f"Saqlaymizmi?",
        reply_markup=get_confirm_keyboard(),
    )


async def reply_service_unavailable(update):
    try:
        if update.message:
            await update.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
        elif update.callback_query and update.callback_query.message:
            await update.callback_query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
    except Exception:
        pass


# ===================== CONVERSATION STATES =====================

KOD_VAQT, NOM, SIFAT, TIL, VAQT, CONFIRM, FOLDER_CHOICE, FOLDER_CREATE, FOLDER_PICK = range(9)
EDIT_KOD, EDIT_NOM, EDIT_SIFAT, EDIT_TIL, EDIT_VAQT = range(9, 14)


async def log_error(update: object, context):
    logger.exception("Telegram handler error", exc_info=context.error)


# ===================== /start =====================

async def start(update, context):
    remember_user(update)
    user_id = update.message.from_user.id

    if user_id == ADMIN_ID:
        await update.message.reply_text(
            "Salom Admin! Movie HD botiga xush kelibsiz!\n\nKino qo'shish uchun video yoki fayl yuboring.",
            reply_markup=get_admin_menu_keyboard(),
        )
        return

    try:
        mark_user_started(user_id)
    except Exception:
        logger.warning("mark_user_started xatosi — o'tkazildi")

    await update.message.reply_text(
        "🎬 Salom! Movie HD botiga xush kelibsiz!\n\n"
        "✅ Botdan foydalanish uchun quyidagi botga obuna bo'ling va /start bosing:\n\n"
        "⬇️ Tugmani bosing, obuna bo'ling va start bosing:",
        reply_markup=get_verification_keyboard(),
    )
    await update.message.reply_text(
        f"⏳ Obuna bo'lgandan so'ng {VERIFICATION_WAIT_SECONDS} soniya kuting va "
        "qayta bu yerga kino kodini yuboring!\n\n"
        "✅ Shundan so'ng kino kodini yuboring — kino darhol yuboriladi!"
    )


async def unknown_command(update, context):
    await update.message.reply_text("❓ Bu komanda mavjud emas. Kino kodini yozing.")


# ===================== VIDEO/DOCUMENT =====================
# BUG FIX: ConversationHandler.END o'rniga CONV_END (-1) ishlatiladi
# chunki ConversationHandler lazy import qilinadi va ba'zan None bo'lib qolishi mumkin

async def handle_video(update, context):
    global _broadcast_active
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if _broadcast_active:
        await handle_admin_broadcast_message(update, context)
        return

    context.user_data["file_id"] = update.message.video.file_id
    context.user_data["file_type"] = "video"
    try:
        existing_movie = get_movie_by_file_id(context.user_data["file_id"])
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\nKod: {existing_movie['code']}\nNom: {existing_movie['nom']}\n\nBoshqa kino faylini yuboring."
        )
        return CONV_END
    context.user_data.pop("vaqt_draft", None)
    context.user_data.pop("vaqt_locked", None)
    try:
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        last_code, next_code = "?", "?"
    await update.message.reply_text(
        f"Oxirgi saqlangan kod: {last_code}\nTavsiya etilayotgan kod: {next_code}\n\nKodini kiriting yoki quyidagi tugmani bosing:",
        reply_markup=get_kod_suggestion_keyboard(next_code),
    )
    return KOD_VAQT


async def handle_document(update, context):
    global _broadcast_active
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if _broadcast_active:
        await handle_admin_broadcast_message(update, context)
        return

    context.user_data["file_id"] = update.message.document.file_id
    context.user_data["file_type"] = "document"
    try:
        existing_movie = get_movie_by_file_id(context.user_data["file_id"])
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\nKod: {existing_movie['code']}\nNom: {existing_movie['nom']}\n\nBoshqa kino faylini yuboring."
        )
        return CONV_END
    context.user_data.pop("vaqt_draft", None)
    context.user_data.pop("vaqt_locked", None)
    try:
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        last_code, next_code = "?", "?"
    await update.message.reply_text(
        f"Oxirgi saqlangan kod: {last_code}\nTavsiya etilayotgan kod: {next_code}\n\nKodini kiriting yoki quyidagi tugmani bosing:",
        reply_markup=get_kod_suggestion_keyboard(next_code),
    )
    return KOD_VAQT


# ===================== CONVERSATION HANDLERS =====================

async def get_kod_vaqt(update, context):
    d = context.user_data
    raw = update.message.text.strip()
    if raw == KEEP_PREVIOUS_TEXT:
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text("Kodini kiriting yoki tavsiyani tanlang:", reply_markup=get_kod_suggestion_keyboard(next_code))
        return KOD_VAQT
    code = raw
    if not code or not code.isdigit():
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text(f"Kod faqat raqamlardan iborat bo'lsin.\nTavsiya: {next_code}", reply_markup=get_kod_suggestion_keyboard(next_code))
        return KOD_VAQT
    try:
        exists = movie_exists(code)
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    if exists:
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text(f"⚠️ {code} kodi allaqachon mavjud!\nTavsiya: {next_code}", reply_markup=get_kod_suggestion_keyboard(next_code))
        return KOD_VAQT
    d["kod"] = code
    await update.message.reply_text("Kino nomini kiriting:", reply_markup=ReplyKeyboardRemove())
    return NOM


async def get_nom(update, context):
    context.user_data["nom"] = update.message.text.strip()
    await update.message.reply_text("Sifatini tanlang yoki qo'lda yozing:", reply_markup=get_sifat_keyboard())
    return SIFAT


async def get_sifat(update, context):
    raw = update.message.text.strip()
    context.user_data["sifat"] = (context.user_data.get("last_sifat") or DEFAULT_SIFAT) if raw == KEEP_PREVIOUS_TEXT else (raw or DEFAULT_SIFAT)
    await update.message.reply_text("Tilini tanlang yoki qo'lda yozing:", reply_markup=get_til_keyboard())
    return TIL


async def get_til(update, context):
    d = context.user_data
    raw = update.message.text.strip()
    if raw == KEEP_PREVIOUS_TEXT:
        d["til"] = d.get("last_til") or DEFAULT_TIL
    else:
        value = raw
        for prefix in ["🇺🇿 ", "🇷🇺 ", "🇬🇧 "]:
            if value.startswith(prefix):
                value = value[len(prefix):]
                break
        d["til"] = value or DEFAULT_TIL
    await update.message.reply_text("Davomiyligini kiriting (masalan: 1:57:36 yoki shunchaki tire - ):", reply_markup=ReplyKeyboardRemove())
    return VAQT


async def get_vaqt(update, context):
    d = context.user_data
    d["vaqt"] = update.message.text.strip() or DEFAULT_VAQT
    await send_confirm_prompt(update, d)
    return CONFIRM


async def confirm_save(update, context):
    choice = update.message.text.strip()
    if choice == CONFIRM_CANCEL_TEXT:
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=ReplyKeyboardRemove())
        return CONV_END
    if choice != CONFIRM_SAVE_TEXT:
        await update.message.reply_text("Iltimos, tugmadan birini tanlang.")
        return CONFIRM
    d = context.user_data
    code = d["kod"]
    data = {"type": d["file_type"], "file_id": d["file_id"], "nom": d["nom"], "sifat": d["sifat"], "til": d["til"], "vaqt": d["vaqt"]}
    try:
        save_movie(code, data)
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    d["last_sifat"] = d["sifat"]
    d["last_til"] = d["til"]
    d.pop("vaqt_locked", None)
    d.pop("vaqt_draft", None)
    await update.message.reply_text("📁 Jildga saqlashni xohlaysizmi?", reply_markup=get_folder_choice_keyboard())
    return FOLDER_CHOICE


async def finish_movie_save(update, context, folder_note=None):
    d = context.user_data
    note_text = f"\n{folder_note}\n" if folder_note else "\n"
    await update.message.reply_text(
        f"✅ Saqlandi.\n\nKod: {d['kod']}\nNom: {d['nom']}\nSifat: {d['sifat']}\nTil: {d['til']}\nDavomiylik: {d['vaqt']}\n{note_text}Keyingi kino uchun yana video yoki fayl yuboring.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return CONV_END


def get_part_number_in_movies(movies, code):
    for idx, movie in enumerate(movies, start=1):
        if movie["code"] == code:
            return idx
    return None


async def save_to_folder_and_finish(update, context, folder_name):
    d = context.user_data
    code = d["kod"]
    try:
        add_movie_to_folder(folder_name, code)
        movies = get_movies_for_folder(folder_name)
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    part_number = get_part_number_in_movies(movies, code)
    folder_note = f"Jild: {folder_name}\nQism: {part_number}/{len(movies)}" if part_number else f"Jild: {folder_name}"
    return await finish_movie_save(update, context, folder_note=folder_note)


async def handle_folder_choice(update, context):
    choice = update.message.text.strip()
    if choice == FOLDER_SKIP_TEXT:
        return await finish_movie_save(update, context)
    if choice == FOLDER_CREATE_TEXT:
        await update.message.reply_text("Yangi jild nomini yozing:", reply_markup=ReplyKeyboardRemove())
        return FOLDER_CREATE
    if choice == FOLDER_ADD_EXISTING_TEXT:
        try:
            folder_names = get_all_folder_names()
        except Exception:
            await reply_service_unavailable(update)
            return CONV_END
        if not folder_names:
            await update.message.reply_text("Hali jildlar yo'q. Avval yangi jild yarating.", reply_markup=get_folder_choice_keyboard())
            return FOLDER_CHOICE
        await update.message.reply_text("Jildni tanlang:", reply_markup=build_folder_list_keyboard(folder_names))
        return FOLDER_PICK
    await update.message.reply_text("Iltimos, tugmalardan birini tanlang.", reply_markup=get_folder_choice_keyboard())
    return FOLDER_CHOICE


async def handle_folder_create(update, context):
    folder_name = update.message.text.strip()
    if not folder_name:
        await update.message.reply_text("Jild nomi bo'sh bo'lmasin. Qayta kiriting:")
        return FOLDER_CREATE
    try:
        exists = folder_exists_by_name(folder_name)
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    if exists:
        await update.message.reply_text("⚠️ Bu nomli jild bor. Boshqa nom kiriting:")
        return FOLDER_CREATE
    return await save_to_folder_and_finish(update, context, folder_name)


async def handle_folder_pick(update, context):
    value = update.message.text.strip()
    if value == FOLDER_BACK_TEXT:
        await update.message.reply_text("Jildga saqlashni xohlaysizmi?", reply_markup=get_folder_choice_keyboard())
        return FOLDER_CHOICE
    if value == FOLDER_SKIP_TEXT:
        return await finish_movie_save(update, context)
    try:
        folder_names = get_all_folder_names()
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    if value not in folder_names:
        await update.message.reply_text("Ro'yxatdan jild tanlang.", reply_markup=build_folder_list_keyboard(folder_names))
        return FOLDER_PICK
    return await save_to_folder_and_finish(update, context, value)


# ===================== EDIT =====================

async def edit_start(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    await update.message.reply_text("Tahrirlash uchun kino kodini kiriting:")
    return EDIT_KOD


async def edit_get_kod(update, context):
    code = update.message.text.strip()
    try:
        data = get_movie(code)
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    if not data:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return CONV_END
    context.user_data['edit_code'] = code
    context.user_data['current_data'] = data
    await update.message.reply_text(
        f"Joriy ma'lumotlar:\nNom: {data['nom']}\nSifat: {data['sifat']}\nTil: {data['til']}\nDavomiylik: {data['vaqt']}\n\nYangi nomni kiriting (bo'sh qoldiring saqlash uchun):"
    )
    return EDIT_NOM


async def edit_get_nom(update, context):
    new_nom = update.message.text.strip()
    context.user_data['nom'] = new_nom or context.user_data['current_data']['nom']
    await update.message.reply_text("Yangi sifatni tanlang (bo'sh qoldiring saqlash uchun):", reply_markup=get_sifat_keyboard())
    return EDIT_SIFAT


async def edit_get_sifat(update, context):
    new_sifat = update.message.text.strip()
    context.user_data['sifat'] = (new_sifat if new_sifat and new_sifat != KEEP_PREVIOUS_TEXT else context.user_data['current_data']['sifat'])
    await update.message.reply_text("Yangi tilni tanlang (bo'sh qoldiring saqlash uchun):", reply_markup=get_til_keyboard())
    return EDIT_TIL


async def edit_get_til(update, context):
    new_til = update.message.text.strip()
    if new_til and new_til != KEEP_PREVIOUS_TEXT:
        for prefix in ["🇺🇿 ", "🇷🇺 ", "🇬🇧 "]:
            if new_til.startswith(prefix):
                new_til = new_til[len(prefix):]
                break
        context.user_data['til'] = new_til
    else:
        context.user_data['til'] = context.user_data['current_data']['til']
    await update.message.reply_text("Yangi davomiylikni kiriting (bo'sh qoldiring saqlash uchun):", reply_markup=ReplyKeyboardRemove())
    return EDIT_VAQT


async def edit_get_vaqt(update, context):
    new_vaqt = update.message.text.strip()
    context.user_data['vaqt'] = new_vaqt or context.user_data['current_data']['vaqt']
    d = context.user_data
    data = {"type": d['current_data']['type'], "file_id": d['current_data']['file_id'], "nom": d['nom'], "sifat": d['sifat'], "til": d['til'], "vaqt": d['vaqt']}
    try:
        save_movie(d['edit_code'], data)
    except Exception:
        await reply_service_unavailable(update)
        return CONV_END
    await update.message.reply_text("✅ Tahrirlandi!", reply_markup=get_admin_menu_keyboard())
    return CONV_END


# ===================== ADMIN BUYRUQLAR =====================

async def delete_movie(update, context):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Ishlatish: /delete <kod>")
        return
    code = context.args[0]
    try:
        exists = movie_exists(code)
    except Exception:
        await reply_service_unavailable(update)
        return
    if not exists:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return
    try:
        delete_movie_db(code)
    except Exception:
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"🗑️ {code} kodli kino o'chirildi.")


async def show_user_count(update, context):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args or context.args[0] != "777":
        await update.message.reply_text("Ishlatish: /foydalanuvchi 777")
        return
    try:
        total_users = get_tracked_user_count()
    except Exception:
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"Foydalanuvchilar soni: {total_users}")


async def list_series_ranges(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        ranges = get_all_series_ranges()
    except Exception:
        await reply_service_unavailable(update)
        return
    if not ranges:
        await update.message.reply_text("Hali birorta ham serial diapazon saqlanmagan.")
        return
    lines = ["Serial diapazonlari:"]
    for item in ranges:
        lines.append(f"{item['start_code_num']}-{item['end_code_num']} | {item['title']}")
    await update.message.reply_text("\n".join(lines))


# ===================== CALLBACKS =====================

async def handle_series_part_callback(update, context):
    remember_user(update)
    query = update.callback_query
    if query is None or query.message is None:
        return
    await query.answer()
    code = query.data[len(SERIES_CALLBACK_PREFIX):]
    user_id = update.effective_user.id if update.effective_user else None
    try:
        data = get_movie(code)
    except Exception:
        await reply_service_unavailable(update)
        return
    if not data:
        await query.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return
    increment_view_count(code)
    await send_movie_to_chat(query.message, code, data, user_id=user_id)


async def handle_favorite_callback(update, context):
    remember_user(update)
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    user_id = update.effective_user.id
    code = query.data[len("fav:"):]
    try:
        in_fav = is_favorite(user_id, code)
        if in_fav:
            remove_from_favorites(user_id, code)
            new_text = "🤍 Sevimlilarga qo'shish"
            notice = "Sevimlilardan olib tashlandi."
        else:
            add_to_favorites(user_id, code)
            new_text = "❤️ Sevimlilardan chiqarish"
            notice = "❤️ Sevimlilarga qo'shildi!"
    except Exception:
        await query.answer("Xato yuz berdi. Keyinroq urinib ko'ring.", show_alert=True)
        return
    try:
        old_markup = query.message.reply_markup
        if old_markup:
            new_rows = []
            for row in old_markup.inline_keyboard:
                new_rows.append([
                    InlineKeyboardButton(new_text, callback_data=query.data) if btn.callback_data == query.data else btn
                    for btn in row
                ])
            await query.message.edit_reply_markup(InlineKeyboardMarkup(new_rows))
    except Exception:
        pass
    await query.answer(notice, show_alert=True)


# ===================== SEVIMLILAR =====================

async def show_favorites(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    try:
        fav_codes = get_favorites(user_id)
    except Exception:
        await reply_service_unavailable(update)
        return
    if not fav_codes:
        await update.message.reply_text("Sevimlilar ro'yxatingiz hali bo'sh.\n\nKino ko'rganingizda pastdagi 🤍 tugmani bosib qo'shing!")
        return
    try:
        movies_info = run_db(lambda col: list(col.find({"code": {"$in": fav_codes}}, {"_id": 0, "code": 1, "nom": 1})))
    except Exception:
        await reply_service_unavailable(update)
        return
    code_to_nom = {m["code"]: m.get("nom", "-") for m in movies_info}
    lines = [f"❤️ Sevimli kinolaringiz ({len(fav_codes)} ta):\n"]
    for i, code in enumerate(fav_codes, start=1):
         lines.append(f"{i}. {code_to_nom.get(code, \"Noma'lum\")}  |  Kod: {code}")
    lines.append("\nKino olish uchun kodini yuboring.")
    await update.message.reply_text("\n".join(lines))


# ===================== KO'RILISH SONI =====================

def increment_view_count(code):
    try:
        run_db(lambda col: col.update_one({"code": code}, {"$inc": {"views": 1}}))
    except Exception:
        pass


# ===================== BROADCAST =====================

async def handle_admin_broadcast_message(update, context):
    global _broadcast_active
    if not _broadcast_active:
        return
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        user_ids = get_all_user_ids()
    except Exception:
        await reply_service_unavailable(update)
        return
    if not user_ids:
        _broadcast_active = False
        await update.message.reply_text("Bazada foydalanuvchilar topilmadi.", reply_markup=get_admin_menu_keyboard())
        return
    sent = failed = blocked = 0
    for uid in user_ids:
        try:
            await update.message.copy_to(int(uid))
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ["blocked", "deactivated", "not found", "chat not found"]):
                blocked += 1
            else:
                logger.warning(f"Broadcast xatosi (uid={uid}): {e}")
                failed += 1
    _broadcast_active = False
    lines = ["✅ Xabar yuborildi!", f"Muvaffaqiyatli: {sent} ta"]
    if blocked:
        lines.append(f"Bot bloklagan: {blocked} ta")
    if failed:
        lines.append(f"Boshqa xato: {failed} ta")
    await update.message.reply_text("\n".join(lines), reply_markup=get_admin_menu_keyboard())


# ===================== ASOSIY MESSAGE HANDLER =====================

async def handle_message(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    if user_id == ADMIN_ID and _broadcast_active:
        await handle_admin_broadcast_message(update, context)
        return

    if user_id != ADMIN_ID:
        verified, detail = is_user_verified(user_id)

        if not verified:
            if detail == "not_started":
                await update.message.reply_text(
                    "⚠️ Botdan foydalanish uchun avval quyidagi botga obuna bo'ling va /start bosing:\n\n"
                    "⬇️ Tugmani bosing:",
                    reply_markup=get_verification_keyboard(),
                )
                await update.message.reply_text(
                    f"⏳ Obuna bo'lgandan so'ng {VERIFICATION_WAIT_SECONDS} soniya kuting va qayta yuboring."
                )
                return
            else:
                remaining = VERIFICATION_WAIT_SECONDS - detail
                await update.message.reply_text(
                    f"⏳ Iltimos, yana {remaining} soniya kuting va qayta yuboring."
                )
                return

    if text.isdigit():
        code = text

        try:
            folder_data = get_folder_by_code(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if folder_data is not None:
            try:
                folder_movies = get_movies_for_folder(folder_data["name"])
            except Exception:
                await reply_service_unavailable(update)
                return
            if folder_movies:
                await send_folder_parts_prompt(update.message, folder_data, folder_movies)
                return

        try:
            series_data = get_series_range_by_code(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if series_data is not None:
            try:
                movies = get_movies_in_range(series_data["start_code_num"], series_data["end_code_num"])
            except Exception:
                await reply_service_unavailable(update)
                return
            if movies:
                await send_series_parts_prompt(update.message, series_data, movies)
                return

        try:
            data = get_movie(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if not data:
            await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
            return
        increment_view_count(code)
        await send_movie_to_chat(update.message, code, data, user_id=user_id)
        return

    if len(text) < 2:
        await update.message.reply_text(
            "🔍 Kino kodini (raqam) yoki nomini yozing.\nMasalan: 25 yoki Ronaldo"
        )
        return

    try:
        results = search_movies_by_name(text, limit=15)
    except Exception:
        await reply_service_unavailable(update)
        return

    if not results:
        await update.message.reply_text(
            f"🔍 «{text}» bo'yicha hech narsa topilmadi.\n\nBoshqa nom yoki kino kodini kiriting."
        )
        return

    if len(results) == 1:
        code = results[0]["code"]
        try:
            data = get_movie(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if data:
            increment_view_count(code)
            await send_movie_to_chat(update.message, code, data, user_id=user_id)
        return

    lines = [f"🔍 «{text}» bo'yicha {len(results)} ta kino topildi:\n"]
    for movie in results:
        lines.append(f"🎬 {movie.get('nom', '-')}\n   🎥 {movie.get('sifat', '-')} | 🌐 {movie.get('til', '-')} | 🆔 Kod: {movie.get('code', '-')}\n")
    lines.append("Kino olish uchun uning kodini yuboring.")
    await update.message.reply_text("\n".join(lines))


# ===================== BUILD APPLICATION =====================

def build_application():
    ensure_telegram_imports()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN topilmadi.")

    try:
        _init_db_client()
        client.admin.command("ping")
        set_health_state(db="connected", last_error="")
        logger.info("MongoDB muvaffaqiyatli ulandi.")
    except Exception as exc:
        logger.error(f"MongoDB boshlang'ich ulanish xatosi: {exc}")
        set_health_state(db="error", last_error=str(exc))

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(log_error)

    # BUG FIX: ConversationHandler.END o'rniga CONV_END ishlatilgani uchun
    # bu yerda ham ConversationHandler.END ni CONV_END bilan bir xil qilib belgilash kerak
    # python-telegram-bot da END = -1, shuning uchun CONV_END = -1 to'g'ri ishlaydi

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.VIDEO & filters.User(ADMIN_ID), handle_video),
            MessageHandler(filters.Document.ALL & filters.User(ADMIN_ID), handle_document),
        ],
        states={
            KOD_VAQT:      [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_kod_vaqt)],
            NOM:           [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_nom)],
            SIFAT:         [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_sifat)],
            TIL:           [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_til)],
            VAQT:          [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_vaqt)],
            CONFIRM:       [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), confirm_save)],
            FOLDER_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_choice)],
            FOLDER_CREATE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_create)],
            FOLDER_PICK:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_pick)],
        },
        fallbacks=[],
    )

    edit_conv = ConversationHandler(
        entry_points=[CommandHandler("edit", edit_start)],
        states={
            EDIT_KOD:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_kod)],
            EDIT_NOM:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_nom)],
            EDIT_SIFAT:[MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_sifat)],
            EDIT_TIL:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_til)],
            EDIT_VAQT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_vaqt)],
        },
        fallbacks=[],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("delete", delete_movie))
    app.add_handler(CommandHandler("foydalanuvchi", show_user_count))
    app.add_handler(CommandHandler("seriallist", list_series_ranges))
    app.add_handler(CommandHandler("sevimli", show_favorites))

    app.add_handler(conv)
    app.add_handler(edit_conv)

    # BUG FIX: Broadcast handler — foto/audio/voice/sticker uchun
    app.add_handler(MessageHandler(
        (filters.PHOTO | filters.AUDIO | filters.VOICE | filters.Sticker.ALL | filters.VIDEO_NOTE | filters.ANIMATION) & filters.User(ADMIN_ID),
        handle_admin_broadcast_message,
    ))

    app.add_handler(CallbackQueryHandler(handle_series_part_callback, pattern=f"^{SERIES_CALLBACK_PREFIX}"))
    app.add_handler(CallbackQueryHandler(handle_favorite_callback, pattern="^fav:"))

    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    return app


def run_bot_forever():
    while True:
        try:
            asyncio.set_event_loop(asyncio.new_event_loop())
            set_health_state(bot="starting", last_error="")
            app = build_application()
            logger.info("Bot ishga tushdi...")
            set_health_state(bot="running", last_error="")
            app.run_polling(bootstrap_retries=-1, allowed_updates=["message", "callback_query"], drop_pending_updates=True)
            logger.warning("Polling to'xtadi. 5 soniyadan keyin qayta ishga tushadi.")
            set_health_state(bot="stopped")
        except Exception as exc:
            logger.exception("Bot ishida xato yuz berdi.")
            set_health_state(bot="error", last_error=str(exc))
        time.sleep(5)


def main():
    if os.environ.get("PORT"):
        keep_alive()
        set_health_state(service="running", bot="starting", db="unknown", last_error="")
    else:
        logger.info("PORT topilmadi. Bot worker rejimida ishga tushmoqda.")
        set_health_state(service="worker", bot="starting", db="unknown", last_error="")
    run_bot_forever()


if __name__ == "__main__":
    main()
