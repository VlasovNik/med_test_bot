import os
import random
import re
import telebot
from telebot import types
import sqlite3
import atexit
import signal
import pytz
from datetime import datetime
import sys
import time
import requests
from requests.exceptions import ConnectionError, Timeout
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone as pytz_timezone
import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import Optional, Dict, List
import shutil
import yookassa
from yookassa import Payment, Configuration
from yookassa.domain.notification import WebhookNotificationEventType, WebhookNotificationFactory
import uuid
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from threading import Lock  # для потокобезопасности

# Загрузка переменных окружения
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# ДОПОЛНИТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ УДОБНОГО ЛОГИРОВАНИЯ
# ============================================================================
def setup_logging():
    """Настройка системы логирования"""
    # Создаем папку /data если её нет
    log_dir = 'data'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
        print(f"✅ Создана папка {log_dir}")

    # Правильный путь к файлу логов
    log_file = os.path.join(log_dir, 'bot.log')

    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            RotatingFileHandler(
                log_file,  # Теперь это правильный путь: data/bot.log
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5,
                encoding='utf-8'
            ),
            logging.StreamHandler()  # Также выводим в консоль
        ]
    )

    print(f"✅ Логирование настроено. Файл логов: {log_file}")

def log_user_action(user_id: int, action: str, details: str = ""):
    """Логирование действий пользователя"""
    user_info = db.get_user(user_id)
    username = f"@{user_info.get('username', 'нет')}" if user_info else "неизвестен"
    log_msg = f"👤 Пользователь {user_id} ({username}): {action}"
    if details:
        log_msg += f" - {details}"
    logger.info(log_msg)

setup_logging()
logger = logging.getLogger(__name__)
# ============================================================================
# КОНСТАНТЫ И КОНФИГУРАЦИЯ
# ============================================================================
TOKEN = os.getenv('BOT_TOKEN')
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения!")

# Конфигурация ЮKassa - одна цена
SUBSCRIPTION_PRICE = 69  # Одна цена: 69 рублей за месяц
SUBSCRIPTION_DAYS = 30    # 30 дней подписка

# Ключи ЮKassa
YOOKASSA_SHOP_ID = os.getenv('YOOKASSA_SHOP_ID')
YOOKASSA_SECRET_KEY = os.getenv('YOOKASSA_SECRET_KEY')

# Настройка ЮKassa
if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
    try:
        Configuration.account_id = YOOKASSA_SHOP_ID
        Configuration.secret_key = YOOKASSA_SECRET_KEY
        logger.info(f"✅ ЮKassa настроена. Цена подписки: {SUBSCRIPTION_PRICE}₽")
    except Exception as e:
        logger.info(f"⚠️ Ошибка настройки ЮKassa: {e}")
else:
    logger.info("⚠️ ЮKassa не настроена (отсутствуют SHOP_ID или SECRET_KEY)")

bot = telebot.TeleBot(TOKEN)
NOVOSIBIRSK_TZ = pytz_timezone('Asia/Novosibirsk')
# Настройка для telebot
#telebot.apihelper.API_URL = "https://api.telegram.org/bot{0}/{1}"
telebot.apihelper.SESSION_TIME_TO_LIVE = 5 * 60

# ============================================================================
# УТИЛИТЫ
# ============================================================================
def setup_retry_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def validate_user_id(user_id):
    """Валидация ID пользователя"""
    try:
        user_id_int = int(user_id)
        if user_id_int <= 0:
            raise ValueError("ID должен быть положительным числом")
        if user_id_int > 2**63 - 1:  # Максимальный для Telegram
            raise ValueError("ID слишком большой")
        return user_id_int
    except (ValueError, TypeError):
        raise ValueError(f"Некорректный ID пользователя: {user_id}")

def validate_days(days):
    """Валидация количества дней"""
    try:
        days_int = int(days)
        if days_int < 0:
            raise ValueError("Количество дней не может быть отрицательным")
        if days_int > 3650:  # 10 лет максимум
            raise ValueError("Слишком большой срок")
        return days_int
    except (ValueError, TypeError):
        raise ValueError(f"Некорректное количество дней: {days}")

def answer_callback_safe(bot_instance, call_id, text=None, show_alert=False):
    """Безопасный ответ на callback query"""
    try:
        if text:
            bot_instance.answer_callback_query(call_id, text=text, show_alert=show_alert)
        else:
            bot_instance.answer_callback_query(call_id)
        return True
    except Exception as e:
        logger.warning(f"⚠️ Не удалось ответить на callback {call_id}: {e}")
        return False

    def send_message_async(chat_id, text, parse_mode=None, reply_markup=None):
        """Асинхронная отправка сообщений без блокировки основного потока"""
        import threading

        def send():
            try:
                bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True  # Ускоряет отправку
                )
            except Exception as e:
                logger.error(f"Ошибка асинхронной отправки: {e}")

        thread = threading.Thread(target=send)
        thread.daemon = True  # Поток завершится с основным
        thread.start()

# ============================================================================
# КЛАСС ДЛЯ УПРАВЛЕНИЯ ДАННЫМИ ПОЛЬЗОВАТЕЛЕЙ С TTL
# ============================================================================
class CacheManager:
    """Менеджер кеширования для ускорения работы"""

    def __init__(self, ttl_seconds=300):
        self.cache = {}
        self.ttl = ttl_seconds

    def get(self, key):
        """Получение значения из кеша"""
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[key]  # Удаляем просроченный кеш
        return None

    def set(self, key, value):
        """Установка значения в кеш"""
        self.cache[key] = (value, time.time())

    def delete(self, key):
        """Удаление значения из кеша"""
        self.cache.pop(key, None)

    def clear(self):
        """Очистка кеша"""
        self.cache.clear()
# ============================================================================
# ЛИМИТЫ ЗАПРОСОВ
# ============================================================================
class RateLimiter:
    def __init__(self, max_requests=10, per_seconds=60):
        self.requests = {}
        self.callback_requests = {}  # ОТДЕЛЬНО ДЛЯ CALLBACK
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.lock = Lock()

    def check(self, user_id):
        """Проверка лимита для сообщений"""
        with self.lock:
            return self._check_impl(user_id, self.requests)

    def check_callback(self, user_id):
        """Проверка лимита для callback-запросов (более щадящий)"""
        with self.lock:
            return self._check_impl(user_id, self.callback_requests, max_reqs=20)  # 20 запросов в минуту

    def _check_impl(self, user_id, storage, max_reqs=None):
        """Общая реализация проверки"""
        current_time = time.time()
        max_allowed = max_reqs or self.max_requests

        if user_id not in storage:
            storage[user_id] = []

        # Очищаем старые запросы
        storage[user_id] = [
            req_time for req_time in storage[user_id]
            if current_time - req_time < self.per_seconds
        ]

        # Проверяем лимит
        if len(storage[user_id]) >= max_allowed:
            return False

        # Добавляем новый запрос
        storage[user_id].append(current_time)
        return True
# ============================================================================
# КЕШИРОВАНИЕ ДАННЫХ
# ============================================================================
class UserDataManager:
    """Менеджер данных пользователей с автоматической очисткой"""

    def __init__(self, ttl_minutes=180, cleanup_interval_minutes=30):
        self.user_data = {}
        self.session_stats = {}
        self.broadcast_states = {}
        self.extend_states = {}
        self.ttl = ttl_minutes * 60  # в секундах
        self.last_cleanup = time.time()
        self.cleanup_interval = cleanup_interval_minutes * 60

    def cleanup_old_data(self):
        """Очистка устаревших данных"""
        current_time = time.time()

        if current_time - self.last_cleanup < self.cleanup_interval:
            return

        logger.info("🧹 Запуск очистки устаревших данных пользователей...")

        # Очищаем user_data
        to_remove = []
        for user_id, data in self.user_data.items():
            if 'last_access' in data and current_time - data['last_access'] > self.ttl:
                to_remove.append(user_id)

        for user_id in to_remove:
            del self.user_data[user_id]

        # Очищаем session_stats
        to_remove = []
        for user_id in self.session_stats:
            if user_id not in self.user_data:  # Удаляем если нет в user_data
                to_remove.append(user_id)

        for user_id in to_remove:
            del self.session_stats[user_id]

        # Очищаем broadcast_states и extend_states
        for state_dict in [self.broadcast_states, self.extend_states]:
            to_remove = []
            for user_id, state in state_dict.items():
                if 'timestamp' in state and current_time - state['timestamp'] > self.ttl:
                    to_remove.append(user_id)

            for user_id in to_remove:
                del state_dict[user_id]

        self.last_cleanup = current_time
        logger.info(f"✅ Очищено: user_data={len(to_remove)}, осталось: user_data={len(self.user_data)}")

    def get_user_data(self, user_id):
        """Получение данных пользователя с обновлением времени доступа"""
        self.cleanup_old_data()

        if user_id not in self.user_data:
            self.user_data[user_id] = {
                'current_topic': None,
                'current_question': None,
                'correct_answer': None,
                'numbered_answers': {},
                'answers_list': [],
                'last_access': time.time(),
                # НОВОЕ: инициализация структур для отслеживания вопросов
                'answered_questions': {},  # {topic: [question_texts...]}
                'session_questions': {},  # {topic: {question_text: answered_correctly}}
                'current_question_topic': None
            }
        else:
            self.user_data[user_id]['last_access'] = time.time()

        return self.user_data[user_id]

    def update_user_data(self, user_id, **kwargs):
        """Обновление данных пользователя"""
        data = self.get_user_data(user_id)
        data.update(kwargs)
        data['last_access'] = time.time()

    def get_session_stats(self, user_id):
        """Получение статистики сессии"""
        self.cleanup_old_data()

        if user_id not in self.session_stats:
            self.session_stats[user_id] = {
                'session_total': 0,
                'session_correct': 0,
                'last_access': time.time()
            }
        else:
            self.session_stats[user_id]['last_access'] = time.time()

        return self.session_stats[user_id]

    def clear_user_data(self, user_id):
        """Очистка всех данных пользователя"""
        for dict_name in [self.user_data, self.session_stats,
                          self.broadcast_states, self.extend_states]:
            dict_name.pop(user_id, None)

    def get_memory_usage(self):
        """Оценка использования памяти"""
        import sys
        total_size = 0

        for obj in [self.user_data, self.session_stats,
                    self.broadcast_states, self.extend_states]:
            total_size += sys.getsizeof(obj)

        return total_size / 1024 / 1024  # в МБ

    # НОВЫЕ МЕТОДЫ ДЛЯ ЛОГИКИ СЕССИЙ ВОПРОСОВ

    def get_session_questions(self, user_id, topic):
        """Получение вопросов текущей сессии для темы"""
        data = self.get_user_data(user_id)
        if 'session_questions' not in data:
            data['session_questions'] = {}
        if topic not in data['session_questions']:
            data['session_questions'][topic] = {}
        return data['session_questions'][topic]

    def get_answered_questions(self, user_id, topic):
        """Получение правильных ответов для темы"""
        data = self.get_user_data(user_id)
        if 'answered_questions' not in data:
            data['answered_questions'] = {}
        if topic not in data['answered_questions']:
            data['answered_questions'][topic] = []
        return data['answered_questions'][topic]

    def mark_question_answered(self, user_id, topic, question_text, is_correct):
        """Отметка вопроса как отвеченного"""
        session_questions = self.get_session_questions(user_id, topic)

        if is_correct:
            # Если ответ правильный, добавляем в список отвеченных
            answered_questions = self.get_answered_questions(user_id, topic)
            if question_text not in answered_questions:
                answered_questions.append(question_text)
            # В сессии отмечаем как правильно отвеченный
            session_questions[question_text] = True
        else:
            # Если ответ неправильный, отмечаем в сессии
            session_questions[question_text] = False

    def clear_topic_session(self, user_id, topic):
        """Очистка сессии для темы"""
        data = self.get_user_data(user_id)
        if 'session_questions' not in data:
            data['session_questions'] = {}
        if topic in data['session_questions']:
            data['session_questions'][topic] = {}
# ============================================================================
# КЛАСС БАЗЫ ДАННЫХ
# ============================================================================
class Database:
    def __init__(self, db_path: str = 'data/users.db'):
            self.db_path = db_path
            self.conn = None  # ВАЖНО: добавляем этот атрибут
            self.create_data_directory()
            self.init_database()
            self.upgrade_database()
            logger.info(f"✅ База данных инициализирована: {self.db_path}")

    def get_connection(self) -> sqlite3.Connection:
        """Получение соединения с базой данных"""
        # Простая версия - всегда возвращаем новое соединение
        conn = sqlite3.connect(self.db_path)

        # Добавляем оптимизации для производительности
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=10000')
            conn.execute('PRAGMA temp_store=MEMORY')
        except:
            pass  # Игнорируем ошибки если не поддерживается

        return conn
    def upgrade_database(self):
        """Безопасное обновление схемы базы данных"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Проверяем существование колонки безопасным способом
            cursor.execute("PRAGMA table_info(users)")
            columns = {column[1]: column for column in cursor.fetchall()}

            if 'subscription_purchased' not in columns:
                # Проверяем версию SQLite
                cursor.execute("SELECT sqlite_version()")
                sqlite_version = cursor.fetchone()[0]
                logger.info(f"🔄 SQLite версия: {sqlite_version}")

                # Безопасное добавление колонки с обработкой ошибок
                try:
                    cursor.execute('''
                    ALTER TABLE users 
                    ADD COLUMN subscription_purchased BOOLEAN DEFAULT FALSE
                    ''')
                    conn.commit()
                    logger.info("✅ Колонка subscription_purchased добавлена")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e).lower():
                        logger.info("ℹ️ Колонка subscription_purchased уже существует")
                    else:
                        raise

            conn.close()

        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка при обновлении базы данных: {e}")

    def create_data_directory(self):
        """Создание директории для данных"""
        data_dir = os.path.dirname(self.db_path)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)

    def init_database(self):
        """Инициализация базы данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Таблица пользователей
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                subscription_paid BOOLEAN DEFAULT FALSE,
                subscription_start_date TIMESTAMP,  -- Изменено на TIMESTAMP
                subscription_end_date TIMESTAMP,    -- Изменено на TIMESTAMP
                is_admin BOOLEAN DEFAULT FALSE,
                is_trial_used BOOLEAN DEFAULT FALSE,
                last_warning_date DATE,
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Таблица статистики
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                telegram_id INTEGER PRIMARY KEY,
                total_answers INTEGER DEFAULT 0,
                correct_answers INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id) ON DELETE CASCADE
            )
            ''')

            # ТАБЛИЦА ПЛАТЕЖЕЙ - ИСПРАВЛЕННАЯ ВЕРСИЯ
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                payment_id TEXT PRIMARY KEY,
                telegram_id INTEGER NOT NULL,
                amount REAL DEFAULT 69.00,
                description TEXT,  -- ДОБАВЛЕНО ОПИСАНИЕ
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                paid_at TIMESTAMP,
                is_processed BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id) ON DELETE CASCADE
            )
            ''')

            conn.commit()
            conn.close()

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при создании базы данных: {e}")


    def add_user(self, telegram_id: int, username=None, first_name=None, last_name=None, is_admin=False) -> bool:
        """Добавление пользователя"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('SELECT telegram_id FROM users WHERE telegram_id = ?', (telegram_id,))
            if cursor.fetchone():
                cursor.execute('''
                UPDATE users 
                SET username = ?, first_name = ?, last_name = ?, last_activity = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (username, first_name, last_name, telegram_id))
            else:
                cursor.execute('''
                INSERT INTO users (telegram_id, username, first_name, last_name, is_admin, registration_date)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (telegram_id, username, first_name, last_name, is_admin))

            conn.commit()
            conn.close()
            return True

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при добавлении пользователя: {e}")
            return False

    def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получение информации о пользователе с кешированием"""
        cache_key = f"user_{telegram_id}"

        # Пробуем получить из кеша
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM users WHERE telegram_id = ?', (telegram_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                result = dict(row)
                cache.set(cache_key, result)
                return result

            cache.set(cache_key, None)
            return None

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при получении пользователя: {e}")
            return None

    def check_subscription(self, telegram_id: int) -> bool:
        """Проверка подписки с корректной обработкой временных зон"""
        try:
            user = self.get_user(telegram_id)
            if not user:
                return False

            if user.get('is_admin'):
                return True

            if not user.get('subscription_paid'):
                return False

            end_date_str = user.get('subscription_end_date')
            if not end_date_str:
                return False

            # ВСЕГДА ХРАНИМ В UTC И РАБОТАЕМ С UTC
            try:
                # Парсим как naive
                end_naive = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    end_naive = datetime.strptime(end_date_str, '%Y-%m-%d')
                    # Добавляем время 23:59:59 для совместимости
                    end_naive = end_naive.replace(hour=23, minute=59, second=59)
                except ValueError:
                    return False

            # Предполагаем, что в БД время в UTC, и делаем его aware
            end_aware = pytz.UTC.localize(end_naive)
            now_aware = datetime.now(pytz.UTC)

            return end_aware > now_aware

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке подписки: {e}")
            return False

    def update_subscription(self, telegram_id: int, paid_status=True, end_datetime=None,
                            is_trial=False, is_purchased=False, conn=None) -> bool:
        """Обновление подписки - ВСЕ ДАТЫ В UTC"""
        close_conn = False
        if conn is None:
            conn = self.get_connection()
            close_conn = True

        try:
            cursor = conn.cursor()
            updated = False  # Флаг, было ли обновление

            if end_datetime:
                # УБЕЖДАЕМСЯ, ЧТО ДАТА В UTC
                if hasattr(end_datetime, 'tzinfo') and end_datetime.tzinfo is not None:
                    # Конвертируем в UTC если нужно
                    end_datetime = end_datetime.astimezone(pytz.UTC)
                else:
                    # Если naive - считаем что это UTC
                    end_datetime = pytz.UTC.localize(end_datetime)

                # ХРАНИМ В БД БЕЗ ЧАСОВОГО ПОЯСА (naive), но В UTC
                end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

                # Текущее время в UTC
                now_aware = datetime.now(pytz.UTC)
                start_str = now_aware.strftime('%Y-%m-%d %H:%M:%S')

                cursor.execute('''
                UPDATE users 
                SET subscription_paid = ?,
                    subscription_start_date = ?,
                    subscription_end_date = ?,
                    is_trial_used = ?,
                    subscription_purchased = ?,
                    last_activity = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (paid_status, start_str, end_str, is_trial, is_purchased, telegram_id))

                updated = True

            if close_conn:
                conn.commit()
                conn.close()

            if updated:  # ✅ Логируем ТОЛЬКО если было обновление
                # Инвалидация кеша - удаляем оба ключа по одному разу
                cache.delete(f"user_{telegram_id}")
                cache.delete(f"subscription_{telegram_id}")
                logger.info(f"✅ Подписка пользователя {telegram_id} обновлена")

            return True

        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка SQLite при обновлении подписки для {telegram_id}: {e}")
            if close_conn and conn:
                try:
                    conn.close()
                except:
                    pass
            return False
        except Exception as e:
            logger.error(f"❌ Общая ошибка при обновлении подписки для {telegram_id}: {e}")
            if close_conn and conn:
                try:
                    conn.close()
                except:
                    pass
            return False

    def update_activity(self, telegram_id: int) -> bool:
        """Обновление времени последней активности"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
            UPDATE users 
            SET last_activity = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            ''', (telegram_id,))

            conn.commit()
            conn.close()
            return True

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при обновлении активности: {e}")
            return False

    def get_user_statistics(self, telegram_id: int) -> Optional[Dict]:
        """Получение статистики пользователя"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM statistics WHERE telegram_id = ?', (telegram_id,))
            row = cursor.fetchone()

            if row:
                stats = dict(row)
                conn.close()
                return stats

            # Создаем запись если её нет
            self.init_user_statistics(telegram_id)

            # Получаем созданную запись
            cursor.execute('SELECT * FROM statistics WHERE telegram_id = ?', (telegram_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                return dict(row)

            return {
                'telegram_id': telegram_id,
                'total_answers': 0,
                'correct_answers': 0,
                'last_updated': datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
            }

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при получении статистики: {e}")
            return None

    def init_user_statistics(self, telegram_id: int) -> bool:
        """Инициализация статистики пользователя"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Проверяем, существует ли уже запись
            cursor.execute('SELECT telegram_id FROM statistics WHERE telegram_id = ?', (telegram_id,))
            if cursor.fetchone():
                conn.close()
                return True

            # Создаем новую запись
            cursor.execute('''
            INSERT INTO statistics (telegram_id, total_answers, correct_answers, last_updated)
            VALUES (?, 0, 0, CURRENT_TIMESTAMP)
            ''', (telegram_id,))

            conn.commit()
            conn.close()
            return True

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при инициализации статистики: {e}")
            return False

    def update_statistics(self, telegram_id: int, is_correct: bool) -> bool:
        """Обновление статистики"""
        try:
            self.init_user_statistics(telegram_id)

            conn = self.get_connection()
            cursor = conn.cursor()

            if is_correct:
                cursor.execute('''
                UPDATE statistics 
                SET total_answers = total_answers + 1,
                    correct_answers = correct_answers + 1,
                    last_updated = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (telegram_id,))
            else:
                cursor.execute('''
                UPDATE statistics 
                SET total_answers = total_answers + 1,
                    last_updated = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (telegram_id,))

            conn.commit()
            conn.close()
            return True

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при обновлении статистики: {e}")
            return False

    def get_admin_ids(self) -> List[int]:
        """Получение ID администраторов"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('SELECT telegram_id FROM users WHERE is_admin = TRUE')
            admin_ids = [row[0] for row in cursor.fetchall()]
            conn.close()

            return admin_ids

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при получении администраторов: {e}")
            return []

    def get_all_users(self) -> List[Dict]:
        """Получение всех пользователей"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM users ORDER BY registration_date DESC')
            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при получении списка пользователей: {e}")
            return []

    def get_all_statistics(self) -> List[Dict]:
        """Получение статистики всех пользователей"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
            SELECT s.telegram_id, s.total_answers, s.correct_answers, 
                   s.last_updated, u.username, u.first_name, u.last_name
            FROM statistics s
            LEFT JOIN users u ON s.telegram_id = u.telegram_id
            ORDER BY s.correct_answers DESC, s.total_answers DESC
            ''')

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при получении всей статистики: {e}")
            return []

    def get_top_users(self, limit=10) -> List[Dict]:
        """Получение топа пользователей"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
            SELECT 
                s.telegram_id,
                s.total_answers,
                s.correct_answers,
                u.username,
                u.first_name,
                u.last_name,
                CASE WHEN s.total_answers > 0 THEN 
                    ROUND(CAST(s.correct_answers AS FLOAT) / s.total_answers * 100, 1)
                ELSE 0 END as success_rate
            FROM statistics s
            LEFT JOIN users u ON s.telegram_id = u.telegram_id
            WHERE s.total_answers > 0
            ORDER BY 
                success_rate DESC,
                s.correct_answers DESC,
                s.total_answers DESC
            LIMIT ?
            ''', (limit,))

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при получении топа: {e}")
            return []

    def reset_user_statistics(self, telegram_id: int) -> bool:
        """Сброс статистики пользователя"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Сначала проверяем существование записи
            cursor.execute('SELECT telegram_id FROM statistics WHERE telegram_id = ?', (telegram_id,))
            if not cursor.fetchone():
                # Создаем запись если её нет
                cursor.execute('''
                INSERT INTO statistics (telegram_id, total_answers, correct_answers, last_updated)
                VALUES (?, 0, 0, CURRENT_TIMESTAMP)
                ''', (telegram_id,))
            else:
                # Обновляем существующую
                cursor.execute('''
                UPDATE statistics 
                SET total_answers = 0,
                    correct_answers = 0,
                    last_updated = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (telegram_id,))

            conn.commit()
            conn.close()
            logger.info(f"✅ Статистика пользователя {telegram_id} сброшена")
            return True

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при сбросе статистики: {e}")
            return False

    def is_payment_processed(self, payment_id: str) -> bool:
        """Проверка, был ли платеж уже обработан"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
            SELECT is_processed FROM payments WHERE payment_id = ?
            ''', (payment_id,))

            result = cursor.fetchone()
            conn.close()

            return result and result[0] == 1
        except Exception as e:
            logger.error(f"Ошибка при проверке платежа {payment_id}: {e}")
            return False

    def get_payment_by_external_id(self, external_id: str):
        """Получение платежа по внешнему ID"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
            SELECT * FROM payments WHERE payment_id = ?
            ''', (external_id,))

            result = cursor.fetchone()
            conn.close()

            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Ошибка при получении платежа {external_id}: {e}")
            return None

    def set_admin(self, telegram_id: int, is_admin: bool = True) -> bool:
        """Назначение/снятие администратора"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
            UPDATE users 
            SET is_admin = ?,
                last_activity = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            ''', (is_admin, telegram_id))

            conn.commit()
            conn.close()

            status = "назначен" if is_admin else "снят"
            logger.info(f"✅ Пользователь {telegram_id} {status} администратором")
            return True

        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при изменении прав администратора: {e}")
            return False

    def grant_subscription(self, telegram_id: int, days: int = 30) -> bool:
        """Выдача подписки пользователю с точным временем"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            start_datetime = datetime.now(pytz.UTC)
            end_datetime = datetime.now(pytz.UTC) + timedelta(days=days)

            start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute('''
            UPDATE users 
            SET subscription_paid = TRUE,
                subscription_start_date = ?,
                subscription_end_date = ?,
                is_trial_used = FALSE,
                subscription_purchased = TRUE,  -- ✅ ВАЖНО: помечаем как купленную
                last_activity = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            ''', (start_str, end_str, telegram_id))

            conn.commit()
            conn.close()

            # ✅ Очищаем кэш
            cache.delete(f"user_{telegram_id}")
            cache.delete(f"subscription_{telegram_id}")

            logger.info(f"✅ Пользователю {telegram_id} выдана подписка до {end_str}")
            return True

        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка при выдаче подписки: {e}")
            return False

    def extend_subscription(self, telegram_id: int, hours: int = 0, days: int = 0) -> bool:
        """Продление подписки пользователю - ИСПРАВЛЕНО: ВСЕ В UTC"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем текущие данные подписки
            cursor.execute('''
            SELECT subscription_end_date, subscription_paid 
            FROM users 
            WHERE telegram_id = ?
            ''', (telegram_id,))

            result = cursor.fetchone()
            if not result:
                conn.close()
                return False

            current_end_date_str, subscription_paid = result

            # Текущее время в UTC
            now_utc = datetime.now(pytz.UTC)

            # Определяем новую дату окончания
            if current_end_date_str and subscription_paid:
                try:
                    # Парсим naive дату из БД
                    current_end_naive = datetime.strptime(current_end_date_str, '%Y-%m-%d %H:%M:%S')
                    # Делаем aware (UTC)
                    current_end_aware = pytz.UTC.localize(current_end_naive)

                    # Если подписка активна - продлеваем от текущей даты окончания
                    if current_end_aware > now_utc:
                        new_end_aware = current_end_aware + timedelta(days=days, hours=hours)
                    else:
                        # Если истекла - от текущего момента
                        new_end_aware = now_utc + timedelta(days=days, hours=hours)
                except ValueError:
                    # Если формат неверный, продлеваем от текущего момента
                    new_end_aware = now_utc + timedelta(days=days, hours=hours)
            else:
                # Если подписки нет, начинаем с текущего момента
                new_end_aware = now_utc + timedelta(days=days, hours=hours)

            # Сохраняем в БД как naive строку (но время в UTC)
            new_end_str = new_end_aware.strftime('%Y-%m-%d %H:%M:%S')

            # Обновляем дату окончания
            cursor.execute('''
            UPDATE users 
            SET subscription_end_date = ?,
                subscription_paid = TRUE,
                last_activity = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            ''', (new_end_str, telegram_id))

            conn.commit()
            conn.close()

            cache_key = f"user_{telegram_id}"
            cache.delete(cache_key)

            logger.info(
                f"✅ Подписка пользователя {telegram_id} продлена до {new_end_str} (+{days} дней, +{hours} часов)")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при продлении подписки для {telegram_id}: {e}")
            logger.error(traceback.format_exc())
            return False

    def extend_all_active_subscriptions(self, hours: int = 0, days: int = 0) -> dict:
        """Продление подписки всем пользователям - ИСПРАВЛЕНО: ВСЕ В UTC"""
        try:
            logger.info(f"🔄 Начинаю массовое продление подписок: +{days} дней, +{hours} часов")

            # ВСЕГДА ИСПОЛЬЗУЕМ UTC ДЛЯ СРАВНЕНИЯ
            now_utc = datetime.now(pytz.UTC)
            logger.info(f"📅 Текущее время UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")

            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем всех пользователей с активными подписками
            cursor.execute('''
            SELECT telegram_id, subscription_end_date, username, first_name
            FROM users 
            WHERE subscription_paid = TRUE 
            AND subscription_end_date IS NOT NULL
            ''')

            users = cursor.fetchall()
            logger.info(f"📊 Найдено пользователей для продления: {len(users)}")

            results = {
                'total': len(users),
                'success': 0,
                'failed': 0,
                'errors': []
            }

            for user_data in users:
                try:
                    telegram_id = user_data[0]
                    current_end_date_str = user_data[1]

                    logger.info(f"🔄 Обработка пользователя {telegram_id}, текущая дата: {current_end_date_str}")

                    # Парсим дату из БД (она ВСЕГДА naive, но мы ЗНАЕМ что это UTC)
                    try:
                        current_end_naive = datetime.strptime(current_end_date_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            current_end_naive = datetime.strptime(current_end_date_str, '%Y-%m-%d')
                            current_end_naive = current_end_naive.replace(hour=23, minute=59, second=59)
                        except ValueError as e:
                            logger.error(f"   ❌ Ошибка парсинга даты: {e}")
                            results['failed'] += 1
                            results['errors'].append(f"{telegram_id}: неверный формат даты")
                            continue

                    # ДЕЛАЕМ naive -> aware (UTC) ДЛЯ СРАВНЕНИЯ
                    current_end_aware = pytz.UTC.localize(current_end_naive)

                    # ТЕПЕРЬ СРАВНИВАЕМ aware С aware
                    if current_end_aware > now_utc:
                        # Подписка активна - продлеваем от текущей даты окончания
                        new_end_aware = current_end_aware + timedelta(days=days, hours=hours)
                        logger.info(
                            f"   ✅ Подписка активна, новая дата (UTC): {new_end_aware.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        # Подписка истекла - начинаем с текущего момента
                        new_end_aware = now_utc + timedelta(days=days, hours=hours)
                        logger.info(
                            f"   ⚠️ Подписка истекла, новая дата (UTC): {new_end_aware.strftime('%Y-%m-%d %H:%M:%S')}")

                    # СОХРАНЯЕМ В БД КАК naive (без часового пояса), но в UTC
                    new_end_str = new_end_aware.strftime('%Y-%m-%d %H:%M:%S')

                    # Обновляем дату окончания
                    cursor.execute('''
                    UPDATE users 
                    SET subscription_end_date = ?,
                        last_activity = CURRENT_TIMESTAMP
                    WHERE telegram_id = ?
                    ''', (new_end_str, telegram_id))

                    if cursor.rowcount > 0:
                        logger.info(f"   ✅ Успешно обновлено для пользователя {telegram_id}")
                        results['success'] += 1

                        # Отправляем уведомление пользователю
                        try:
                            # Конвертируем UTC в локальное время для уведомления
                            local_tz = pytz_timezone('Asia/Novosibirsk')
                            new_end_local = new_end_aware.astimezone(local_tz)
                            end_str_local = new_end_local.strftime('%d.%m.%Y в %H:%M')

                            notification = f"🎉 <b>Ваша подписка продлена!</b>\n\n"
                            if days > 0 and hours > 0:
                                notification += f"⏱️ Срок: +{days} дн. {hours} ч.\n"
                            elif days > 0:
                                notification += f"⏱️ Срок: +{days} дн.\n"
                            elif hours > 0:
                                notification += f"⏱️ Срок: +{hours} ч.\n"
                            notification += f"📅 Действует до: {end_str_local}"

                            bot.send_message(telegram_id, notification, parse_mode='HTML')
                            logger.info(f"   ✅ Уведомление отправлено пользователю {telegram_id}")
                        except Exception as e:
                            logger.warning(f"   ⚠️ Не удалось отправить уведомление {telegram_id}: {e}")

                    else:
                        logger.error(f"   ❌ Нет обновленных строк для пользователя {telegram_id}")
                        results['failed'] += 1
                        results['errors'].append(f"{telegram_id}: нет обновленных строк")

                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"{telegram_id}: {str(e)}"
                    results['errors'].append(error_msg)
                    logger.error(f"❌ Ошибка при продлении подписки пользователя {telegram_id}: {e}")
                    logger.error(traceback.format_exc())

            conn.commit()
            logger.info(f"💾 Изменения сохранены в БД")
            conn.close()

            logger.info(f"✅ Массовое продление завершено: успешно {results['success']}, ошибок {results['failed']}")
            return results

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при массовом продлении подписок: {e}")
            logger.error(traceback.format_exc())
            return {'total': 0, 'success': 0, 'failed': 0, 'errors': [str(e)]}

    def create_payment(self, payment_id: str, telegram_id: int, amount: float, description: str) -> bool:
        """Создание записи о платеже"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
            INSERT INTO payments (payment_id, telegram_id, amount, description, status)
            VALUES (?, ?, ?, ?, 'pending')
            ''', (payment_id, telegram_id, amount, description))

            conn.commit()
            conn.close()
            return True
        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при создании платежа: {e}")
            return False

    def update_payment_status(self, payment_id: str, status: str) -> bool:
        """Обновление статуса платежа"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if status == 'succeeded':
                cursor.execute('''
                UPDATE payments 
                SET status = ?, paid_at = CURRENT_TIMESTAMP
                WHERE payment_id = ?
                ''', (status, payment_id))
            else:
                cursor.execute('''
                UPDATE payments 
                SET status = ?
                WHERE payment_id = ?
                ''', (status, payment_id))

            conn.commit()
            conn.close()
            return True
        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при обновлении статуса платежа: {e}")
            return False

    def mark_payment_processed(self, payment_id: str) -> bool:
        """Отметка платежа как обработанного"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
            UPDATE payments 
            SET is_processed = TRUE
            WHERE payment_id = ?
            ''', (payment_id,))

            conn.commit()
            conn.close()
            return True
        except sqlite3.Error as e:
            logger.info(f"❌ Ошибка при отметке платежа: {e}")
            return False



class ThreadSafeDict:
    """Потокобезопасный словарь"""

    def __init__(self):
        self._data = {}
        self._lock = Lock()

    def __getitem__(self, key):
        with self._lock:
            return self._data.get(key)

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value

    def __delitem__(self, key):
        with self._lock:
            if key in self._data:
                del self._data[key]

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    def pop(self, key, default=None):
        with self._lock:
            return self._data.pop(key, default)

    def clear(self):
        with self._lock:
            self._data.clear()

# ============================================================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ============================================================================
questions_by_topic = {}
topics_list = []
questions_loaded = False
scheduler = None
user_data_manager = UserDataManager(ttl_minutes=120, cleanup_interval_minutes=10)
# Создаем глобальный кеш-менеджер
cache = CacheManager(ttl_seconds=300)  # 5 минут
rate_limiter = RateLimiter(max_requests=60, per_seconds=60)  # 30 запросов в минуту
db = Database()

# ============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С ВОПРОСАМИ
# ============================================================================
def check_database_health():
    """Проверка здоровья базы данных"""
    logger.info("🏥 Проверка здоровья базы данных...")

    try:
        conn = db.get_connection()
        cursor = conn.cursor()

        # Проверяем все таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        logger.info(f"📊 Таблиц в базе данных: {len(tables)}")

        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"  - {table_name}: {count} записей")

        # Проверяем пользователей
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_paid = TRUE")
        active_subscriptions = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM payments")
        total_payments = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM payments WHERE status = 'succeeded' AND is_processed = FALSE")
        unprocessed_payments = cursor.fetchone()[0]

        logger.info(f"👥 Всего пользователей: {total_users}")
        logger.info(f"✅ Активных подписок: {active_subscriptions}")
        logger.info(f"💰 Всего платежей: {total_payments}")
        logger.info(f"⏳ Необработанных успешных платежей: {unprocessed_payments}")

        conn.close()

        return {
            'tables': len(tables),
            'total_users': total_users,
            'active_subscriptions': active_subscriptions,
            'total_payments': total_payments,
            'unprocessed_payments': unprocessed_payments
        }

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке здоровья базы данных: {e}")
        return None


def load_and_parse_questions(filename: str) -> bool:
    """Оптимизированная загрузка вопросов с выводом статистики по каждому вопросу"""
    global questions_by_topic, topics_list, questions_loaded

    try:
        if not os.path.exists(filename):
            logger.info(f"❌ Файл '{filename}' не найден!")
            return False

        # Используем более быстрый парсинг
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()

        questions_by_topic.clear()
        topics_list.clear()

        # Используем словарь для временного хранения
        temp_topics = {}
        current_topic = None
        current_question = None
        current_question_text = None
        current_question_number = None
        current_answers = []

        # Счетчики для статистики
        total_questions_parsed = 0
        questions_with_zero_correct = 0
        questions_with_multiple_correct = 0

        lines = content.split('\n')

        for line in lines:
            line = line.strip()
            if not line:  # Пропускаем пустые строки
                continue

            # Проверяем, является ли строка темой
            if line.startswith('МДК'):
                # Сохраняем предыдущий вопрос если есть
                if current_topic and current_question and current_question_text:
                    # Подсчитываем правильные ответы
                    correct_count = sum(1 for a in current_answers if a.get('correct', False))

                    # Выводим информацию о вопросе
                    print(f"📊 Вопрос #{current_question_number}: {correct_count} правильных ответов")
                    print(f"   Текст: {current_question_text[:50]}...")
                    print(f"   Всего ответов: {len(current_answers)}")
                    print("-" * 50)

                    # Статистика
                    total_questions_parsed += 1
                    if correct_count == 0:
                        questions_with_zero_correct += 1
                    elif correct_count > 1:
                        questions_with_multiple_correct += 1

                    temp_topics.setdefault(current_topic, []).append({
                        'number': current_question_number,
                        'question': current_question_text,
                        'full_question': current_question,
                        'answers': current_answers.copy(),
                        'correct_count': correct_count
                    })

                current_topic = line
                current_question = None
                current_question_text = None
                current_question_number = None
                current_answers = []
                print(f"\n📚 НАЧАЛО ТЕМЫ: {current_topic}")
                print("=" * 70)

            # Проверяем, является ли строка номером вопроса
            elif re.match(r'^\d+\.', line):
                # Сохраняем предыдущий вопрос если есть
                if current_topic and current_question and current_question_text:
                    # Подсчитываем правильные ответы
                    correct_count = sum(1 for a in current_answers if a.get('correct', False))

                    # Выводим информацию о вопросе
                    print(f"📊 Вопрос #{current_question_number}: {correct_count} правильных ответов")
                    print(f"   Текст: {current_question_text[:50]}...")
                    print(f"   Всего ответов: {len(current_answers)}")

                    # Если правильных ответов нет - это потенциальная проблема
                    if correct_count == 0:
                        print(f"   ⚠️ ВНИМАНИЕ: Нет правильных ответов!")
                    elif correct_count > 1:
                        print(f"   ℹ️ Несколько правильных ответов: {correct_count}")
                    print("-" * 50)

                    # Статистика
                    total_questions_parsed += 1
                    if correct_count == 0:
                        questions_with_zero_correct += 1
                    elif correct_count > 1:
                        questions_with_multiple_correct += 1

                    temp_topics.setdefault(current_topic, []).append({
                        'number': current_question_number,
                        'question': current_question_text,
                        'full_question': current_question,
                        'answers': current_answers.copy(),
                        'correct_count': correct_count
                    })

                # Извлекаем номер вопроса
                match = re.match(r'^(\d+)\.', line)
                if match:
                    current_question_number = int(match.group(1))

                # Сохраняем полную строку вопроса
                current_question = line
                current_question_text = None
                current_answers = []

            # Проверяем, является ли строка текстом вопроса (идет сразу после номера)
            elif current_question and current_question_text is None and not line.startswith(('+', '-')) and line:
                # Это текст вопроса
                current_question_text = line

            # Проверяем, является ли строка вариантом ответа
            elif current_question and (line.startswith('+') or line.startswith('-')):
                is_correct = line.startswith('+')
                answer_text = line[1:].strip()
                if answer_text:
                    current_answers.append({
                        'text': answer_text,
                        'correct': is_correct
                    })

        # Сохраняем последний вопрос
        if current_topic and current_question and current_question_text:
            # Подсчитываем правильные ответы
            correct_count = sum(1 for a in current_answers if a.get('correct', False))

            # Выводим информацию о последнем вопросе
            print(f"\n📊 Вопрос #{current_question_number}: {correct_count} правильных ответов")
            print(f"   Текст: {current_question_text[:50]}...")
            print(f"   Всего ответов: {len(current_answers)}")

            if correct_count == 0:
                print(f"   ⚠️ ВНИМАНИЕ: Нет правильных ответов!")
            elif correct_count > 1:
                print(f"   ℹ️ Несколько правильных ответов: {correct_count}")
            print("-" * 50)

            total_questions_parsed += 1
            if correct_count == 0:
                questions_with_zero_correct += 1
            elif correct_count > 1:
                questions_with_multiple_correct += 1

            temp_topics.setdefault(current_topic, []).append({
                'number': current_question_number,
                'question': current_question_text,
                'full_question': current_question,
                'answers': current_answers,
                'correct_count': correct_count
            })

        # Копируем в глобальные переменные
        questions_by_topic.update(temp_topics)
        topics_list = list(temp_topics.keys())

        if topics_list:
            topics_list.append("🎲 Все темы (рандом)")
            questions_loaded = True

        # Подсчитываем общее количество вопросов
        total_questions = sum(len(q) for q in questions_by_topic.values())

        # Выводим итоговую статистику
        print("\n" + "=" * 70)
        print("📊 ИТОГОВАЯ СТАТИСТИКА:")
        print("=" * 70)
        print(f"📚 Всего тем: {len(topics_list) - 1}")
        print(f"❓ Всего вопросов: {total_questions}")
        print(f"📝 Всего вопросов (по parsed): {total_questions_parsed}")
        print(
            f"✅ Вопросов с 1 правильным ответом: {total_questions_parsed - questions_with_zero_correct - questions_with_multiple_correct}")
        print(f"⚠️ Вопросов без правильных ответов: {questions_with_zero_correct}")
        print(f"ℹ️ Вопросов с несколькими правильными ответами: {questions_with_multiple_correct}")
        print("=" * 70)

        # Выводим пример для проверки
        if topics_list and questions_by_topic:
            first_topic = topics_list[0]
            if questions_by_topic[first_topic]:
                example = questions_by_topic[first_topic][0]
                logger.info(f"📝 Пример вопроса из '{first_topic}':")
                logger.info(f"   Номер: {example.get('number', 'N/A')}")
                logger.info(f"   Текст вопроса: {example['question'][:50]}...")
                logger.info(f"   Ответов: {len(example['answers'])}")
                logger.info(f"   Правильных ответов: {example.get('correct_count', 0)}")

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки: {e}")
        logger.error(traceback.format_exc())
        return False


def get_random_question_from_topic(user_id, topic_name: str) -> Optional[Dict]:
    """Получение случайного вопроса из темы с учетом уже отвеченных"""
    try:
        # Получаем все вопросы для темы
        if topic_name == "🎲 Все темы (рандом)":
            all_questions = []
            for topic in questions_by_topic.keys():
                for question in questions_by_topic[topic]:
                    question_copy = question.copy()
                    question_copy['source_topic'] = topic  # Сохраняем исходную тему
                    all_questions.append(question_copy)
        elif topic_name in questions_by_topic:
            all_questions = questions_by_topic[topic_name].copy()
            for question in all_questions:
                question['source_topic'] = topic_name  # Добавляем исходную тему
        else:
            return None

        if not all_questions:
            return None

        # Получаем данные пользователя
        user_data = user_data_manager.get_user_data(user_id)

        # Получаем отвеченные вопросы для этой темы
        answered_questions = []
        if 'answered_questions' in user_data and topic_name in user_data['answered_questions']:
            answered_questions = user_data['answered_questions'][topic_name]

        # Получаем вопросы текущей сессии
        session_questions = {}
        if 'session_questions' in user_data and topic_name in user_data['session_questions']:
            session_questions = user_data['session_questions'][topic_name]

        # Фильтруем вопросы
        available_questions = []
        incorrect_questions = []

        for question in all_questions:
            question_text = question['question']

            # Если вопрос уже правильно отвечен в этой теме, пропускаем
            if question_text in answered_questions:
                continue

            # Если вопрос в текущей сессии
            if question_text in session_questions:
                if session_questions[question_text] == True:
                    # Уже правильно отвечен в этой сессии
                    continue
                else:
                    # Неправильно отвечен - добавляем в список неправильных
                    incorrect_questions.append(question)
            else:
                # Новый вопрос
                available_questions.append(question)

        # Сначала используем новые вопросы, потом неправильно отвеченные
        if available_questions:
            return random.choice(available_questions)
        elif incorrect_questions:
            return random.choice(incorrect_questions)
        else:
            # Все вопросы отвечены правильно
            return None

    except Exception as e:
        logger.error(f"❌ Ошибка при получении вопроса: {e}")
        traceback.print_exc()
        return None


def check_and_load_questions() -> bool:
    """Проверка и загрузка вопросов"""
    global questions_loaded

    if os.path.exists('тест.txt'):
        logger.info("📂 Файл 'тест.txt' найден. Загружаю вопросы...")
        questions_loaded = load_and_parse_questions('тест.txt')
        if questions_loaded:
            logger.info("✅ Вопросы успешно загружены!")
        else:
            logger.info("❌ Не удалось загрузить вопросы")
        return questions_loaded
    else:
        logger.info("❌ Файл 'тест.txt' не найден!")
        return False


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================





def sync_paid_subscriptions_on_startup():
    """Синхронизация оплаченных подписок - ВСЕ В UTC"""
    logger.info("🔄 Запуск синхронизации оплаченных подписок...")

    try:
        conn = db.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        MAX_DAYS_FOR_PAYMENT_CHECK = 3
        ACTIVATION_WINDOW_HOURS = 24

        # Текущее время в UTC
        now_utc = datetime.now(pytz.UTC)

        cursor.execute(f'''
            SELECT 
                p.payment_id,
                p.telegram_id,
                p.amount,
                p.created_at,
                p.paid_at,
                u.subscription_paid,
                u.subscription_end_date,
                u.subscription_purchased,
                u.subscription_start_date,
                u.username
            FROM payments p
            LEFT JOIN users u ON p.telegram_id = u.telegram_id
            WHERE p.status = 'succeeded' 
            AND p.is_processed = FALSE
            AND (
                p.paid_at >= datetime('now', '-{MAX_DAYS_FOR_PAYMENT_CHECK} days')
                OR 
                (p.paid_at IS NULL AND p.created_at >= datetime('now', '-{MAX_DAYS_FOR_PAYMENT_CHECK} days'))
            )
            ORDER BY p.paid_at ASC, p.created_at ASC
        ''')

        payments = cursor.fetchall()

        if not payments:
            logger.info(f"✅ Нет свежих необработанных платежей")
            conn.close()
            # ВОЗВРАЩАЕМ max_days
            return {
                'total': 0,
                'activated': 0,
                'skipped': 0,
                'errors': 0,
                'max_days': MAX_DAYS_FOR_PAYMENT_CHECK  # Добавлено
            }

        logger.info(f"📋 Найдено {len(payments)} платежей")

        activated_count = 0
        skipped_count = 0
        errors_count = 0

        for payment in payments:
            try:
                payment_id = payment['payment_id']
                telegram_id = payment['telegram_id']
                username = payment['username'] or f"user_{telegram_id}"
                paid_at = payment['paid_at']

                logger.info(f"\n🔍 Обработка платежа {payment_id} для {username}")

                # Определяем время платежа
                payment_datetime = None
                if paid_at:
                    try:
                        payment_naive = datetime.strptime(paid_at, '%Y-%m-%d %H:%M:%S')
                        payment_datetime = pytz.UTC.localize(payment_naive)
                    except:
                        pass

                if not payment_datetime:
                    continue

                # Проверяем текущую подписку
                subscription_end_date = payment['subscription_end_date']
                subscription_end_datetime = None
                if subscription_end_date:
                    try:
                        end_naive = datetime.strptime(subscription_end_date, '%Y-%m-%d %H:%M:%S')
                        subscription_end_datetime = pytz.UTC.localize(end_naive)
                    except:
                        pass
                user_has_active_subscription = False
                if subscription_end_datetime:
                    user_has_active_subscription = subscription_end_datetime > now_utc
                if (payment['subscription_paid'] == 1 and subscription_end_date):
                    try:
                        subscription_end_datetime = datetime.strptime(subscription_end_date, '%Y-%m-%d %H:%M:%S')
                        user_has_active_subscription = subscription_end_datetime > datetime.now(pytz.UTC)
                    except:
                        pass

                user_has_purchased_subscription = payment['subscription_purchased'] == 1

                # Логика активации
                should_activate = False

                if not user_has_active_subscription:
                    should_activate = True
                elif user_has_active_subscription and not user_has_purchased_subscription:
                    should_activate = True
                elif user_has_active_subscription and user_has_purchased_subscription:
                    if subscription_end_datetime:
                        hours_until_expiry = (subscription_end_datetime - datetime.now(pytz.UTC)).total_seconds() / 3600
                        if hours_until_expiry <= ACTIVATION_WINDOW_HOURS:
                            should_activate = True

                if should_activate:
                    # Определяем дату окончания
                    if user_has_active_subscription and subscription_end_datetime:
                        if subscription_end_datetime > datetime.now(pytz.UTC):
                            end_datetime = subscription_end_datetime + timedelta(days=30)
                        else:
                            end_datetime = datetime.now(pytz.UTC) + timedelta(days=30)
                    else:
                        end_datetime = payment_datetime + timedelta(days=30)

                    # Корректируем если платеж был давно
                    hours_since_payment = (datetime.now(pytz.UTC) - payment_datetime).total_seconds() / 3600
                    if hours_since_payment > 24:
                        end_datetime = datetime.now(pytz.UTC) + timedelta(days=30)

                    # ОБНОВЛЯЕМ ПОДПИСКУ используя СУЩЕСТВУЮЩЕЕ соединение!
                    try:
                        # Обновляем users
                        end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
                        start_str = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')

                        cursor.execute('''
                            UPDATE users 
                            SET subscription_paid = ?,
                                subscription_start_date = ?,
                                subscription_end_date = ?,
                                is_trial_used = ?,
                                subscription_purchased = ?,
                                last_activity = CURRENT_TIMESTAMP
                            WHERE telegram_id = ?
                            ''', (True,  # 1 - subscription_paid
                                  start_str,  # 2 - subscription_start_date
                                  end_str,  # 3 - subscription_end_date
                                  False,  # 4 - is_trial_used
                                  True,  # 5 - subscription_purchased ← ВАЖНО!
                                  telegram_id))  # 6 - telegram_id

                        # Помечаем платеж как обработанный
                        cursor.execute('UPDATE payments SET is_processed = TRUE WHERE payment_id = ?', (payment_id,))

                        activated_count += 1
                        logger.info(f"   ✅ Подписка активирована до {end_str}")

                        # Отправляем уведомление
                        try:
                            bot.send_message(
                                telegram_id,
                                f"🎉 <b>Ваша подписка активирована!</b>\n\n"
                                f"Подписка действует до: {end_datetime.strftime('%d.%m.%Y %H:%M')}",
                                parse_mode='HTML'
                            )
                        except Exception as e:
                            logger.warning(f"   ⚠️ Не удалось отправить уведомление: {e}")

                    except Exception as e:
                        errors_count += 1
                        logger.error(f"   ❌ Ошибка обновления: {e}")

                else:
                    # Не активируем, но помечаем как обработанный
                    cursor.execute('UPDATE payments SET is_processed = TRUE WHERE payment_id = ?', (payment_id,))
                    skipped_count += 1
                    logger.info(f"   ⏩ Пропущен платеж")

            except Exception as e:
                errors_count += 1
                logger.error(f"❌ Ошибка при обработке платежа: {e}")
                logger.error(traceback.format_exc())

        conn.commit()
        conn.close()

        logger.info(f"📊 Итоги: активировано {activated_count}, пропущено {skipped_count}, ошибок {errors_count}")

        # ВОЗВРАЩАЕМ max_days
        return {
            'total': len(payments),
            'activated': activated_count,
            'skipped': skipped_count,
            'errors': errors_count,
            'max_days': MAX_DAYS_FOR_PAYMENT_CHECK  # Добавлено
        }

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при синхронизации: {e}")
        # ВОЗВРАЩАЕМ max_days даже при ошибке
        return {
            'total': 0,
            'activated': 0,
            'skipped': 0,
            'errors': 1,
            'max_days': 3,
            'error': str(e)
        }


def cleanup_old_payments():
    """Очистка старых необработанных платежей (старше 7 дней)"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()

        # Находим старые необработанные платежи (старше 7 дней)
        cursor.execute('''
        SELECT COUNT(*) as count
        FROM payments 
        WHERE is_processed = FALSE
        AND (
            (paid_at IS NOT NULL AND paid_at < datetime('now', '-7 days'))
            OR 
            (paid_at IS NULL AND created_at < datetime('now', '-7 days'))
        )
        ''')

        old_payments_count = cursor.fetchone()[0]

        if old_payments_count > 0:
            # Помечаем старые платежи как обработанные
            cursor.execute('''
            UPDATE payments 
            SET is_processed = TRUE,
                status = CASE 
                    WHEN status = 'pending' THEN 'expired' 
                    ELSE status 
                END
            WHERE is_processed = FALSE
            AND (
                (paid_at IS NOT NULL AND paid_at < datetime('now', '-7 days'))
                OR 
                (paid_at IS NULL AND created_at < datetime('now', '-7 days'))
            )
            ''')

            conn.commit()
            logger.info(f"🧹 Очищено {old_payments_count} старых необработанных платежей (старше 7 дней)")

        conn.close()
        return old_payments_count

    except Exception as e:
        logger.error(f"❌ Ошибка при очистке старых платежей: {e}")
        return 0


def check_subscription_consistency():
    """Проверка согласованности данных о подписках"""
    logger.info("🔍 Запуск проверки согласованности данных о подписках...")

    try:
        conn = db.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        problems = []

        # 1. Проверяем пользователей с subscription_purchased = TRUE, но без активной подписки
        cursor.execute('''
        SELECT telegram_id, username, subscription_purchased, 
               subscription_paid, subscription_end_date
        FROM users 
        WHERE subscription_purchased = TRUE 
        AND (
            subscription_paid = FALSE 
            OR subscription_end_date IS NULL 
            OR subscription_end_date < CURRENT_TIMESTAMP
        )
        ''')

        purchased_but_not_active = cursor.fetchall()
        for user in purchased_but_not_active:
            problem = f"❌ Пользователь {user['telegram_id']} (@{user['username'] or 'нет'}) КУПИЛ подписку, но она НЕ АКТИВНА!"
            problems.append(problem)
            logger.warning(problem)

        # 2. Проверяем успешные платежи без subscription_purchased
        cursor.execute('''
        SELECT p.telegram_id, u.username, p.payment_id, p.paid_at,
               u.subscription_purchased
        FROM payments p
        JOIN users u ON p.telegram_id = u.telegram_id
        WHERE p.status = 'succeeded'
        AND u.subscription_purchased = FALSE
        ''')

        successful_payments_without_purchase = cursor.fetchall()
        for payment in successful_payments_without_purchase:
            problem = f"❌ Пользователь {payment['telegram_id']} (@{payment['username'] or 'нет'}) оплатил, но subscription_purchased=FALSE!"
            problems.append(problem)
            logger.warning(problem)

        # ✅ КОРРЕКТНАЯ ПРОВЕРКА: ТОЛЬКО если есть оплата, но нет подписки И нет пробного доступа
        cursor.execute('''
        SELECT u.telegram_id, u.username, u.is_trial_used, u.subscription_purchased
        FROM users u
        WHERE u.subscription_purchased = FALSE
        AND u.subscription_paid = FALSE
        AND u.is_trial_used = FALSE
        AND EXISTS (
            SELECT 1 FROM payments p 
            WHERE p.telegram_id = u.telegram_id 
            AND p.status = 'succeeded'
        )
        ''')

        weird_cases = cursor.fetchall()
        for user in weird_cases:
            problem = f"❌ Пользователь {user['telegram_id']} (@{user['username'] or 'нет'}) оплатил, но нет ни подписки, ни пробного доступа!"
            problems.append(problem)
            logger.warning(problem)

        # ℹ️ ИНФОРМАЦИОННОЕ СООБЩЕНИЕ (НЕ ОШИБКА)
        cursor.execute('''
        SELECT telegram_id, username, is_trial_used, subscription_purchased
        FROM users 
        WHERE is_trial_used = TRUE 
        AND subscription_purchased = TRUE
        ''')

        trial_then_purchased = cursor.fetchall()
        if trial_then_purchased:
            logger.info(f"ℹ️ Пользователи, которые взяли пробный и потом оплатили: {len(trial_then_purchased)}")
            for user in trial_then_purchased[:5]:
                logger.info(f"  • {user['telegram_id']} (@{user['username'] or 'нет'}) - пробный + оплата")

        conn.close()

        if problems:
            logger.warning(f"⚠️ Найдено {len(problems)} КРИТИЧЕСКИХ проблем с согласованностью данных")
            for i, problem in enumerate(problems[:10], 1):
                logger.warning(f"  {i}. {problem}")
            if len(problems) > 10:
                logger.warning(f"  ... и еще {len(problems) - 10} проблем")
        else:
            logger.info("✅ Данные о подписках полностью согласованы")

        return problems

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке согласованности данных: {e}")
        logger.error(traceback.format_exc())
        return None

def create_yookassa_payment(telegram_id: int) -> Optional[Dict]:
    """Создание платежа в ЮKassa - упрощенная версия"""
    try:
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
            logger.info("❌ ЮKassa не настроена")
            return None

        # Генерируем уникальный ID для платежа
        payment_id = str(uuid.uuid4())

        # Описание платежа
        description = "Подписка на бота для подготовки к тестам (30 дней)"

        # Создаем платеж в ЮKassa
        payment = Payment.create({
            "amount": {
                "value": f"{SUBSCRIPTION_PRICE:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": f"https://t.me/{bot.get_me().username}"
            },
            "capture": True,
            "description": description,
            "metadata": {
                "telegram_id": telegram_id,
                "subscription_days": SUBSCRIPTION_DAYS
            }
        }, payment_id)

        # Сохраняем платеж в базу данных
        if db.create_payment(payment.id, telegram_id, SUBSCRIPTION_PRICE, description):
            logger.info(f"✅ Создан платеж {payment.id} для пользователя {telegram_id}")
            return {
                'id': payment.id,
                'status': payment.status,
                'confirmation_url': payment.confirmation.confirmation_url,
                'amount': SUBSCRIPTION_PRICE,
                'description': description
            }
        else:
            logger.info(f"❌ Не удалось сохранить платеж в БД")
            return None

    except Exception as e:
        logger.info(f"❌ Ошибка при создании платежа: {e}")
        return None


def ensure_subscription_status(user_id):
    """Гарантированная проверка статуса подписки при каждом действии"""
    logger.info(f"🔍 ПРОВЕРКА ПОДПИСКИ для пользователя {user_id}")
    try:
        cache_key = f"user_{user_id}"
        cache.delete(cache_key)

        user = db.get_user(user_id)
        logger.info(f"   Данные пользователя: {user}")

        if not user:
            logger.info(f"   ❌ Пользователь не найден")
            return False

        if user.get('is_admin'):
            logger.info(f"   ✅ Администратор, доступ разрешен")
            return True

        if not user.get('subscription_paid'):
            logger.info(f"   ❌ subscription_paid = False")
            return False

        end_date_str = user.get('subscription_end_date')

        if not end_date_str:
            logger.info(f"   ❌ Нет даты окончания")
            return False

        try:
            end_naive = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                end_naive = datetime.strptime(end_date_str, '%Y-%m-%d')
                end_naive = end_naive.replace(hour=23, minute=59, second=59)
            except ValueError:
                logger.info(f"   ❌ Неверный формат даты")
                return False

        end_aware = pytz.UTC.localize(end_naive)
        now_aware = datetime.now(pytz.UTC)
        logger.info(f"   Подписка активна: {end_aware > now_aware}")

        if end_aware <= now_aware:
            logger.info(f"   ⚠️ Подписка истекла, деактивирую...")
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users 
                SET subscription_paid = FALSE,
                    subscription_start_date = NULL,
                    subscription_end_date = NULL
                WHERE telegram_id = ?
            ''', (user_id,))
            conn.commit()
            conn.close()
            logger.info(f"   ✅ Подписка деактивирована")
            return False

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка в ensure_subscription_status: {e}")
        logger.error(traceback.format_exc())
        return False

def check_user_access(chat_id: int, send_message: bool = True) -> bool:
    """Проверка доступа пользователя с автоматической деактивацией истекших подписок"""
    logger.info(f"🔐 check_user_access для {chat_id}, send_message={send_message}")

    cache.delete(f"user_{chat_id}")

    user = db.get_user(chat_id)
    logger.info(f"   Пользователь: {user.get('username') if user else 'None'}")
    if user and user.get('is_admin'):
        logger.info(f"   ✅ Админ, доступ разрешен")
        return True

    has_active = ensure_subscription_status(chat_id)
    logger.info(f"   📊 Результат проверки подписки: {has_active}")

    if not questions_loaded:
        if send_message:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Проверить вопросы", callback_data="check_questions"))
            bot.send_message(
                chat_id,
                "⏳ Вопросы еще не загружены. Пожалуйста, подождите...",
                reply_markup=markup
            )
        return False

    if not has_active:
        if send_message:
            user_info = db.get_user(chat_id)
            if user_info:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))
                markup.add(types.InlineKeyboardButton("🎁 Получить пробный доступ", callback_data="trial"))
                markup.row(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))
                markup.row(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

                bot.send_message(
                    chat_id,
                    "🚫 <b>Доступ ограничен!</b>\n\nДля использования бота необходима активная подписка.",
                    parse_mode='HTML',
                    reply_markup=markup
                )
            else:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("🎁 Получить пробный доступ", callback_data="trial"))
                markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))

                bot.send_message(
                    chat_id,
                    "👋 <b>Добро пожаловать!</b>\n\nДля начала работы необходимо оформить подписку или получить пробный доступ.",
                    parse_mode='HTML',
                    reply_markup=markup
                )
        return False
    logger.info(f"   ✅ Доступ разрешен, обновляю активность")
    db.update_activity(chat_id)
    return True


def create_main_menu() -> types.InlineKeyboardMarkup:
    """Создание главного меню"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"),
        types.InlineKeyboardButton("🎲 Случайный вопрос", callback_data="random_question")
    )
    markup.add(
        types.InlineKeyboardButton("📊 Моя статистика", callback_data="show_stats"),
        types.InlineKeyboardButton("💳 Подписка", callback_data="subscribe_info")
    )
    markup.add(
        types.InlineKeyboardButton("ℹ️ Информация", callback_data="info"),
        types.InlineKeyboardButton("🆘 Помощь", callback_data="help_menu")
    )
    return markup


def create_back_button(target: str = "main_menu") -> types.InlineKeyboardMarkup:
    """Создание кнопки Назад"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data=target))
    return markup


# ============================================================================
# НАСТРОЙКА КОМАНД БОТА
# ============================================================================
def setup_bot_commands():
    """Настройка меню команд бота"""
    try:
        # Основные команды для всех пользователей
        commands = [
            types.BotCommand("start", "Главное меню"),
            types.BotCommand("help", "Справка по командам"),
            types.BotCommand("stats", "Ваша статистика"),
            types.BotCommand("myinfo", "Информация о вас"),
            types.BotCommand("checkmypayment", "Проверить мой платеж"),
        ]

        bot.set_my_commands(commands)
        logger.info("✅ Основные команды бота настроены")

        # Команды для администраторов
        admin_commands = [
            types.BotCommand("start", "Главное меню"),
            types.BotCommand("help", "Помощь"),
            types.BotCommand("stats", "Статистика"),
            types.BotCommand("myinfo", "Моя информация"),
            types.BotCommand("admin", "Панель администратора"),
            types.BotCommand("reload", "Перезагрузить вопросы"),
            types.BotCommand("check_subs", "Проверить подписки"),
            types.BotCommand("all_stats", "Вся статистика"),
            types.BotCommand("scheduler_status", "Статус планировщика"),
            types.BotCommand("reset_stats", "Сбросить статистику"),
            types.BotCommand("grant_sub", "Выдать подписку"),
            types.BotCommand("extend_sub", "Продлить подписку"),  # НОВАЯ КОМАНДА
            types.BotCommand("set_admin", "Назначить админа"),
            types.BotCommand("check_sub_sync", "Синхронизация подписок"),
            types.BotCommand("send_all_users", "Массовая рассылка"),
        ]

        # Настраиваем команды для администраторов
        admin_ids = db.get_admin_ids()
        for admin_id in admin_ids:
            try:
                bot.set_my_commands(
                    admin_commands,
                    scope=types.BotCommandScopeChat(admin_id)
                )
                logger.info(f"✅ Админские команды настроены для {admin_id}")
            except Exception as e:
                logger.info(f"⚠️ Ошибка настройки админских команд для {admin_id}: {e}")

        return True

    except Exception as e:
        logger.info(f"❌ Ошибка настройки команд бота: {e}")
        return False


# ============================================================================
# ОСНОВНЫЕ ОБРАБОТЧИКИ СООБЩЕНИЙ (ВКЛЮЧАЯ АДМИНИСТРАТИВНЫЕ)
# ============================================================================


@bot.message_handler(commands=['help'])
def handle_help(message):
    """Обработчик команды /help"""
    chat_id = message.chat.id

    # Проверяем, является ли пользователь администратором
    user = db.get_user(chat_id)
    is_admin = user and user.get('is_admin')

    help_text = """
🆘 <b>Доступные команды:</b>

<code>/start</code> - Главное меню
<code>/help</code> - Эта справка
<code>/stats</code> - Ваша статистика
<code>/myinfo</code> - Информация о вас
<code>/checkmypayment</code> - Проверить мой платеж
"""

    if is_admin:
        help_text += """

👑 <b>Команды администратора:</b>
<code>/admin</code> - Панель администратора
<code>/reload</code> - Перезагрузить вопросы
<code>/check_subs</code> - Проверить подписки
<code>/all_stats</code> - Вся статистика
<code>/scheduler_status</code> - Статус планировщика
<code>/reset_stats</code> - Сбросить статистику
<code>/grant_sub</code> - Выдать подписку
<code>/set_admin</code> - Назначить админа
<code>/send_all_users</code> - Массовая рассылка всем пользователям
"""

    help_text += """

📞 <b>Поддержка:</b> @ZlotaR

💡 <b>Совет:</b> Нажмите на кнопку меню (📎) рядом с полем ввода, чтобы увидеть все команды!
    """

    bot.send_message(chat_id, help_text, parse_mode='HTML')


def send_question_inline(chat_id, message_id=None):
    """Отправка вопроса с вариантами ответов с учетом логики сессии"""
    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        if message_id:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="❌ Требуется активная подписка!",
                reply_markup=create_back_button("main_menu")
            )
        else:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))
            markup.add(types.InlineKeyboardButton("🎁 Получить пробный доступ", callback_data="trial"))
            bot.send_message(
                chat_id,
                "🚫 <b>Доступ ограничен!</b>\n\nДля использования бота необходима активная подписка.",
                parse_mode='HTML',
                reply_markup=markup
            )
        return

    # Получаем данные пользователя
    user_data = user_data_manager.get_user_data(chat_id)

    if not user_data.get('current_topic'):
        if message_id:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⚠️ Сначала выберите тему!",
                reply_markup=create_back_button("change_topic")
            )
        else:
            bot.send_message(
                chat_id,
                "⚠️ Сначала выберите тему!",
                reply_markup=create_back_button("main_menu")
            )
        return

    topic = user_data['current_topic']

    # Получаем случайный вопрос из темы с учетом логики сессии
    question_data = get_random_question_from_topic(chat_id, topic)

    if not question_data:
        # Все вопросы в теме отвечены правильно
        # Очищаем сессию для этой темы
        user_data_manager.clear_topic_session(chat_id, topic)

        # Получаем статистику сессии
        session_stats_data = user_data_manager.get_session_stats(chat_id)
        session_total = session_stats_data.get('session_total', 0)
        session_correct = session_stats_data.get('session_correct', 0)
        session_percentage = (session_correct / session_total * 100) if session_total > 0 else 0

        # Находим номер темы для callback_data
        topic_num = topics_list.index(topic) if topic in topics_list else 0

        # Получаем общее количество вопросов
        if topic == "🎲 Все темы (рандом)":
            total_questions = sum(len(q) for q in questions_by_topic.values())
        else:
            total_questions = len(questions_by_topic.get(topic, []))

        # Формируем сообщение о завершении темы
        completion_text = f"""
🎉 <b>Поздравляем!</b>

📚 <b>Тема завершена:</b> {topic}

✅ Вы ответили правильно на все вопросы в этой теме!
📊 <b>Всего вопросов:</b> {total_questions}

📈 <b>Статистика сессии:</b>
• Правильных ответов: {session_correct}/{session_total}
• Процент правильных: {session_percentage:.1f}%

Выберите другую тему или начните новую сессию.
"""

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"))
        markup.add(types.InlineKeyboardButton("🔄 Начать новую сессию", callback_data=f"r_{topic_num}"))
        markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        if message_id:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=completion_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        else:
            bot.send_message(
                chat_id,
                completion_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        return
    # Извлекаем правильные ответы
    correct_answers = []
    for answer in question_data['answers']:
        if answer['correct']:
            correct_answers.append(answer['text'])

    # Перемешиваем ответы
    answers = question_data['answers'].copy()
    random.shuffle(answers)

    # Сохраняем данные вопроса через менеджер
    numbered_answers = {}
    answers_list = []
    for i, answer in enumerate(answers, 1):
        answer_text = answer['text']
        numbered_answers[i] = answer_text
        answers_list.append(answer_text)

    # Обновляем данные пользователя
    user_data_manager.update_user_data(
        chat_id,
        current_question=question_data['question'],
        current_question_full=question_data.get('full_question', ''),  # Полная строка с номером
        current_question_number=question_data.get('number', ''),  # Номер вопроса
        correct_answer=correct_answers,
        numbered_answers=numbered_answers,
        answers_list=answers_list,
        current_question_topic=topic
    )

    # Формируем текст вопроса
    topic_display = topic
    question_text = f"📚 <b>Тема:</b> {topic_display}\n\n"

    # Добавляем информацию о прогрессе
    user_data = user_data_manager.get_user_data(chat_id)
    answered_questions = user_data.get('answered_questions', {}).get(topic, [])

    if topic == "🎲 Все темы (рандом)":
        total_questions = sum(len(q) for q in questions_by_topic.values())
    else:
        total_questions = len(questions_by_topic.get(topic, []))

    answered_count = len(answered_questions)
    progress_percentage = (answered_count / total_questions * 100) if total_questions > 0 else 0

    question_text += f"📊 <b>Прогресс:</b> {answered_count}/{total_questions} ({progress_percentage:.1f}%)\n\n"

    # Добавляем статистику сессии если есть
    session_stats_data = user_data_manager.get_session_stats(chat_id)
    if session_stats_data['session_total'] > 0:
        session_total = session_stats_data['session_total']
        session_correct = session_stats_data['session_correct']
        session_percentage = (session_correct / session_total * 100) if session_total > 0 else 0
        question_text += f"📊 <b>Сессия:</b> {session_correct}/{session_total} ({session_percentage:.1f}%)\n\n"

    # Формируем текст вопроса с номером
    q_text = question_data['question']
    q_number = question_data.get('number', '')

    if q_number:
        question_text += f"❓ <b>Вопрос #{q_number}:</b>\n{q_text}\n\n"
    else:
        question_text += f"❓ <b>Вопрос:</b>\n{q_text}\n\n"

    # Добавляем варианты ответов
    question_text += "📋 <b>Варианты ответов:</b>\n"
    for i in range(1, len(answers) + 1):
        question_text += f"{i}. {numbered_answers[i]}\n"

    question_text += "\n👇 Выберите номер правильного ответа:"

    # Создаем inline клавиатуру
    markup = types.InlineKeyboardMarkup(row_width=4)

    # Кнопки с номерами ответов
    buttons = []
    for i in range(1, len(answers) + 1):
        buttons.append(types.InlineKeyboardButton(
            text=str(i),
            callback_data=f"answer_{i}"
        ))

    # Добавляем кнопки по 4 в ряд
    for i in range(0, len(buttons), 4):
        markup.row(*buttons[i:i + 4])

    # Дополнительные кнопки
    markup.row(
        types.InlineKeyboardButton("📊 Статистика", callback_data="show_stats"),
        types.InlineKeyboardButton("🔄 Другой вопрос", callback_data="get_question")
    )
    markup.row(
        types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic"),
        types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )

    try:
        if message_id:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=question_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        else:
            bot.send_message(
                chat_id,
                question_text,
                parse_mode='HTML',
                reply_markup=markup
            )
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке вопроса: {e}")
        # Если не удалось редактировать сообщение, отправляем новое
        bot.send_message(
            chat_id,
            question_text,
            parse_mode='HTML',
            reply_markup=markup
        )


@bot.message_handler(commands=['stats'])
def handle_stats(message):
    """Обработчик команды /stats"""
    chat_id = message.chat.id

    # Проверяем доступ
    if not check_user_access(chat_id):
        return

    # Используем show_stats_message которая теперь определена выше
    show_stats_message(chat_id)


@bot.message_handler(commands=['myinfo'])
def handle_myinfo(message):
    """Обработчик команды /myinfo"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user:
        bot.send_message(chat_id, "❌ Вы не зарегистрированы. Используйте /start")
        return

    subscription_status = "✅ Активна" if db.check_subscription(chat_id) else "❌ Не активна"
    is_admin = "✅ Да" if user.get('is_admin') else "❌ Нет"

    info_text = f"""
📋 <b>Ваша информация</b>

🆔 ID: {user['telegram_id']}
👤 Имя: {user.get('first_name', 'не указано')} {user.get('last_name', '')}
📱 Username: @{user.get('username', 'не указан')}

💳 Подписка: {subscription_status}
👑 Администратор: {is_admin}

📅 Дата регистрации: {user.get('registration_date', 'неизвестно')[:10]}
🕒 Последняя активность: {user.get('last_activity', 'неизвестно')[:16]}
"""

    if user.get('subscription_end_date'):
        info_text += f"\n📅 Подписка действует до: {user['subscription_end_date']}"

    bot.send_message(chat_id, info_text, parse_mode='HTML')


# ============================================================================
# АДМИНИСТРАТИВНЫЕ КОМАНДЫ
# ============================================================================
@bot.message_handler(commands=['admin'])
def handle_admin(message):
    """Панель администратора"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав администратора.", reply_markup=create_main_menu())
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
        types.InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")
    )
    markup.add(
        types.InlineKeyboardButton("🔑 Выдать подписку", callback_data="admin_grant_sub"),
        types.InlineKeyboardButton("⏱️ Продлить подписку", callback_data="admin_extend_sub")  # НОВАЯ КНОПКА
    )
    markup.add(
        types.InlineKeyboardButton("👑 Назначить админа", callback_data="admin_grant_admin"),
        types.InlineKeyboardButton("📢 Массовая рассылка", callback_data="admin_broadcast")
    )
    markup.add(
        types.InlineKeyboardButton("📝 Логи", callback_data="admin_logs"),
        types.InlineKeyboardButton("🔄 Рестарт", callback_data="admin_restart")
    )
    markup.add(
        types.InlineKeyboardButton("🗄️ Скачать БД", callback_data="admin_db"),
        types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )

    bot.send_message(
        chat_id,
        "👑 <b>Панель администратора</b>\n\nВыберите действие:",
        parse_mode='HTML',
        reply_markup=markup
    )


@bot.message_handler(commands=['reload'])
def handle_reload(message):
    """Перезагрузка вопросов"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    bot.send_message(chat_id, "🔄 Перезагружаю вопросы из файла...")

    global questions_loaded
    questions_loaded = check_and_load_questions()

    if questions_loaded:
        bot.send_message(
            chat_id,
            f"✅ Вопросы успешно перезагружены!\nЗагружено тем: {len(topics_list) - 1}"
        )
    else:
        bot.send_message(
            chat_id,
            "❌ Не удалось загрузить вопросы. Проверьте файл 'тест.txt'"
        )


@bot.message_handler(commands=['check_subs'])
def handle_check_subs(message):
    """Ручная проверка подписок"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    bot.send_message(chat_id, "🔄 Начинаю ручную проверку подписок...")
    check_and_update_subscriptions()
    bot.send_message(chat_id, "✅ Ручная проверка подписок завершена!")


@bot.message_handler(commands=['all_stats'])
def handle_all_stats(message):
    """Вся статистика"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    all_stats = db.get_all_statistics()
    all_users = db.get_all_users()

    active_users = [u for u in all_users if db.check_subscription(u['telegram_id'])]

    stats_text = f"""
📊 <b>Вся статистика системы</b>

👥 <b>Всего пользователей:</b> {len(all_users)}
✅ <b>Активных подписок:</b> {len(active_users)}
📝 <b>Записей статистики:</b> {len(all_stats)}

📈 <b>Топ-5 пользователей:</b>
"""

    top_users = db.get_top_users(5)
    for i, user in enumerate(top_users, 1):
        username = user.get('username', 'нет username')
        first_name = user.get('first_name', '')
        correct = user['correct_answers']
        total = user['total_answers']
        rate = user['success_rate'] if 'success_rate' in user else 0

        stats_text += f"\n{i}. {first_name} (@{username}) - {correct}/{total} ({rate}%)"

    bot.send_message(chat_id, stats_text, parse_mode='HTML')


@bot.message_handler(commands=['scheduler_status'])
def handle_scheduler_status(message):
    """Статус планировщика"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    if scheduler is None:
        bot.send_message(chat_id, "❌ Планировщик не запущен")
        return

    status_text = "⏰ <b>Статус планировщика APScheduler</b>\n\n"

    try:
        jobs = scheduler.get_jobs()
        if not jobs:
            status_text += "⚠️ Нет активных задач\n"
        else:
            status_text += f"📋 Активных задач: {len(jobs)}\n\n"

            for i, job in enumerate(jobs, 1):
                if job.next_run_time:
                    try:
                        next_run = job.next_run_time.astimezone(NOVOSIBIRSK_TZ).strftime('%d.%m.%Y %H:%M')
                    except Exception as e:
                        next_run = f"Ошибка формата: {e}"
                else:
                    next_run = "Не запланировано"

                status_text += f"{i}. <b>{job.name}</b>\n"
                status_text += f"   ID: {job.id}\n"
                status_text += f"   Следующий запуск: {next_run}\n"

                if hasattr(job.trigger, 'start_date'):
                    try:
                        start_date = job.trigger.start_date.astimezone(NOVOSIBIRSK_TZ).strftime('%d.%m.%Y %H:%M')
                        status_text += f"   Начало: {start_date}\n"
                    except:
                        pass

                status_text += "\n"

        # Добавляем информацию о состоянии планировщика
        status_text += f"\n📊 Состояние планировщика: {'✅ Запущен' if scheduler.running else '❌ Остановлен'}"

        bot.send_message(chat_id, status_text, parse_mode='HTML')

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при получении статуса планировщика: {e}")

@bot.message_handler(commands=['reset_stats'])
def handle_reset_stats(message):
    """Сброс статистики пользователя"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    # Извлекаем ID пользователя из текста сообщения
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.send_message(chat_id, "❌ Использование: /reset_stats <user_id>")
            return

        target_id = int(parts[1])

        if db.reset_user_statistics(target_id):
            bot.send_message(chat_id, f"✅ Статистика пользователя {target_id} сброшена")
        else:
            bot.send_message(chat_id, f"❌ Не удалось сбросить статистику пользователя {target_id}")

    except ValueError:
        bot.send_message(chat_id, "❌ Неверный формат ID пользователя")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")


@bot.message_handler(commands=['grant_sub'])
def handle_grant_sub(message):
    """Выдача подписки пользователю с валидацией"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.send_message(chat_id,
                             "❌ Использование: /grant_sub <user_id> [days=30]\n\n"
                             "Примеры:\n"
                             "/grant_sub 123456789\n"
                             "/grant_sub 123456789 90")
            return

        # ВАЛИДАЦИЯ
        target_id = validate_user_id(parts[1])
        days = 30 if len(parts) < 3 else validate_days(parts[2])

        if db.grant_subscription(target_id, days):
            bot.send_message(chat_id,
                             f"✅ Пользователю {target_id} выдана подписка на {days} дней")
        else:
            bot.send_message(chat_id,
                             f"❌ Не удалось выдать подписку пользователю {target_id}")

    except ValueError as e:
        bot.send_message(chat_id, f"❌ Ошибка валидации: {e}")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")

@bot.message_handler(commands=['checkmypayment'])
def handle_check_my_payment(message):
    """Проверка последнего платежа пользователя"""
    chat_id = message.chat.id
    if not check_user_access(chat_id):
        return
    try:
        conn = db.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
        SELECT payment_id, status, created_at 
        FROM payments 
        WHERE telegram_id = ? 
        ORDER BY created_at DESC 
        LIMIT 1
        ''', (chat_id,))

        payment = cursor.fetchone()
        conn.close()

        if not payment:
            bot.send_message(chat_id, "📭 У вас нет активных платежей")
            return

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🔄 Проверить статус", callback_data=f"check_payment_{payment['payment_id']}"))

        bot.send_message(
            chat_id,
            f"""📋 <b>Ваш последний платеж</b>

🆔 ID: {payment['payment_id'][:8]}...
📅 Дата: {payment['created_at'][:19]}
📊 Статус: {payment['status']}

Нажмите кнопку ниже для проверки текущего статуса:""",
            parse_mode='HTML',
            reply_markup=markup
        )

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")

@bot.message_handler(commands=['extend_sub'])
def handle_extend_sub(message):
    """Команда для продления подписки"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав администратора.", reply_markup=create_main_menu())
        return

    try:
        parts = message.text.split()
        if len(parts) < 4:
            bot.send_message(
                chat_id,
                "❌ <b>Использование:</b>\n"
                "<code>/extend_sub &lt;user_id&gt; &lt;days&gt; &lt;hours&gt;</code>\n\n"
                "<b>Примеры:</b>\n"
                "<code>/extend_sub 123456789 7 0</code> - продлить на 7 дней\n"
                "<code>/extend_sub 123456789 0 12</code> - продлить на 12 часов\n"
                "<code>/extend_sub all 3 0</code> - продлить всем активным на 3 дня",
                parse_mode='HTML'
            )
            return

        if parts[1].lower() == 'all':
            # Продление всем
            days = int(parts[2])
            hours = int(parts[3])

            result = db.extend_all_active_subscriptions(hours=hours, days=days)

            time_text = ""
            if hours > 0 and days > 0:
                time_text = f"{hours} час(ов) и {days} день(ей)"
            elif hours > 0:
                time_text = f"{hours} час(ов)"
            elif days > 0:
                time_text = f"{days} день(ей)"

            report = f"✅ <b>Массовое продление завершено!</b>\n\n"
            report += f"📅 Срок: {time_text}\n"
            report += f"👥 Всего пользователей: {result['total']}\n"
            report += f"✅ Успешно: {result['success']}\n"
            report += f"❌ Ошибок: {result['failed']}"

            bot.send_message(chat_id, report, parse_mode='HTML')

        else:
            # Продление конкретному пользователю
            user_id = int(parts[1])
            days = int(parts[2])
            hours = int(parts[3])

            if db.extend_subscription(user_id, hours=hours, days=days):
                user_info = db.get_user(user_id)
                end_date = user_info.get('subscription_end_date', 'неизвестно')

                time_text = ""
                if hours > 0 and days > 0:
                    time_text = f"{hours} час(ов) и {days} день(ей)"
                elif hours > 0:
                    time_text = f"{hours} час(ов)"
                elif days > 0:
                    time_text = f"{days} день(ей)"

                report = f"✅ <b>Подписка продлена!</b>\n\n"
                report += f"👤 Пользователь ID: {user_id}\n"
                report += f"📅 Срок: {time_text}\n"
                report += f"🕐 Действует до: {end_date}"

                # Отправляем уведомление пользователю
                try:
                    notification = f"🎉 <b>Ваша подписка продлена!</b>\n\n"
                    notification += f"Администратор продлил вашу подписку на {time_text}.\n"
                    notification += f"Теперь она действует до: {end_date}"

                    bot.send_message(user_id, notification, parse_mode='HTML')
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
                    report += f"\n\n⚠️ Не удалось отправить уведомление пользователю"

            else:
                report = f"❌ <b>Не удалось продлить подписку</b>\n\n"
                report += f"Пользователь ID: {user_id}"

            bot.send_message(chat_id, report, parse_mode='HTML')

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")

@bot.message_handler(commands=['set_admin'])
def handle_set_admin(message):
    """Назначение администратора"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    try:
        parts = message.text.split()
        if len(parts) < 3:
            bot.send_message(chat_id, "❌ Использование: /set_admin <user_id> <1/0> (1 - назначить, 0 - снять)")
            return

        target_id = int(parts[1])
        is_admin = bool(int(parts[2]))

        if db.set_admin(target_id, is_admin):
            status = "назначен" if is_admin else "снят"
            bot.send_message(chat_id, f"✅ Пользователь {target_id} {status} администратором")
        else:
            bot.send_message(chat_id, f"❌ Не удалось изменить права пользователя {target_id}")

    except ValueError:
        bot.send_message(chat_id, "❌ Неверный формат данных")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")


@bot.message_handler(commands=['check_sub_sync'])
def handle_check_sub_sync(message):
    """Ручная проверка синхронизации подписок - ТЕПЕРЬ С ПОЛНОЙ СИНХРОНИЗАЦИЕЙ"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    bot.send_message(chat_id, "🔄 Запускаю ПОЛНУЮ синхронизацию подписок...")

    try:
        # 1. Сначала проверяем согласованность
        problems = check_subscription_consistency()

        # 2. ЗАПУСКАЕМ ПОЛНУЮ СИНХРОНИЗАЦИЮ (обновляет subscription_purchased)
        full_result = full_sync_subscriptions()

        # 3. Старая синхронизация (только новые платежи)
        old_result = sync_paid_subscriptions_on_startup()

        # Формируем отчет
        report = f"📊 <b>Результаты ПОЛНОЙ синхронизации:</b>\n\n"

        report += f"🔧 <b>Исправление subscription_purchased:</b>\n"
        report += f"✅ Исправлено пользователей: {full_result.get('fixed', 0)}\n"
        report += f"📋 Всего найдено: {full_result.get('total', 0)}\n\n"

        report += f"💰 <b>Обработка новых платежей:</b>\n"
        report += f"✅ Всего платежей: {old_result.get('total', 0)}\n"
        report += f"✅ Активировано подписок: {old_result.get('activated', 0)}\n"
        report += f"⏩ Пропущено: {old_result.get('skipped', 0)}\n"
        report += f"❌ Ошибок: {old_result.get('errors', 0)}\n\n"

        if problems:
            report += f"⚠️ <b>Проблем согласованности:</b> {len(problems)}\n"
            for i, problem in enumerate(problems[:5], 1):
                report += f"{i}. {problem[:100]}...\n"
            if len(problems) > 5:
                report += f"... и еще {len(problems) - 5} проблем\n"
        else:
            report += "✅ <b>Проблем с согласованностью не найдено</b>"

        bot.send_message(chat_id, report, parse_mode='HTML')

    except Exception as e:
        error_msg = f"❌ Ошибка выполнения команды /check_sub_sync: {e}"
        bot.send_message(chat_id, error_msg)
        logger.error(error_msg)
        logger.error(traceback.format_exc())

def main_menu_callback(call):
    """Обработчик главного меню"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    user = db.get_user(chat_id)
    if user:
        welcome_text = f"👋 С возвращением, {user.get('first_name', 'друг')}!"
    else:
        welcome_text = "👋 Вы в главном меню."

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=welcome_text,
        parse_mode='HTML',
        reply_markup=create_main_menu()
    )
    answer_callback_safe(bot, call.id)

def random_question_callback(call):
    """Обработчик случайного вопроса"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        answer_callback_safe(bot, call.id, "❌ Требуется активная подписка!")
        return

    # Устанавливаем тему "Все темы" через менеджер
    user_data_manager.update_user_data(
        chat_id,
        current_topic="🎲 Все темы (рандом)",
        current_question=None,
        correct_answer=None,
        numbered_answers={},
        answers_list=[]
    )

    # Отправляем вопрос
    answer_callback_safe(bot, call.id, "🎲 Загружаю случайный вопрос...")
    send_question_inline(chat_id, message_id)

def show_stats_callback(call):
    """Обработчик показа статистики"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        answer_callback_safe(bot, call.id, "❌ Требуется активная подписка!")
        return

    show_stats_message(chat_id, message_id)


def change_topic_callback(call):
    """Обработчик смены темы с отображением прогресса"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        answer_callback_safe(bot, call.id, "❌ Требуется активная подписка!")
        return

    # Формируем текст со списком тем
    topics_text = "📚 <b>ДОСТУПНЫЕ ТЕМЫ:</b>\n\n"

    # Получаем данные пользователя для отображения прогресса
    user_data = user_data_manager.get_user_data(chat_id)
    user_answered = user_data.get('answered_questions', {})

    for i, topic in enumerate(topics_list, 1):
        # Определяем общее количество вопросов
        if topic == "🎲 Все темы (рандом)":
            total_questions = sum(len(q) for q in questions_by_topic.values())
        else:
            total_questions = len(questions_by_topic.get(topic, []))

        # Получаем количество отвеченных
        answered_count = len(user_answered.get(topic, []))

        # Формируем строку с прогрессом
        if total_questions > 0:
            progress_percentage = (answered_count / total_questions * 100)
            progress_text = f" ({answered_count}/{total_questions} - {progress_percentage:.1f}%)"
        else:
            progress_text = ""

        topics_text += f"{i}. {topic}{progress_text}\n"

    topics_text += "\n👇 Выберите номер темы:"

    # Создаем inline клавиатуру
    markup = types.InlineKeyboardMarkup(row_width=5)

    # Кнопки с номерами тем (индекс начинается с 0)
    buttons = []
    for i in range(len(topics_list)):
        button_text = str(i + 1)  # Для отображения начинаем с 1
        buttons.append(types.InlineKeyboardButton(
            text=button_text,
            callback_data=f"t_{i}"  # Упрощенный формат: t_0, t_1 и т.д.
        ))

    # Добавляем кнопки по 5 в ряд
    for i in range(0, len(buttons), 5):
        markup.row(*buttons[i:i + 5])

    markup.row(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    try:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=topics_text,
            parse_mode='HTML',
            reply_markup=markup
        )
        answer_callback_safe(bot, call.id)
    except Exception as e:
        logger.error(f"Ошибка при изменении сообщения: {e}")
        answer_callback_safe(bot, call.id, "❌ Ошибка обновления меню")


def get_question_callback(call):
    """Обработчик получения вопроса"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        answer_callback_safe(bot, call.id, "❌ Требуется активная подписка!")
        return

    # Удаляем ответ на callback, чтобы не было двойных сообщений
    answer_callback_safe(bot, call.id, "🔄 Загружаю вопрос...")

    # Отправляем вопрос
    send_question_inline(chat_id, message_id)

def subscribe_info_callback(call):
    """Обработчик информации о подписке"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    user = db.get_user(chat_id)
    has_subscription = db.check_subscription(chat_id)

    if has_subscription and user and user.get('subscription_end_date'):
        try:
            end_datetime = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
            end_str = end_datetime.strftime("%d.%m.%Y в %H:%M")

            time_left = end_datetime - datetime.now(pytz.UTC)
            if time_left.total_seconds() > 0:
                days = time_left.days
                hours = time_left.seconds // 3600

                if days > 0:
                    time_left_str = f"{days} дн. {hours} ч."
                elif hours > 0:
                    time_left_str = f"{hours} ч."
                else:
                    time_left_str = f"менее часа"

                status_text = f"✅ <b>Подписка активна</b>\nДействует до: {end_str}\nОсталось: {time_left_str}"
            else:
                status_text = "❌ <b>Подписка истекла</b>"
        except:
            status_text = "✅ <b>Подписка активна</b>"
    else:
        status_text = "❌ <b>Подписка не активна</b>"

    markup = types.InlineKeyboardMarkup()
    if not has_subscription:
        markup.add(
            types.InlineKeyboardButton("💳 Оплатить подписку (69₽)", callback_data="pay_now"),
            types.InlineKeyboardButton("🎁 Пробный доступ", callback_data="trial")
        )
    markup.add(types.InlineKeyboardButton("📋 Условия подписки", callback_data="subscription_terms"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="main_menu"))

    info_text = f"""
💳 <b>Информация о подписке</b>

{status_text}

💰 <b>Тариф:</b>
• 30 дней - 69₽

🎁 <b>Пробный период:</b> 1 день бесплатно
📞 <b>Поддержка:</b> @ZlotaR
    """

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=info_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def subscribe_callback(call):
    """Обработчик оформления подписки - одна цена"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("💳 Оплатить 69₽", callback_data="pay_now"))
    markup.add(types.InlineKeyboardButton("📋 Условия подписки", callback_data="subscription_terms"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe_info"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="""💳 <b>Оформление подписки</b>

💰 <b>Стоимость:</b> 69₽
📅 <b>Срок:</b> 30 дней
🎁 <b>Что входит:</b>
• Полный доступ ко всем темам
• Неограниченное количество вопросов
• Статистика ответов
• Поддержка 24/7

👇 Нажмите "Оплатить 69₽" для продолжения""",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def pay_now_callback(call):
    """Обработчик оплаты"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем, настроена ли ЮKassa
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📞 Связь с поддержкой", url="https://t.me/ZlotaR"))
        markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ <b>Система оплаты временно недоступна</b>\n\nПожалуйста, свяжитесь с поддержкой для оформления подписки:\n@ZlotaR",
            parse_mode='HTML',
            reply_markup=markup
        )
        answer_callback_safe(bot, call.id, "❌ Система оплаты недоступна")
        return

    answer_callback_safe(bot, call.id, "🔄 Создаю платеж...")

    # Создаем платеж
    payment_info = create_yookassa_payment(chat_id)

    if not payment_info:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Попробовать снова", callback_data="pay_now"))
        markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="❌ <b>Не удалось создать платеж</b>\n\nПожалуйста, попробуйте снова или обратитесь в поддержку.",
            parse_mode='HTML',
            reply_markup=markup
        )
        return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Перейти к оплате", url=payment_info['confirmation_url']))
    markup.add(types.InlineKeyboardButton("✅ Проверить оплату", callback_data=f"check_payment_{payment_info['id']}"))
    markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"""💳 <b>Оплата подписки</b>

💰 Сумма: {SUBSCRIPTION_PRICE}₽
📅 Срок: {SUBSCRIPTION_DAYS} дней

👇 <b>Инструкция:</b>
1. Нажмите <b>"Перейти к оплате"</b>
2. Оплатите 69₽ удобным способом
3. После оплаты вернитесь в бот
4. Нажмите <b>"Проверить оплату"</b>

⚠️ <b>Важно:</b>
• Сохраните квитанцию об оплате
• При проблемах - обращайтесь в поддержку""",
        parse_mode='HTML',
        reply_markup=markup
    )

def trial_callback(call):
    """Обработчик пробного доступа с точным временем - ИСПРАВЛЕННАЯ"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем, использовал ли уже пробный доступ
    user = db.get_user(chat_id)
    if user and user.get('is_trial_used'):
        # Дополнительная проверка: возможно, пробный доступ уже истек
        if user.get('subscription_end_date'):
            try:
                end_datetime = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
                if end_datetime < datetime.now(pytz.UTC):
                    # Пробный доступ истек, можно предложить платную подписку
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))
                    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="main_menu"))

                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text="❌ <b>Пробный доступ уже был использован!</b>\n\nСрок пробного периода истек. Оформите подписку для продолжения использования.",
                        parse_mode='HTML',
                        reply_markup=markup
                    )
                    answer_callback_safe(bot, call.id, "❌ Пробный доступ уже был использован!")
                    return
            except:
                pass

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))
        markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="main_menu"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="❌ <b>Пробный доступ уже был использован!</b>\n\nОформите подписку.",
            parse_mode='HTML',
            reply_markup=markup
        )
        answer_callback_safe(bot, call.id, "❌ Пробный доступ уже был использован!")
        return

    # Даем пробный доступ на 1 день от текущего момента
    end_datetime = datetime.now(pytz.UTC) + timedelta(days=1)

    # Проверяем, нет ли уже активной подписки
    if user and user.get('subscription_paid') and user.get('subscription_end_date'):
        try:
            current_end = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
            if current_end > datetime.now(pytz.UTC):
                # Уже есть активная подписка
                answer_callback_safe(bot, call.id, "✅ У вас уже есть активная подписка!")
                return
        except:
            pass

    db.update_subscription(
        telegram_id=chat_id,
        paid_status=True,
        end_datetime=end_datetime,
        is_trial=True,
        is_purchased=False
    )

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🚀 Начать обучение", callback_data="main_menu"))
    markup.add(types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"))

    end_str = end_datetime.strftime("%d.%m.%Y в %H:%M")
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"🎉 <b>Пробный доступ активирован!</b>\n\nДоступ до {end_str}",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id, "✅ Пробный доступ активирован!")

def info_callback(call):
    """Обработчик информации о боте"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    info_text = f"""
ℹ️ <b>Информация о боте</b>

🤖 <b>Бот для подготовки к тестам</b>
Версия: 1.2
❓<b>Обновление от 11.02.2026 что нового?</b>
• Обновлена система отправки вопросов.
• Увеличена стабильность системы.
• Появилась более точная статистика.
• Появилась возможность обнулять статистику.
❓<b>Обновление от 18.03.2026 что нового?</b>
• Обновлён банк вопросов под актальный файл.
• Увеличение стабильности системы.
📚 <b>Загружено:</b>
• Тем: {len(topics_list) - 1 if topics_list else 0}
• Вопросов: {sum(len(q) for q in questions_by_topic.values()) if questions_by_topic else 0}

📞 <b>Поддержка:</b> @ZlotaR
    """

    markup = create_back_button("main_menu")
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=info_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def help_menu_callback(call):
    """Обработчик помощи"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    help_text = """
🆘 <b>Помощь и инструкции</b>

❓ <b>Как начать обучение?</b>
1. Нажмите "Выбрать тему"
2. Выберите интересующую тему
3. Начните отвечать на вопросы

📊 <b>Как работает статистика?</b>
• Отслеживаются правильные/неправильные ответы
• Рассчитывается процент правильных ответов
• Можно просмотреть в любое время

💳 <b>Как оформить подписку?</b>
1. Нажмите "Оформить подписку"
2. Выберите тариф
3. Оплатите удобным способом
4. Отправьте чек в поддержку

🔧 <b>Проблемы с ботом?</b>
• Обратитесь в поддержку @ZlotaR
    """

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Подписка", callback_data="subscribe_info"))
    markup.add(types.InlineKeyboardButton("📚 Темы", callback_data="change_topic"))
    markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=help_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def check_questions_callback(call):
    """Обработчик проверки вопросов"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    global questions_loaded
    questions_loaded = check_and_load_questions()

    if questions_loaded:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="✅ Вопросы успешно загружены!",
            reply_markup=create_back_button("main_menu")
        )
    else:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="❌ Не удалось загрузить вопросы.",
            reply_markup=create_back_button("main_menu")
        )
    answer_callback_safe(bot, call.id)


def show_stats_message(chat_id, message_id=None):
    """Показать статистику пользователя с прогрессом по темам"""
    stats = db.get_user_statistics(chat_id)
    user_data = user_data_manager.get_user_data(chat_id)

    # Получаем отвеченные вопросы
    user_answered = user_data.get('answered_questions', {})

    if not stats or stats['total_answers'] == 0:
        stats_text = "📊 Статистика еще не собрана. Начните отвечать на вопросы!"
    else:
        total_answers = stats['total_answers']
        correct_answers = stats['correct_answers']
        correct_percentage = (correct_answers / total_answers) * 100

        # Получаем статистику сессии через менеджер
        session_stats_data = user_data_manager.get_session_stats(chat_id)
        session_total = session_stats_data.get('session_total', 0)
        session_correct = session_stats_data.get('session_correct', 0)
        session_percentage = (session_correct / session_total * 100) if session_total > 0 else 0

        stats_text = f"""
📊 <b>ВАША СТАТИСТИКА</b>

📈 <b>Всего отвечено вопросов:</b> {total_answers}
✅ <b>Правильных ответов:</b> {correct_answers}
❌ <b>Неправильных ответов:</b> {total_answers - correct_answers}
🎯 <b>Процент правильных ответов:</b> {correct_percentage:.1f}%

📊 <b>Статистика сессии:</b>
✅ Правильных: {session_correct}/{session_total} ({session_percentage:.1f}%)

📚 <b>Прогресс по темам:</b>
"""

        # Добавляем прогресс по каждой теме
        for topic in topics_list:
            if topic == "🎲 Все темы (рандом)":
                total_questions = sum(len(q) for q in questions_by_topic.values())
            else:
                total_questions = len(questions_by_topic.get(topic, []))

            if total_questions > 0:
                answered_count = len(user_answered.get(topic, []))
                progress_percentage = (answered_count / total_questions * 100) if total_questions > 0 else 0
                stats_text += f"\n• {topic}: {answered_count}/{total_questions} ({progress_percentage:.1f}%)"

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton("🏆 Топ игроков", callback_data="top_players"))

    # Проверяем наличие темы
    if user_data.get('current_topic'):
        markup.add(
            types.InlineKeyboardButton("🎲 Продолжить", callback_data="get_question"),
            types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic")
        )
    else:
        markup.add(
            types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"),
            types.InlineKeyboardButton("🎲 Случайный вопрос", callback_data="random_question")
        )

    # 🔥 НОВАЯ КНОПКА СБРОСА СТАТИСТИКИ
    markup.add(
        types.InlineKeyboardButton("🔄 Сбросить мою статистику", callback_data="reset_my_stats"),
        types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )
    if message_id:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=stats_text,
            parse_mode='HTML',
            reply_markup=markup
        )
    else:
        bot.send_message(
            chat_id,
            stats_text,
            parse_mode='HTML',
            reply_markup=markup
        )


def reset_my_stats_callback(call):
    """Сброс статистики пользователем с подтверждением"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    if call.data == "reset_my_stats":
        # Запрашиваем подтверждение
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Да, сбросить всё", callback_data="confirm_reset_stats"),
            types.InlineKeyboardButton("❌ Нет, отмена", callback_data="show_stats")
        )

        try:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⚠️ <b>ПОДТВЕРДИТЕ СБРОС СТАТИСТИКИ</b>\n\n"
                     "Вы уверены, что хотите сбросить всю свою статистику?\n\n"
                     "• Обнулятся все правильные/неправильные ответы\n"
                     "• Сбросится прогресс по всем темам\n"
                     "• Очистится статистика сессии\n\n"
                     "<b>Это действие необратимо!</b>",
                parse_mode='HTML',
                reply_markup=markup
            )
        except Exception as e:
            bot.send_message(
                chat_id=chat_id,
                text="⚠️ <b>ПОДТВЕРДИТЕ СБРОС СТАТИСТИКИ</b>\n\n"
                     "Вы уверены, что хотите сбросить всю свою статистику?\n\n"
                     "• Обнулятся все правильные/неправильные ответы\n"
                     "• Сбросится прогресс по всем темам\n"
                     "• Очистится статистика сессии\n\n"
                     "<b>Это действие необратимо!</b>",
                parse_mode='HTML',
                reply_markup=markup
            )
        answer_callback_safe(bot, call.id)

    elif call.data == "confirm_reset_stats":
        # Выполняем сброс статистики
        success = db.reset_user_statistics(chat_id)

        # Полностью очищаем все данные пользователя в менеджере
        user_data_manager.clear_user_data(chat_id)

        # Сбрасываем answered_questions и session_questions
        user_data = user_data_manager.get_user_data(chat_id)
        user_data['answered_questions'] = {}
        user_data['session_questions'] = {}

        # Сбрасываем статистику сессии
        session_stats = user_data_manager.get_session_stats(chat_id)
        session_stats['session_total'] = 0
        session_stats['session_correct'] = 0

        if success:
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"),
                types.InlineKeyboardButton("🎲 Случайный вопрос", callback_data="random_question")
            )
            markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

            try:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="✅ <b>Статистика успешно сброшена!</b>\n\n"
                         "Теперь вы можете начать обучение с чистого листа.",
                    parse_mode='HTML',
                    reply_markup=markup
                )
            except:
                bot.send_message(
                    chat_id=chat_id,
                    text="✅ <b>Статистика успешно сброшена!</b>\n\n"
                         "Теперь вы можете начать обучение с чистого листа.",
                    parse_mode='HTML',
                    reply_markup=markup
                )

            logger.info(f"👤 Пользователь {chat_id} сбросил свою статистику")
            answer_callback_safe(bot, call.id, "✅ Статистика сброшена!")
        else:
            bot.send_message(
                chat_id=chat_id,
                text="❌ <b>Ошибка при сбросе статистики</b>\n\n"
                     "Пожалуйста, попробуйте позже или обратитесь в поддержку.",
                parse_mode='HTML'
            )
            answer_callback_safe(bot, call.id, "❌ Ошибка сброса")

def admin_broadcast_callback(call):
    """Массовая рассылка через админ-панель"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Сохраняем состояние пользователя
    user_data_manager.broadcast_states[chat_id] = {
        'state': 'waiting_for_message',
        'message': None,
        'confirmed': False
    }

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="📢 <b>МАССОВАЯ РАССЫЛКА</b>\n\n"
             "Отправьте сообщение, которое хотите разослать всем пользователям бота.\n"
             "Можно использовать HTML-разметку.\n\n"
             "<i>Или нажмите кнопку Отмена для выхода</i>",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def admin_extend_sub_callback(call):
    """Меню продления подписки"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("👤 Одному пользователю", callback_data="extend_user_menu"),
        types.InlineKeyboardButton("👥 Всем активным", callback_data="extend_all_menu")
    )
    markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="⏱️ <b>Продление подписки</b>\n\n"
             "Выберите, кому продлить подписку:",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def extend_user_menu_callback(call):
    """Меню продления подписки конкретному пользователю"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Сохраняем состояние
    user_data_manager.extend_states[chat_id] = {
        'state': 'waiting_for_user_id',
        'action': 'extend_user',
        'user_id': None,
        'hours': 0,
        'days': 0
    }

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="admin_extend_sub"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="👤 <b>Продление подписки пользователю</b>\n\n"
             "Отправьте ID пользователя, которому нужно продлить подписку:",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def extend_all_menu_callback(call):
    """Меню продления подписки всем активным пользователям"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup(row_width=3)

    # Часы
    markup.row(types.InlineKeyboardButton("🕐 +1 час", callback_data="extend_all_hours_1"),
               types.InlineKeyboardButton("🕑 +3 часа", callback_data="extend_all_hours_3"),
               types.InlineKeyboardButton("🕒 +6 часов", callback_data="extend_all_hours_6"))
    markup.row(types.InlineKeyboardButton("🕓 +12 часов", callback_data="extend_all_hours_12"),
               types.InlineKeyboardButton("🕔 +24 часа", callback_data="extend_all_hours_24"))

    # Дни
    markup.row(types.InlineKeyboardButton("📅 +1 день", callback_data="extend_all_days_1"),
               types.InlineKeyboardButton("📅 +3 дня", callback_data="extend_all_days_3"),
               types.InlineKeyboardButton("📅 +7 дней", callback_data="extend_all_days_7"))
    markup.row(types.InlineKeyboardButton("📅 +14 дней", callback_data="extend_all_days_14"),
               types.InlineKeyboardButton("📅 +30 дней", callback_data="extend_all_days_30"),
               types.InlineKeyboardButton("📅 +60 дней", callback_data="extend_all_days_60"))

    markup.row(types.InlineKeyboardButton("↩️ Назад", callback_data="admin_extend_sub"))

    # Получаем статистику активных пользователей
    all_users = db.get_all_users()
    active_users = [u for u in all_users if db.check_subscription(u['telegram_id'])]

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"👥 <b>Продление подписки всем активным пользователям</b>\n\n"
             f"📊 Активных подписок: {len(active_users)}\n\n"
             "Выберите срок продления:",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)





def handle_extend_all_callback(call):
    """Обработка выбора срока продления для всех"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Парсим данные из callback (формат: extend_all_hours_1 или extend_all_days_1)
        parts = call.data.split('_')

        # Проверяем формат
        if len(parts) < 4:
            logger.error(f"❌ Неверный формат callback: {call.data}")
            answer_callback_safe(bot, call.id, "❌ Неверный формат команды")
            return

        # Определяем тип и значение
        time_type = parts[2]  # hours или days
        value = int(parts[3])  # значение

        logger.info(f"📊 Продление всем: тип={time_type}, значение={value}")

        # Устанавливаем часы и дни
        hours = value if time_type == 'hours' else 0
        days = value if time_type == 'days' else 0

        # Создаем подтверждающее сообщение
        time_text = ""
        if hours > 0 and days > 0:
            time_text = f"{hours} час(ов) и {days} день(ей)"
        elif hours > 0:
            time_text = f"{hours} час(ов)"
        elif days > 0:
            time_text = f"{days} день(ей)"

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(f"✅ Да, продлить на {time_text}",
                                       callback_data=f"confirm_extend_all_{hours}_{days}"),
            types.InlineKeyboardButton("❌ Нет, отмена", callback_data="admin_extend_sub")
        )

        # Получаем статистику активных пользователей
        all_users = db.get_all_users()
        active_users = [u for u in all_users if db.check_subscription(u['telegram_id'])]

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"⚠️ <b>Подтверждение продления</b>\n\n"
                 f"Вы уверены, что хотите продлить подписку ВСЕМ активным пользователям на {time_text}?\n\n"
                 f"📊 Будет затронуто: {len(active_users)} пользователей",
            parse_mode='HTML',
            reply_markup=markup
        )
        answer_callback_safe(bot, call.id, f"⏳ Продление на {time_text}...")

    except ValueError as e:
        logger.error(f"❌ Ошибка преобразования значения: {e}, callback: {call.data}")
        answer_callback_safe(bot, call.id, "❌ Ошибка в значении срока")
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_extend_all_callback: {e}")
        logger.error(traceback.format_exc())
        answer_callback_safe(bot, call.id, "❌ Ошибка обработки запроса")


def handle_confirm_extend_callback(call):
    """Подтверждение и выполнение продления"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        if call.data.startswith("confirm_extend_all_"):
            # Продление всем
            parts = call.data.split('_')
            # confirm_extend_all_1_0 -> ['confirm', 'extend', 'all', '1', '0']
            hours = int(parts[3]) if len(parts) >= 4 else 0
            days = int(parts[4]) if len(parts) >= 5 else 0

            answer_callback_safe(bot, call.id, "⏳ Продлеваю подписки...")
            logger.info(f"🚀 ЗАПУСК ПРОДЛЕНИЯ ВСЕМ: +{days} дней, +{hours} часов")

            result = db.extend_all_active_subscriptions(hours=hours, days=days)

            time_text = f"{days} дн. {hours} ч." if days > 0 or hours > 0 else "0 часов 0 дней"

            report = f"✅ <b>Продление завершено!</b>\n\n"
            report += f"📅 Срок: {time_text}\n"
            report += f"👥 Всего пользователей: {result.get('total', 0)}\n"
            report += f"✅ Успешно: {result.get('success', 0)}\n"
            report += f"❌ Ошибок: {result.get('failed', 0)}\n"

        elif call.data.startswith("confirm_extend_user_"):
            # Продление конкретному пользователю
            # Формат: confirm_extend_user_123456789_1_0
            parts = call.data.split('_')
            # ['confirm', 'extend', 'user', '123456789', '1', '0']
            if len(parts) >= 6:
                user_id = int(parts[3])
                hours = int(parts[4])
                days = int(parts[5])

                answer_callback_safe(bot, call.id, "⏳ Продлеваю подписку...")

                # Выполняем продление
                if db.extend_subscription(user_id, hours=hours, days=days):
                    user_info = db.get_user(user_id)
                    end_date = user_info.get('subscription_end_date', 'неизвестно')
                    username = user_info.get('username', 'нет username')

                    time_text = f"{days} дн. {hours} ч." if days > 0 or hours > 0 else "0 часов 0 дней"

                    report = f"✅ <b>Подписка продлена!</b>\n\n"
                    report += f"👤 Пользователь ID: {user_id}\n"
                    report += f"📱 Username: @{username}\n"
                    report += f"📅 Срок: {time_text}\n"
                    report += f"🕐 Действует до: {end_date}\n\n"

                    # Отправляем уведомление пользователю
                    try:
                        notification = f"🎉 <b>Ваша подписка продлена!</b>\n\n"
                        notification += f"Администратор продлил вашу подписку на {time_text}.\n"
                        notification += f"Теперь она действует до: {end_date}"
                        bot.send_message(user_id, notification, parse_mode='HTML')
                        report += f"✅ Уведомление отправлено пользователю"
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление {user_id}: {e}")
                        report += f"⚠️ Не удалось отправить уведомление"
                else:
                    report = f"❌ <b>Не удалось продлить подписку</b>\n\nПользователь ID: {user_id}"
            else:
                report = "❌ <b>Ошибка формата данных</b>"
        else:
            return

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("↩️ Назад к продлению", callback_data="admin_extend_sub"))
        markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=report,
            parse_mode='HTML',
            reply_markup=markup
        )

    except Exception as e:
        logger.error(f"❌ Ошибка в handle_confirm_extend_callback: {e}")
        logger.error(traceback.format_exc())
        answer_callback_safe(bot, call.id, "❌ Ошибка при продлении")

def handle_admin_callback(call):
    """Обработка административных callback-запросов"""
    chat_id = call.message.chat.id

    # ПРОВЕРКА ПРАВ В САМОМ НАЧАЛЕ
    user = db.get_user(chat_id)
    if not user or not user.get('is_admin'):
        logger.warning(f"⚠️ Попытка доступа к админке от {chat_id} без прав")
        try:
            bot.answer_callback_query(call.id, "❌ У вас нет прав администратора!", show_alert=True)
        except:
            pass
        return

    message_id = call.message.message_id
    logger.info(f"👑 Админ callback: {call.data} от {chat_id}")

    if call.data == "admin_stats":
        admin_stats_callback(call)
    elif call.data == "admin_users":
        admin_users_callback(call)
    elif call.data == "admin_grant_sub":
        admin_grant_sub_callback(call)
    elif call.data == "admin_extend_sub":
        admin_extend_sub_callback(call)
    elif call.data == "extend_all_menu":
        extend_all_menu_callback(call)
    elif call.data == "extend_user_menu":
        extend_user_menu_callback(call)
    # Обработка всех вариантов продления для всех пользователей
    elif call.data.startswith("extend_all_hours_") or call.data.startswith("extend_all_days_"):
        handle_extend_all_callback(call)
    # Обработка всех вариантов продления для конкретного пользователя
    elif call.data.startswith("extend_user_"):
        handle_extend_user_callback(call)
    elif call.data == "admin_grant_admin":
        admin_grant_admin_callback(call)
    elif call.data == "admin_broadcast":
        handle_send_all_users(call)
    elif call.data == "admin_logs":
        admin_logs_callback(call)
    elif call.data == "admin_restart":
        admin_restart_callback(call)
    elif call.data == "admin_db":
        admin_db_callback(call)
    elif call.data.startswith("confirm_extend_"):
        handle_confirm_extend_callback(call)
    elif call.data == "back_to_admin":
        back_to_admin_callback(call)
    elif call.data == "logs_last_100":
        logs_last_100_callback(call)
    elif call.data == "logs_stats":
        logs_stats_callback(call)
    elif call.data == "logs_get_file":
        logs_get_file_callback(call)
    elif call.data == "logs_clear":
        logs_clear_callback(call)
    elif call.data == "logs_clear_confirm":
        logs_clear_confirm_callback(call)
    elif call.data == "restart_confirm":
        restart_confirm_callback(call)
    else:
        logger.warning(f"⚠️ Неизвестный админ callback: {call.data}")
        answer_callback_safe(bot, call.id, "❌ Неизвестная команда администратора")

def admin_stats_callback(call):
    """Статистика системы"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    all_users = db.get_all_users()
    active_users = [u for u in all_users if db.check_subscription(u['telegram_id'])]

    stats_text = f"""
📊 <b>Статистика системы</b>

👥 <b>Всего пользователей:</b> {len(all_users)}
✅ <b>Активных подписок:</b> {len(active_users)}
👑 <b>Администраторов:</b> {sum(1 for u in all_users if u.get('is_admin'))}

📅 <b>Последние 5 регистраций:</b>
"""

    for i, user in enumerate(all_users[:5], 1):
        username = user.get('username', 'нет username')
        first_name = user.get('first_name', '')
        reg_date = user.get('registration_date', 'неизвестно')
        stats_text += f"\n{i}. {first_name} (@{username}) - {reg_date[:10]}"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=stats_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def admin_users_callback(call):
    """Список пользователей"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    all_users = db.get_all_users()

    users_text = f"""
👥 <b>Список пользователей</b>
Всего: {len(all_users)}

<b>Последние 10 пользователей:</b>
"""

    for i, user in enumerate(all_users[:10], 1):
        username = user.get('username', 'нет username')
        first_name = user.get('first_name', '')
        user_id = user['telegram_id']
        is_admin = "👑" if user.get('is_admin') else ""
        has_sub = "✅" if db.check_subscription(user_id) else "❌"

        users_text += f"\n{i}. {first_name} (@{username}) ID: {user_id} {is_admin} {has_sub}"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=users_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def admin_grant_sub_callback(call):
    """Выдача подписки"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="🔑 <b>Выдача подписки</b>\n\nИспользуйте команду:\n<code>/grant_sub &lt;user_id&gt; [days]</code>\n\nПример:\n<code>/grant_sub 123456789 30</code>",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def admin_grant_admin_callback(call):
    """Назначение администратора"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="👑 <b>Назначение администратора</b>\n\nИспользуйте команду:\n<code>/set_admin &lt;user_id&gt; &lt;1/0&gt;</code>\n\n1 - назначить администратором\n0 - снять права администратора\n\nПример:\n<code>/set_admin 123456789 1</code>",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def admin_logs_callback(call):
    """Управление логами"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📄 Последние 100 строк", callback_data="logs_last_100"),
        types.InlineKeyboardButton("📊 Статистика логов", callback_data="logs_stats")
    )
    markup.add(
        types.InlineKeyboardButton("📁 Получить файл логов", callback_data="logs_get_file"),
        types.InlineKeyboardButton("🧹 Очистить логи", callback_data="logs_clear")
    )
    markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="📝 <b>Управление логами</b>\n\nВыберите действие:",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def admin_restart_callback(call):
    """Перезагрузка бота"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🔄 Да, перезагрузить", callback_data="restart_confirm"),
        types.InlineKeyboardButton("❌ Нет, отмена", callback_data="back_to_admin")
    )

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="⚠️ <b>ВНИМАНИЕ!</b>\n\nВы действительно хотите перезагрузить бота?\nЭто действие перезапустит все системы.",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def back_to_admin_callback(call):
    """Назад в админку"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
        types.InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")
    )
    markup.add(
        types.InlineKeyboardButton("🔑 Выдать подписку", callback_data="admin_grant_sub"),
        types.InlineKeyboardButton("⏱️ Продлить подписку", callback_data="admin_extend_sub")  # НОВАЯ КНОПКА
    )
    markup.add(
        types.InlineKeyboardButton("👑 Назначить админа", callback_data="admin_grant_admin"),
        types.InlineKeyboardButton("📢 Массовая рассылка", callback_data="admin_broadcast")
    )
    markup.add(
        types.InlineKeyboardButton("📝 Логи", callback_data="admin_logs"),
        types.InlineKeyboardButton("🔄 Рестарт", callback_data="admin_restart")
    )
    markup.add(
        types.InlineKeyboardButton("🗄️ Скачать БД", callback_data="admin_db"),
        types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="👑 <b>Панель администратора</b>\n\nВыберите действие:",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def top_players_callback(call):
    """Топ игроков"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    top_users = db.get_top_users(10)

    if not top_users:
        top_text = "🏆 <b>Топ игроков</b>\n\nПока никто не ответил на вопросы."
    else:
        top_text = "🏆 <b>ТОП-10 ИГРОКОВ</b>\n\n"

        for i, user in enumerate(top_users, 1):
            username = user.get('username', 'нет username')
            first_name = user.get('first_name', '')
            correct = user['correct_answers']
            total = user['total_answers']
            rate = user.get('success_rate', 0)

            top_text += f"{i}. {first_name} (@{username})\n"
            top_text += f"   📊 {correct}/{total} ({rate}%)\n\n"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📊 Моя статистика", callback_data="show_stats"))
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=top_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

def subscription_terms_callback(call):
    """Условия подписки - упрощенные"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    terms_text = f"""
📋 <b>Условия подписки</b>

✅ <b>Что входит в подписку за {SUBSCRIPTION_PRICE}₽:</b>
• Полный доступ ко всем темам
• Неограниченное количество вопросов
• Статистика ответов
• Поддержка 24/7

⏱️ <b>Срок действия:</b>
• Подписка на {SUBSCRIPTION_DAYS} дней
• Активируется сразу после оплаты
• Автопродление не предусмотрено
• Для продления подписки необходимо просто оплатить ещё раз,
  оплаченные дни суммируются!


💰 <b>Стоимость:</b>
• {SUBSCRIPTION_PRICE}₽ за {SUBSCRIPTION_DAYS} дней

📞 <b>Поддержка:</b>
• Telegram: @ZlotaR
• Ответ в течение 24 часов
    """

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Оплатить подписку", callback_data="pay_now"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe_info"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=terms_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)


def full_sync_subscriptions():
    """ПОЛНАЯ синхронизация - обновляет ВСЕХ пользователей с успешными платежами"""
    logger.info("🔄 Запуск ПОЛНОЙ синхронизации подписок...")

    try:
        conn = db.get_connection()
        cursor = conn.cursor()

        # Находим ВСЕХ пользователей с успешными платежами, у которых subscription_purchased = FALSE
        cursor.execute('''
        SELECT DISTINCT 
            p.telegram_id,
            u.username,
            u.subscription_paid,
            u.subscription_end_date,
            u.subscription_purchased
        FROM payments p
        JOIN users u ON p.telegram_id = u.telegram_id
        WHERE p.status = 'succeeded'
        AND (u.subscription_purchased = FALSE OR u.subscription_purchased IS NULL)
        ''')

        users_to_fix = cursor.fetchall()

        if not users_to_fix:
            logger.info("✅ Нет пользователей для исправления")
            return {'fixed': 0, 'total': 0}

        fixed_count = 0
        for user_data in users_to_fix:
            user_id = user_data[0]

            # Устанавливаем subscription_purchased = TRUE
            cursor.execute('''
            UPDATE users 
            SET subscription_purchased = TRUE,
                last_activity = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            ''', (user_id,))

            # Также проверяем и обновляем subscription_paid если нужно
            if not user_data[2]:  # subscription_paid = FALSE
                # Берем дату из последнего успешного платежа
                cursor.execute('''
                SELECT paid_at, created_at
                FROM payments 
                WHERE telegram_id = ? AND status = 'succeeded'
                ORDER BY paid_at DESC, created_at DESC
                LIMIT 1
                ''', (user_id,))

                payment = cursor.fetchone()
                if payment:
                    payment_date = payment[0] or payment[1]
                    try:
                        if payment_date:
                            start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            end_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')

                            cursor.execute('''
                            UPDATE users 
                            SET subscription_paid = TRUE,
                                subscription_start_date = ?,
                                subscription_end_date = ?
                            WHERE telegram_id = ?
                            ''', (start_date, end_date, user_id))
                    except:
                        pass

            fixed_count += 1

        conn.commit()
        conn.close()

        logger.info(f"✅ Полная синхронизация: исправлено {fixed_count} пользователей")
        return {'fixed': fixed_count, 'total': len(users_to_fix)}

    except Exception as e:
        logger.error(f"❌ Ошибка полной синхронизации: {e}")
        logger.error(traceback.format_exc())
        return {'fixed': 0, 'total': 0, 'error': str(e)}

def check_payment_callback(call):
    """Проверка статуса платежа с улучшенной обработкой ошибок"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Извлекаем payment_id из callback данных
        if not call.data or '_' not in call.data:
            answer_callback_safe(bot, call.id, "❌ Ошибка в данных запроса")
            return

        parts = call.data.split('_')
        if len(parts) < 3:
            answer_callback_safe(bot, call.id, "❌ Неверный формат запроса")
            return

        payment_id = parts[2]

        # ДОБАВЛЯЕМ ПРОВЕРКУ НАЛИЧИЯ payment_id
        if not payment_id or len(payment_id) < 5:
            answer_callback_safe(bot, call.id, "❌ Неверный ID платежа")
            return

        answer_callback_safe(bot, call.id, "🔄 Проверяем статус оплаты...")

        # Проверяем статус платежа с таймаутом и обработкой ошибок
        payment = None
        try:
            # Пробуем получить статус платежа с таймаутом
            payment = Payment.find_one(payment_id)  # Таймаут 10 секунд
        except yookassa.errors.ApiError as api_error:
            logger.error(f"❌ Ошибка API ЮKassa: {api_error}")
            # Показываем пользователю сообщение об ошибке API
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Попробовать снова",
                                                  callback_data=f"check_payment_{payment_id}"))
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⚠️ <b>Временная проблема с платежной системой</b>\n\n"
                     "Не удалось проверить статус платежа из-за ошибки в платежной системе.\n"
                     "Попробуйте проверить статус позже или обратитесь в поддержку.",
                parse_mode='HTML',
                reply_markup=markup
            )
            return
        except requests.exceptions.Timeout:
            logger.error(f"❌ Таймаут при запросе статуса платежа {payment_id}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Попробовать снова",
                                                  callback_data=f"check_payment_{payment_id}"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏱️ <b>Таймаут запроса</b>\n\n"
                     "Платежная система не отвечает.\n"
                     "Попробуйте проверить статус позже.",
                parse_mode='HTML',
                reply_markup=markup
            )
            return
        except requests.exceptions.ConnectionError:
            logger.error(f"❌ Ошибка соединения при проверке платежа {payment_id}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Попробовать снова",
                                                  callback_data=f"check_payment_{payment_id}"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="🔌 <b>Ошибка соединения</b>\n\n"
                     "Нет подключения к платежной системе.\n"
                     "Проверьте ваше интернет-соединение.",
                parse_mode='HTML',
                reply_markup=markup
            )
            return
        except Exception as api_error:
            logger.error(f"❌ Неизвестная ошибка при проверке платежа {payment_id}: {api_error}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Попробовать снова",
                                                  callback_data=f"check_payment_{payment_id}"))
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⚠️ <b>Неизвестная ошибка</b>\n\n"
                     "Произошла непредвиденная ошибка при проверке платежа.\n"
                     "Попробуйте позже или обратитесь в поддержку.",
                parse_mode='HTML',
                reply_markup=markup
            )
            return

        # Если платеж не найден
        if not payment:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("💳 Попробовать снова", callback_data="pay_now"))
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="❌ <b>Платеж не найден</b>\n\n"
                     "Не удалось найти информацию о платеже.\n"
                     "Возможно, платеж был отменен или произошла ошибка при его создании.",
                parse_mode='HTML',
                reply_markup=markup
            )
            return

        # Обновляем статус в базе данных
        db.update_payment_status(payment_id, payment.status)

        if payment.status == 'succeeded':
            # Проверяем, не был ли платеж уже обработан
            conn = db.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
            SELECT is_processed, telegram_id 
            FROM payments 
            WHERE payment_id = ?
            ''', (payment_id,))

            payment_data = cursor.fetchone()
            conn.close()

            if payment_data and payment_data['is_processed']:
                # Платеж уже был обработан ранее
                answer_callback_safe(bot, call.id, "✅ Платеж уже был обработан ранее")
                user = db.get_user(chat_id)
                if user and not user.get('subscription_purchased'):
                    # Если пользователь не отмечен как купивший, исправляем
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute('''
                            UPDATE users 
                            SET subscription_purchased = TRUE 
                            WHERE telegram_id = ?
                            ''', (chat_id,))
                    conn.commit()
                    conn.close()
                    logger.info(f"✅ Исправлен subscription_purchased для {chat_id}")
                # Просто показываем статус
                user_info = db.get_user(chat_id)
                end_date = user_info.get('subscription_end_date', 'неизвестно')

                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("🚀 Начать обучение", callback_data="main_menu"))

                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"""✅ <b>Платеж уже был обработан</b>

💰 Сумма: {SUBSCRIPTION_PRICE}₽
📅 Подписка активна до: {end_date}
🎉 Теперь вам доступны все функции бота!""",
                    parse_mode='HTML',
                    reply_markup=markup
                )
                return

            # Платеж успешен и еще не обработан
            telegram_id = payment.metadata.get('telegram_id') if hasattr(payment, 'metadata') else chat_id

            # Улучшенная логика активации подписки
            user = db.get_user(telegram_id)
            if user and user.get('subscription_paid'):
                # У пользователя уже есть активная подписка
                if user.get('subscription_end_date'):
                    try:
                        current_end = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
                        # Продлеваем от текущей даты окончания, если она в будущем
                        if current_end > datetime.now(pytz.UTC):
                            end_datetime = current_end + timedelta(days=30)
                        else:
                            # Иначе начинаем с текущего момента + 1 день (буфер)
                            end_datetime = datetime.now(pytz.UTC) + timedelta(days=30)
                    except:
                        end_datetime = datetime.now(pytz.UTC) + timedelta(days=30)
                else:
                    end_datetime = datetime.now(pytz.UTC) + timedelta(days=30)
            else:
                # Новая подписка
                end_datetime = datetime.now(pytz.UTC) + timedelta(days=30)

            # Активируем подписку с пометкой о покупке
            db.update_subscription(
                telegram_id=telegram_id,
                paid_status=True,
                end_datetime=end_datetime,
                is_trial=False,
                is_purchased=True  # Указываем, что это купленная подписка
            )

            # Помечаем платеж как обработанный
            db.mark_payment_processed(payment_id)

            end_str = end_datetime.strftime("%d.%m.%Y в %H:%M")

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🚀 Начать обучение", callback_data="main_menu"))
            markup.add(types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"""✅ <b>Оплата успешно завершена!</b>

💰 Сумма: {SUBSCRIPTION_PRICE}₽
📅 Подписка активна до: {end_str}
🎉 Теперь вам доступны все функции бота!

Приятного обучения!""",
                parse_mode='HTML',
                reply_markup=markup
            )

        elif payment.status == 'pending':
            # Платеж в обработке
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_payment_{payment_id}"))
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏳ <b>Оплата в обработке</b>\n\n"
                     "Платеж получен, но еще не подтвержден банком.\n"
                     "Подождите 1-2 минуты и проверьте снова.\n\n"
                     "Если статус не меняется в течение 15 минут,\n"
                     "обратитесь в поддержку.",
                parse_mode='HTML',
                reply_markup=markup
            )

        elif payment.status == 'waiting_for_capture':
            # Платеж ожидает подтверждения (захвата)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_payment_{payment_id}"))
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏳ <b>Ожидание подтверждения</b>\n\n"
                     "Платеж авторизован, но ожидает подтверждения (захвата).\n"
                     "Обычно это занимает несколько минут.",
                parse_mode='HTML',
                reply_markup=markup
            )

        else:
            # Платеж отменен или отклонен
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("💳 Попробовать снова", callback_data="pay_now"))
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            status_text = {
                'canceled': 'отменен',
                'failed': 'не прошел',
                'rejected': 'отклонен банком'
            }.get(payment.status, payment.status)

            reason = ""
            if hasattr(payment, 'cancellation_details') and payment.cancellation_details:
                reason = f"\nПричина: {payment.cancellation_details.reason}"

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"❌ <b>Платеж {status_text}</b>\n\n"
                     f"Платеж не был завершен успешно.{reason}\n\n"
                     f"Пожалуйста, попробуйте снова или обратитесь в поддержку.",
                parse_mode='HTML',
                reply_markup=markup
            )

    except IndexError:
        logger.error(f"❌ Ошибка парсинга payment_id из callback: {call.data}")
        answer_callback_safe(bot, call.id, "❌ Ошибка в данных платежа")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в check_payment_callback: {e}")
        logger.error(traceback.format_exc())

        # Показываем общее сообщение об ошибке
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⚠️ <b>Произошла ошибка</b>\n\n"
                     "При обработке платежа произошла непредвиденная ошибка.\n"
                     "Пожалуйста, обратитесь в поддержку.",
                parse_mode='HTML',
                reply_markup=markup
            )
        except:
            pass
        finally:
            answer_callback_safe(bot, call.id, "❌ Произошла ошибка")





# ============================================================================
# МАССОВАЯ РАССЫЛКА СООБЩЕНИЙ ВСЕМ ПОЛЬЗОВАТЕЛЯМ
# ============================================================================

# Словарь для хранения состояний пользователей при массовой рассылке
user_data_manager.broadcast_states = {}

@bot.message_handler(func=lambda message:
    message.chat.id in user_data_manager.extend_states and
    user_data_manager.extend_states[message.chat.id]['state'] == 'waiting_for_user_id')
def handle_extend_user_id(message):
    """Обработка ввода ID пользователя для продления"""
    chat_id = message.chat.id
    user_state = user_data_manager.extend_states[chat_id]
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return
    try:
        user_id = int(message.text.strip())
        user_state['user_id'] = user_id

        # Проверяем существование пользователя
        user_info = db.get_user(user_id)
        if not user_info:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Ввести другой ID", callback_data="extend_user_menu"),
                       types.InlineKeyboardButton("↩️ Назад", callback_data="admin_extend_sub"))

            bot.send_message(
                chat_id,
                f"❌ <b>Пользователь не найден</b>\n\n"
                f"Пользователь с ID {user_id} не найден в базе данных.",
                parse_mode='HTML',
                reply_markup=markup
            )
            del user_data_manager.extend_states[chat_id]
            return

        # Показываем информацию о пользователе
        username = user_info.get('username', 'нет username')
        first_name = user_info.get('first_name', 'неизвестно')
        has_sub = "✅" if db.check_subscription(user_id) else "❌"
        end_date = user_info.get('subscription_end_date', 'нет подписки')

        markup = types.InlineKeyboardMarkup(row_width=3)

        # Часы
        markup.row(types.InlineKeyboardButton("🕐 +1 час", callback_data=f"extend_user_{user_id}_hours_1"),
                   types.InlineKeyboardButton("🕑 +3 часа", callback_data=f"extend_user_{user_id}_hours_3"),
                   types.InlineKeyboardButton("🕒 +6 часов", callback_data=f"extend_user_{user_id}_hours_6"))
        markup.row(types.InlineKeyboardButton("🕓 +12 часов", callback_data=f"extend_user_{user_id}_hours_12"),
                   types.InlineKeyboardButton("🕔 +24 часа", callback_data=f"extend_user_{user_id}_hours_24"))

        # Дни
        markup.row(types.InlineKeyboardButton("📅 +1 день", callback_data=f"extend_user_{user_id}_days_1"),
                   types.InlineKeyboardButton("📅 +3 дня", callback_data=f"extend_user_{user_id}_days_3"),
                   types.InlineKeyboardButton("📅 +7 дней", callback_data=f"extend_user_{user_id}_days_7"))
        markup.row(types.InlineKeyboardButton("📅 +14 дней", callback_data=f"extend_user_{user_id}_days_14"),
                   types.InlineKeyboardButton("📅 +30 дней", callback_data=f"extend_user_{user_id}_days_30"),
                   types.InlineKeyboardButton("📅 +60 дней", callback_data=f"extend_user_{user_id}_days_60"))

        markup.row(types.InlineKeyboardButton("↩️ Назад", callback_data="admin_extend_sub"))

        user_info_text = f"👤 <b>Информация о пользователе</b>\n\n"
        user_info_text += f"🆔 ID: {user_id}\n"
        user_info_text += f"👤 Имя: {first_name}\n"
        user_info_text += f"📱 Username: @{username}\n"
        user_info_text += f"💳 Подписка: {has_sub}\n"
        if end_date != 'нет подписки':
            user_info_text += f"📅 Действует до: {end_date}\n"
        user_info_text += f"\nВыберите срок продления:"

        bot.send_message(
            chat_id,
            user_info_text,
            parse_mode='HTML',
            reply_markup=markup
        )

        del user_data_manager.extend_states[chat_id]

    except ValueError:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Попробовать снова", callback_data="extend_user_menu"),
                   types.InlineKeyboardButton("↩️ Назад", callback_data="admin_extend_sub"))

        bot.send_message(
            chat_id,
            "❌ <b>Неверный формат ID</b>\n\n"
            "ID пользователя должен быть числом. Попробуйте снова.",
            parse_mode='HTML',
            reply_markup=markup
        )
        del user_data_manager.extend_states[chat_id]
    except Exception as e:
        logger.error(f"Ошибка в handle_extend_user_id: {e}")
        bot.send_message(chat_id, f"❌ Ошибка: {e}")
        if chat_id in user_data_manager.extend_states:
            del user_data_manager.extend_states[chat_id]

def handle_extend_user_callback(call):
    """Обработка выбора срока продления для конкретного пользователя"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Парсим данные из callback (формат: extend_user_[user_id]_[тип]_[значение])
        parts = call.data.split('_')

        if len(parts) < 5:
            answer_callback_safe(bot, call.id, "❌ Неверный формат команды")
            return

        user_id = int(parts[2])  # ID пользователя
        time_type = parts[3]  # hours или days
        value = int(parts[4])  # значение

        # Устанавливаем часы и дни
        hours = value if time_type == 'hours' else 0
        days = value if time_type == 'days' else 0

        # Получаем информацию о пользователе
        user_info = db.get_user(user_id)
        if not user_info:
            answer_callback_safe(bot, call.id, "❌ Пользователь не найден")
            return

        username = user_info.get('username', 'нет username')
        first_name = user_info.get('first_name', 'неизвестно')

        # Создаем подтверждающее сообщение
        time_text = ""
        if hours > 0:
            time_text = f"{hours} час(ов)"
        elif days > 0:
            time_text = f"{days} день(ей)"

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(f"✅ Да, продлить на {time_text}",
                                       callback_data=f"confirm_extend_user_{user_id}_{hours}_{days}"),
            types.InlineKeyboardButton("❌ Нет, отмена", callback_data="admin_extend_sub")
        )

        confirmation_text = f"⚠️ <b>Подтверждение продления</b>\n\n"
        confirmation_text += f"👤 Пользователь: {first_name} (@{username})\n"
        confirmation_text += f"🆔 ID: {user_id}\n"
        confirmation_text += f"⏱️ Срок: {time_text}\n\n"
        confirmation_text += f"Вы уверены, что хотите продлить подписку этому пользователю?"

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=confirmation_text,
            parse_mode='HTML',
            reply_markup=markup
        )
        answer_callback_safe(bot, call.id)

    except Exception as e:
        logger.error(f"Ошибка в handle_extend_user_callback: {e}")
        answer_callback_safe(bot, call.id, "❌ Ошибка обработки запроса")

@bot.message_handler(commands=['send_all_users'])
def handle_send_all_users(call):
    """Запуск массовой рассылки сообщений всем пользователям через callback"""
    chat_id = call.message.chat.id  # Исправлено: получаем chat_id из call.message.chat
    message_id = call.message.message_id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        answer_callback_safe(bot, call.id, "❌ У вас нет прав администратора для этой команды.")
        return

    # Сохраняем состояние пользователя
    user_data_manager.broadcast_states[chat_id] = {
        'state': 'waiting_for_message',
        'message': None,
        'confirmed': False
    }

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="📢 <b>МАССОВАЯ РАССЫЛКА</b>\n\n"
             "Отправьте сообщение, которое хотите разослать всем пользователям бота.\n"
             "Можно использовать HTML-разметку.\n\n"
             "<i>Или нажмите кнопку Отмена для выхода</i>",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)

@bot.message_handler(func=lambda message: message.chat.id in user_data_manager.broadcast_states and
                                          user_data_manager.broadcast_states[message.chat.id]['state'] == 'waiting_for_message')
def handle_broadcast_message(message):
    """Обработка сообщения для рассылки"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return
    # Проверяем существование состояния
    if chat_id not in user_data_manager.broadcast_states:
        return

    user_state = user_data_manager.broadcast_states[chat_id]

    # БЕЗОПАСНОЕ извлечение текста
    message_text = None
    if message.text:
        message_text = message.text
    elif message.caption:
        message_text = message.caption
    elif message.content_type == 'text':
        message_text = message.text
    else:
        # Для медиафайлов без подписи
        message_text = "📎 [Медиафайл без подписи]"

    # Сохраняем сообщение
    user_state['state'] = 'waiting_for_confirmation'
    user_state['message'] = message_text
    user_state['message_type'] = message.content_type
    user_state['message_id'] = message.message_id
    user_state['timestamp'] = time.time()  # Добавляем timestamp для очистки

    # Если есть фото/документ/другие медиафайлы
    if message.photo:
        user_state['photo'] = message.photo[-1].file_id
    if message.document:
        user_state['document'] = message.document.file_id
    if message.video:
        user_state['video'] = message.video.file_id
    if message.audio:
        user_state['audio'] = message.audio.file_id

    # Получаем информацию о пользователях
    all_users = db.get_all_users()
    active_users = [u for u in all_users if db.check_subscription(u['telegram_id'])]
    total_users = len(all_users)

    # Предпросмотр сообщения
    preview_text = "📢 <b>ПРЕДПРОСМОТР РАССЫЛКИ</b>\n\n"
    preview_text += f"📝 <b>Сообщение:</b>\n{user_state['message'][:200]}"
    if len(user_state['message']) > 200:
        preview_text += "..."

    preview_text += f"\n\n📊 <b>Статистика:</b>\n"
    preview_text += f"👥 Всего пользователей: {total_users}\n"
    preview_text += f"✅ Активных подписок: {len(active_users)}\n\n"
    preview_text += "⚠️ <b>Внимание:</b> Это сообщение будет отправлено ВСЕМ пользователям бота."

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("✅ Да, отправить всем", callback_data="confirm_broadcast"),
        types.InlineKeyboardButton("✏️ Редактировать", callback_data="edit_broadcast")
    )
    markup.add(
        types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast"),
        types.InlineKeyboardButton("📊 Только активным", callback_data="broadcast_active_only")
    )

    # Отправляем предпросмотр
    try:
        # Если есть фото
        if 'photo' in user_state:
            bot.send_photo(
                chat_id,
                photo=user_state['photo'],
                caption=preview_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        elif 'document' in user_state:
            bot.send_document(
                chat_id,
                document=user_state['document'],
                caption=preview_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        elif 'video' in user_state:
            bot.send_video(
                chat_id,
                video=user_state['video'],
                caption=preview_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        elif 'audio' in user_state:
            bot.send_audio(
                chat_id,
                audio=user_state['audio'],
                caption=preview_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        else:
            bot.send_message(
                chat_id,
                preview_text,
                parse_mode='HTML',
                reply_markup=markup
            )
    except Exception as e:
        bot.send_message(
            chat_id,
            f"❌ Ошибка при создании предпросмотра: {e}\n\nПопробуйте отправить сообщение еще раз.",
            parse_mode='HTML'
        )
        user_data_manager.broadcast_states[chat_id]['state'] = 'waiting_for_message'

#@bot.callback_query_handler(func=lambda call: call.data in ['confirm_broadcast', 'edit_broadcast',
#                                                           'cancel_broadcast', 'broadcast_active_only'])
def handle_broadcast_callback(call):
    """Обработка callback для массовой рассылки"""
    chat_id = call.message.chat.id

    if chat_id not in user_data_manager.broadcast_states:
        answer_callback_safe(bot, call.id, "❌ Сессия рассылки устарела")
        return

    user_state = user_data_manager.broadcast_states[chat_id]

    if call.data == 'confirm_broadcast':
        # Подтверждение отправки всем пользователям
        answer_callback_safe(bot, call.id, "🚀 Начинаю рассылку...")
        send_broadcast_to_all(chat_id, user_state, call.message.message_id, active_only=False)

    elif call.data == 'broadcast_active_only':
        # Отправка только активным пользователям
        answer_callback_safe(bot, call.id, "🚀 Начинаю рассылку активным пользователям...")
        send_broadcast_to_all(chat_id, user_state, call.message.message_id, active_only=True)

    elif call.data == 'edit_broadcast':
        # Редактирование сообщения
        answer_callback_safe(bot, call.id, "✏️ Отправьте новое сообщение")
        user_data_manager.broadcast_states[chat_id]['state'] = 'waiting_for_message'

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="📢 <b>МАССОВАЯ РАССЫЛКА</b>\n\n"
                 "Отправьте новое сообщение для рассылки.\n"
                 "Можно использовать HTML-разметку.\n\n"
                 "<i>Или нажмите кнопку Отмена для выхода</i>",
            parse_mode='HTML',
            reply_markup=markup
        )

    elif call.data == 'cancel_broadcast':
        # Отмена рассылки
        answer_callback_safe(bot, call.id, "❌ Рассылка отменена")
        if chat_id in user_data_manager.broadcast_states:
            del user_data_manager.broadcast_states[chat_id]

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="❌ <b>Рассылка отменена</b>",
            parse_mode='HTML'
        )

def send_broadcast_to_all(admin_chat_id, broadcast_data, message_id, active_only=False):
    """Отправка рассылки всем пользователям"""
    try:
        # Получаем всех пользователей
        all_users = db.get_all_users()

        if active_only:
            users_to_send = [u for u in all_users if db.check_subscription(u['telegram_id'])]
            filter_text = "только активным пользователям"
        else:
            users_to_send = all_users
            filter_text = "всем пользователям"

        total_users = len(users_to_send)
        success_count = 0
        failed_count = 0
        failed_users = []

        # Отправляем статус
        status_message = bot.send_message(
            admin_chat_id,
            f"📤 <b>Начинаю рассылку {filter_text}</b>\n\n"
            f"👥 Всего получателей: {total_users}\n"
            f"✅ Успешно отправлено: 0/{total_users}\n"
            f"❌ Ошибок: 0\n"
            f"⏳ Ожидание: {total_users}",
            parse_mode='HTML'
        )

        # Отправляем сообщение каждому пользователю
        for i, user in enumerate(users_to_send, 1):
            try:
                user_id = user['telegram_id']

                # Пропускаем администратора, который отправляет рассылку
                if user_id == admin_chat_id:
                    success_count += 1
                    continue

                # Отправляем сообщение в зависимости от типа
                if 'photo' in broadcast_data:
                    bot.send_photo(
                        user_id,
                        photo=broadcast_data['photo'],
                        caption=broadcast_data['message'],
                        parse_mode='HTML'
                    )
                elif 'document' in broadcast_data:
                    bot.send_document(
                        user_id,
                        document=broadcast_data['document'],
                        caption=broadcast_data['message'],
                        parse_mode='HTML'
                    )
                elif 'video' in broadcast_data:
                    bot.send_video(
                        user_id,
                        video=broadcast_data['video'],
                        caption=broadcast_data['message'],
                        parse_mode='HTML'
                    )
                elif 'audio' in broadcast_data:
                    bot.send_audio(
                        user_id,
                        audio=broadcast_data['audio'],
                        caption=broadcast_data['message'],
                        parse_mode='HTML'
                    )
                else:
                    bot.send_message(
                        user_id,
                        broadcast_data['message'],
                        parse_mode='HTML'
                    )

                success_count += 1

                # Обновляем активность пользователя
                db.update_activity(user_id)

            except Exception as e:
                failed_count += 1
                failed_users.append(f"{user_id} ({user.get('username', 'нет username')})")
                logger.error(f"Ошибка при отправке рассылки пользователю {user_id}: {e}")

            # Обновляем статус каждые 10 сообщений или в конце
            if i % 10 == 0 or i == total_users:
                try:
                    bot.edit_message_text(
                        chat_id=admin_chat_id,
                        message_id=status_message.message_id,
                        text=f"📤 <b>Рассылка в процессе...</b>\n\n"
                             f"👥 Всего получателей: {total_users}\n"
                             f"✅ Успешно отправлено: {success_count}/{total_users}\n"
                             f"❌ Ошибок: {failed_count}\n"
                             f"⏳ Ожидание: {total_users - i}",
                        parse_mode='HTML'
                    )
                except:
                    pass

            # Небольшая задержка, чтобы не превысить лимиты Telegram
            time.sleep(0.1)

        # Логируем результат
        logger.info(
            f"Администратор {admin_chat_id} отправил рассылку. Успешно: {success_count}, Ошибок: {failed_count}")

        # Формируем итоговый отчет
        report_text = f"📊 <b>ИТОГ РАССЫЛКИ</b>\n\n"
        report_text += f"✅ <b>Успешно отправлено:</b> {success_count}/{total_users}\n"
        report_text += f"❌ <b>Ошибок:</b> {failed_count}\n"

        if active_only:
            report_text += f"🎯 <b>Фильтр:</b> Только активные пользователи\n"
        else:
            report_text += f"🎯 <b>Фильтр:</b> Все пользователи\n"

        if failed_count > 0 and len(failed_users) > 0:
            report_text += f"\n📝 <b>Список ошибок (первые 10):</b>\n"
            for failed in failed_users[:10]:
                report_text += f"• {failed}\n"

            if len(failed_users) > 10:
                report_text += f"... и еще {len(failed_users) - 10} пользователей\n"

        # Отправляем итоговый отчет
        bot.edit_message_text(
            chat_id=admin_chat_id,
            message_id=status_message.message_id,
            text=report_text,
            parse_mode='HTML'
        )

        # Очищаем состояние
        if admin_chat_id in user_data_manager.broadcast_states:
            del user_data_manager.broadcast_states[admin_chat_id]

        # Отправляем уведомление в лог-чат
        log_text = f"📢 Администратор {admin_chat_id} провел рассылку\n"
        log_text += f"✅ Успешно: {success_count}, ❌ Ошибок: {failed_count}"
        logger.info(log_text)

    except Exception as e:
        logger.error(f"Ошибка при массовой рассылке: {e}")
        bot.send_message(
            admin_chat_id,
            f"❌ <b>Критическая ошибка при рассылке:</b>\n{e}",
            parse_mode='HTML'
        )

        # Очищаем состояние
        if admin_chat_id in user_data_manager.broadcast_states:
            del user_data_manager.broadcast_states[admin_chat_id]


def check_answer_callback(call):
    """Обработчик проверки ответа с обновлением логики сессии"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        answer_callback_safe(bot, call.id, "❌ Требуется активная подписка!")
        return

    # Получаем данные пользователя
    user_data = user_data_manager.get_user_data(chat_id)
    if not user_data.get('current_question'):
        answer_callback_safe(bot, call.id, "⚠️ Нет активного вопроса!")
        return

    try:
        answer_number = int(call.data.split('_')[1])

        if answer_number not in user_data['numbered_answers']:
            answer_callback_safe(bot, call.id, "❌ Неверный номер ответа!")
            return

        selected_answer = user_data['numbered_answers'][answer_number]
        correct_answers = user_data['correct_answer']
        question_text = user_data['current_question']
        topic = user_data.get('current_question_topic', user_data.get('current_topic'))

        if not topic:
            answer_callback_safe(bot, call.id, "⚠️ Не определена тема вопроса!")
            return

        # Проверяем ответ
        is_correct = selected_answer in correct_answers

        # Отмечаем вопрос как отвеченный в сессии
        user_data_manager.mark_question_answered(chat_id, topic, question_text, is_correct)

        # Обновляем статистику в базе данных
        db.update_statistics(chat_id, is_correct)

        # Обновляем статистику сессии
        session_stats_data = user_data_manager.get_session_stats(chat_id)
        session_stats_data['session_total'] += 1
        if is_correct:
            session_stats_data['session_correct'] += 1

        # Получаем общую статистику
        total_stats = db.get_user_statistics(chat_id)

        # Получаем статистику текущей сессии
        session_total = session_stats_data['session_total']
        session_correct = session_stats_data['session_correct']
        session_percentage = (session_correct / session_total * 100) if session_total > 0 else 0

        # Создаем текст результата
        result_text = ""
        if is_correct:
            result_text += "✅ <b>Правильно!</b>\n\n"
        else:
            result_text += f"❌ <b>Неправильно!</b>\nВы выбрали: {selected_answer}\n\n"

        # Показываем правильный ответ
        if correct_answers:
            if len(correct_answers) == 1:
                result_text += f"📖 <b>Правильный ответ:</b> {correct_answers[0]}"
            else:
                result_text += "📖 <b>Правильные ответы:</b>\n"
                for i, ans in enumerate(correct_answers, 1):
                    result_text += f"{i}. {ans}\n"

        # Добавляем статистику
        result_text += f"\n\n📊 <b>Статистика сессии:</b>"
        result_text += f"\n✅ Правильных: {session_correct}/{session_total} ({session_percentage:.1f}%)"

        # Добавляем общую статистику
        if total_stats:
            total_total = total_stats['total_answers']
            total_correct = total_stats['correct_answers']
            total_percentage = (total_correct / total_total * 100) if total_total > 0 else 0

            result_text += f"\n\n📈 <b>Общая статистика:</b>"
            result_text += f"\n✅ Правильных: {total_correct}/{total_total} ({total_percentage:.1f}%)"

        # Создаем кнопки
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("➡️ Следующий вопрос", callback_data="get_question"),
            types.InlineKeyboardButton("📊 Статистика", callback_data="show_stats")
        )
        markup.add(
            types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic"),
            types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
        )

        # Обновляем сообщение
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=result_text,
            parse_mode='HTML',
            reply_markup=markup
        )

        # Отправляем уведомление
        if is_correct:
            answer_callback_safe(bot, call.id, "✅ Правильно!")
        else:
            answer_callback_safe(bot, call.id, "❌ Неправильно!")

    except (ValueError, IndexError) as e:
        logger.error(f"❌ Ошибка обработки ответа: {e}")
        answer_callback_safe(bot, call.id, "❌ Ошибка при обработке ответа.")

def logs_last_100_callback(call):
    """Последние 100 строк логов"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Читаем логи из файла
        log_file = 'data/bot.log'
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            last_lines = lines[-100:] if len(lines) > 100 else lines
            logs_text = ''.join(last_lines)

            if len(logs_text) > 4000:
                logs_text = logs_text[-4000:]  # Ограничиваем длину

            if not logs_text.strip():
                logs_text = "⚠️ Логи пустые"
        else:
            logs_text = "⚠️ Файл логов не найден"

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("↩️ Назад в логи", callback_data="admin_logs"))
        markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"📄 <b>Последние 100 строк логов:</b>\n\n<code>{logs_text}</code>",
            parse_mode='HTML',
            reply_markup=markup
        )
    except Exception as e:
        answer_callback_safe(bot, call.id, f"❌ Ошибка: {e}")

def logs_stats_callback(call):
    """Статистика логов"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        log_file = 'data/bot.log'
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()

            lines = content.split('\n')
            file_size = os.path.getsize(log_file) / 1024  # Размер в КБ

            logs_text = f"""
📊 <b>Статистика логов</b>

📁 Файл: {log_file}
📏 Размер: {file_size:.2f} КБ
📝 Строк: {len(lines)}
⏰ Последнее изменение: {datetime.fromtimestamp(os.path.getmtime(log_file)).strftime('%d.%m.%Y %H:%M:%S')}

🔍 <b>Анализ:</b>
• Ошибки (❌): {content.count('❌')}
• Предупреждения (⚠️): {content.count('⚠️')}
• Успехи (✅): {content.count('✅')}
• Callback-и (🔄): {content.count('🔄')}
"""
        else:
            logs_text = "⚠️ Файл логов не найден"

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("↩️ Назад в логи", callback_data="admin_logs"))
        markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=logs_text,
            parse_mode='HTML',
            reply_markup=markup
        )
    except Exception as e:
        answer_callback_safe(bot, call.id, f"❌ Ошибка: {e}")


def logs_get_file_callback(call):
    """Получить файл логов"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        log_file = 'data/bot.log'
        if os.path.exists(log_file):
            with open(log_file, 'rb') as f:
                bot.send_document(chat_id, f, caption="📁 Файл логов")
        else:
            answer_callback_safe(bot, call.id, "❌ Файл логов не найден")
    except Exception as e:
        answer_callback_safe(bot, call.id, f"❌ Ошибка: {e}")

def logs_clear_callback(call):
    """Очистить логи"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🗑️ Да, очистить", callback_data="logs_clear_confirm"),
        types.InlineKeyboardButton("❌ Нет, отмена", callback_data="admin_logs")
    )

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="⚠️ <b>ВНИМАНИЕ!</b>\n\nВы действительно хотите очистить все логи?\nЭто действие необратимо.",
        parse_mode='HTML',
        reply_markup=markup
    )

def logs_clear_confirm_callback(call):
    """Подтверждение очистки логов"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        log_file = 'data/bot.log'
        if os.path.exists(log_file):
            # Создаем резервную копию
            backup_file = f'bot.log.backup_{datetime.now(pytz.UTC).strftime("%Y%m%d_%H%M%S")}'
            shutil.copy2(log_file, backup_file)

            # Очищаем файл
            open(log_file, 'w').close()

            answer_callback_safe(bot, call.id, "✅ Логи очищены, создана резервная копия")

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("↩️ Назад в логи", callback_data="admin_logs"))
            markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="✅ <b>Логи успешно очищены!</b>\n\nСоздана резервная копия: " + backup_file,
                parse_mode='HTML',
                reply_markup=markup
            )
        else:
            answer_callback_safe(bot, call.id, "❌ Файл логов не найден")
    except Exception as e:
        answer_callback_safe(bot, call.id, f"❌ Ошибка: {e}")


def admin_db_callback(call):
    """Скачать базу данных"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        db_file = 'data/users.db'
        if os.path.exists(db_file):
            # Создаем временную копию для безопасности
            temp_file = f'users_backup_{datetime.now(pytz.UTC).strftime("%Y%m%d_%H%M%S")}.db'
            shutil.copy2(db_file, temp_file)

            with open(temp_file, 'rb') as f:
                bot.send_document(chat_id, f, caption="📁 Резервная копия базы данных")

            # Удаляем временный файл
            os.remove(temp_file)

            answer_callback_safe(bot, call.id, "✅ Файл базы данных отправлен")
        else:
            answer_callback_safe(bot, call.id, "❌ Файл базы данных не найден")
    except Exception as e:
        answer_callback_safe(bot, call.id, f"❌ Ошибка: {e}")

def restart_confirm_callback(call):
    """Подтверждение перезагрузки бота"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("↩️ Назад в админку", callback_data="back_to_admin"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔄 <b>Перезагрузка...</b>\n\nБот будет перезапущен.\nПожалуйста, подождите...",
            parse_mode='HTML',
            reply_markup=markup
        )

        # Здесь должен быть код перезагрузки бота
        # В реальном проекте это может быть перезапуск процесса
        answer_callback_safe(bot, call.id, "✅ Команда на перезагрузку отправлена")

    except Exception as e:
        answer_callback_safe(bot, call.id, f"❌ Ошибка: {e}")

def payment_instructions_callback(call):
    """Инструкция по оплате"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Перейти к оплате", url="https://your_payment_link.com"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="📋 <b>Инструкция по оплате</b>\n\n1. Перейдите по ссылке оплаты\n2. Выберите способ оплаты\n3. Оплатите выбранный тариф\n4. Отправьте чек в поддержку",
        parse_mode='HTML',
        reply_markup=markup
    )
    answer_callback_safe(bot, call.id)


def handle_topic_selection(call):
    """ЕДИНСТВЕННЫЙ обработчик выбора темы"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        answer_callback_safe(bot, call.id, "❌ Требуется активная подписка!")
        return

    try:
        # Поддерживаем оба формата: "t_0" и "topic_0"
        parts = call.data.split('_')
        if len(parts) < 2:
            answer_callback_safe(bot, call.id, "❌ Неверный формат выбора темы")
            return

        topic_num = int(parts[1])

        # Валидация
        if topic_num < 0 or topic_num >= len(topics_list):
            logger.error(f"❌ Неверный номер темы: {topic_num}, всего тем: {len(topics_list)}")
            answer_callback_safe(bot, call.id, "❌ Неверный номер темы")
            return

        selected_topic = topics_list[topic_num]
        topic_display = selected_topic[:30] + "..." if len(selected_topic) > 30 else selected_topic

        # Очищаем сессию для новой темы
        user_data_manager.clear_topic_session(chat_id, selected_topic)

        # Сохраняем выбранную тему
        user_data_manager.update_user_data(
            chat_id,
            current_topic=selected_topic,
            current_question=None,
            correct_answer=None,
            numbered_answers={},
            answers_list=[],
            current_question_topic=selected_topic
        )

        # Получаем статистику
        if selected_topic == "🎲 Все темы (рандом)":
            topic_questions_count = sum(len(q) for q in questions_by_topic.values())
        else:
            topic_questions_count = len(questions_by_topic.get(selected_topic, []))

        user_data = user_data_manager.get_user_data(chat_id)
        answered_questions = user_data.get('answered_questions', {}).get(selected_topic, [])
        answered_count = len(answered_questions)
        remaining_count = topic_questions_count - answered_count

        topic_info = f"""
✅ <b>Выбрана тема:</b> {selected_topic}
📊 <b>Вопросов в теме:</b> {topic_questions_count}

📈 <b>Прогресс:</b>
• Отвечено: {answered_count}/{topic_questions_count}
• Осталось: {remaining_count}

👇 Выберите действие:
        """

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🎲 Начать/продолжить", callback_data="get_question"),
            types.InlineKeyboardButton("🔄 Начать заново", callback_data=f"r_{topic_num}")
        )
        markup.add(
            types.InlineKeyboardButton("📊 Статистика", callback_data="show_stats"),
            types.InlineKeyboardButton("🔄 Выбрать другую тему", callback_data="change_topic")
        )
        markup.add(
            types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
        )

        try:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=topic_info,
                parse_mode='HTML',
                reply_markup=markup
            )
            answer_callback_safe(bot, call.id, f"Выбрана тема: {topic_display}")
        except Exception as e:
            logger.error(f"Ошибка при редактировании: {e}")
            bot.send_message(chat_id, topic_info, parse_mode='HTML', reply_markup=markup)
            answer_callback_safe(bot, call.id, f"Выбрана тема: {topic_display}")

    except ValueError as e:
        logger.error(f"❌ Ошибка парсинга номера темы: {call.data} - {e}")
        answer_callback_safe(bot, call.id, "❌ Ошибка при выборе темы")
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка в handle_topic_selection: {e}")
        logger.error(traceback.format_exc())
        answer_callback_safe(bot, call.id, "❌ Ошибка выбора темы")

def handle_topic_restart(call):
    """Обработчик перезапуска темы из упрощенного формата (r_0, r_1)"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Извлекаем номер темы из callback_data (формат: r_0, r_1 и т.д.)
        topic_num = int(call.data.split('_')[1])

        if 0 <= topic_num < len(topics_list):
            selected_topic = topics_list[topic_num]
            topic_display = selected_topic[:30] + "..." if len(selected_topic) > 30 else selected_topic

            # Очищаем сессию для темы
            user_data_manager.clear_topic_session(chat_id, selected_topic)

            # Обнуляем статистику сессии
            session_stats = user_data_manager.get_session_stats(chat_id)
            session_stats['session_total'] = 0
            session_stats['session_correct'] = 0

            user_data_manager.update_user_data(
                chat_id,
                current_topic=selected_topic,
                current_question=None,
                correct_answer=None,
                numbered_answers={},
                answers_list=[],
                current_question_topic=selected_topic
            )

            # Получаем обновленную информацию о теме
            if selected_topic == "🎲 Все темы (рандом)":
                topic_questions_count = sum(len(q) for q in questions_by_topic.values())
            elif selected_topic in questions_by_topic:
                topic_questions_count = len(questions_by_topic[selected_topic])
            else:
                topic_questions_count = 0

            # Формируем сообщение
            restart_text = f"""
🔄 <b>Сессия для темы '{topic_display}' перезапущена!</b>

📊 <b>Вопросов в теме:</b> {topic_questions_count}
📈 <b>Прогресс:</b> 0/{topic_questions_count} (0.0%)

👇 Выберите действие:
            """

            # Создаем кнопки с безопасными callback_data
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("🎲 Начать обучение", callback_data="get_question"),
                types.InlineKeyboardButton("📊 Статистика", callback_data="show_stats")
            )
            markup.add(
                types.InlineKeyboardButton("↩️ Назад к выбору темы", callback_data="change_topic"),
                types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            )

            try:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=restart_text,
                    parse_mode='HTML',
                    reply_markup=markup
                )
                answer_callback_safe(bot, call.id, f"Сессия для '{topic_display}' перезапущена")
            except Exception as e:
                logger.error(f"Ошибка при редактировании сообщения: {e}")
                bot.send_message(
                    chat_id,
                    restart_text,
                    parse_mode='HTML',
                    reply_markup=markup
                )
                answer_callback_safe(bot, call.id, f"Сессия для '{topic_display}' перезапущена")
        else:
            logger.error(f"❌ Неверный номер темы для перезапуска: {topic_num}")
            answer_callback_safe(bot, call.id, "❌ Неверный номер темы")

    except ValueError as e:
        logger.error(f"❌ Ошибка парсинга номера темы для перезапуска: {call.data} - {e}")
        answer_callback_safe(bot, call.id, "❌ Ошибка перезапуска темы")
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка в handle_topic_restart: {e}")
        logger.error(traceback.format_exc())
        answer_callback_safe(bot, call.id, "❌ Ошибка перезапуска")

@bot.message_handler(commands=['start'])
def handle_start(message):
    """Обработчик команды /start"""
    chat_id = message.chat.id
    user = message.from_user

    logger.info(f"📨 Получен /start от {user.first_name} (ID: {chat_id})")

    # Регистрируем пользователя
    db.add_user(
        telegram_id=chat_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )

    # Проверяем доступ
    if not check_user_access(chat_id):
        return

    # Инициализируем данные пользователя через менеджер
    user_data_manager.update_user_data(
        chat_id,
        current_topic=None,
        current_question=None,
        correct_answer=None,
        numbered_answers={},
        answers_list=[]
    )

    # Инициализируем статистику сессии через менеджер
    user_data_manager.get_session_stats(chat_id)

    # Отправляем приветственное сообщение
    welcome_text = f"""
👋 Привет, {user.first_name}!

Я бот для подготовки к тестам. Помогу тебе подготовиться к экзаменам и улучшить знания.

📊 <b>Загружено тем:</b> {len(topics_list) - 1 if topics_list else 0}

👇 Выберите действие:
    """

    bot.send_message(
        chat_id,
        welcome_text,
        parse_mode='HTML',
        reply_markup=create_main_menu()
    )

# Использование в обработчиках:
@bot.message_handler(func=lambda message: True)
def rate_limit_wrapper(message):
    """Обертка для rate limiting"""
    user_id = message.chat.id

    if not rate_limiter.check(user_id):
        bot.send_message(
            user_id,
            "⚠️ <b>Слишком много запросов!</b>\n\n"
            "Пожалуйста, подождите 1 минуту перед следующим запросом.",
            parse_mode='HTML'
        )
        return


@bot.callback_query_handler(func=lambda call: True)
def universal_callback_handler(call):
    user_id = call.from_user.id

    # Rate limiting для callback
    if not rate_limiter.check_callback(user_id):
        try:
            bot.answer_callback_query(
                call.id,
                "⚠️ Слишком много запросов! Подождите минуту.",
                show_alert=True
            )
        except:
            pass
        return

    # 🔥🔥🔥 КРИТИЧЕСКИ ВАЖНО: ПРОВЕРЯЕМ ПОДПИСКУ ПРИ ЛЮБОМ ДЕЙСТВИИ 🔥🔥🔥
    # Пропускаем только callback'и, которые не требуют подписки
    exempt_callbacks = [
        "subscribe", "pay_now", "trial", "check_payment_",
        "subscribe_info", "subscription_terms", "help_menu",
        "info", "main_menu", "check_questions", "admin_"
    ]

    # Проверяем, нужно ли проверять подписку для этого callback
    should_check_subscription = True
    for exempt in exempt_callbacks:
        if call.data.startswith(exempt):
            should_check_subscription = False
            break

    if should_check_subscription:
        # Проверяем подписку (send_message=False, чтобы не отправлять дублирующее сообщение)
        if not check_user_access(user_id, send_message=False):
            try:
                # Отвечаем на callback с предупреждением
                bot.answer_callback_query(
                    call.id,
                    "❌ Требуется активная подписка!",
                    show_alert=True
                )

                # Отправляем сообщение о необходимости подписки
                user = db.get_user(user_id)
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))
                markup.add(types.InlineKeyboardButton("🎁 Получить пробный доступ", callback_data="trial"))
                markup.row(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

                bot.send_message(
                    user_id,
                    "🚫 <b>Доступ ограничен!</b>\n\nДля использования бота необходима активная подписка.",
                    parse_mode='HTML',
                    reply_markup=markup
                )
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить сообщение о подписке: {e}")
            return
    try:
        # Логируем полученный callback для отладки
        logger.info(
            f"📨 Callback получен: {call.data} от пользователя {call.from_user.id} ({call.from_user.username or 'нет username'})")

        # Безопасный ответ на callback
        def safe_answer(text=None, show_alert=False):
            try:
                if text:
                    bot.answer_callback_query(call.id, text=text, show_alert=show_alert)
                else:
                    bot.answer_callback_query(call.id)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось ответить на callback {call.id}: {e}")

        # Маршрутизация по типам callback
        data = call.data

        # 1. Главное меню и основные действия
        if data == "main_menu":
            main_menu_callback(call)
            safe_answer()
        elif data == "show_stats":
            show_stats_callback(call)
            safe_answer()
        elif data == "change_topic":
            change_topic_callback(call)
            safe_answer()
        elif data == "get_question":
            get_question_callback(call)
            safe_answer("🔄 Загружаю вопрос...")
        elif data == "random_question":
            random_question_callback(call)
            safe_answer("🎲 Загружаю случайный вопрос...")
        elif data == "subscribe_info":
            subscribe_info_callback(call)
            safe_answer()
        elif data == "subscribe":
            subscribe_callback(call)
            safe_answer()
        elif data == "pay_now":
            pay_now_callback(call)
            safe_answer("🔄 Создаю платеж...")
        elif data == "trial":
            trial_callback(call)
            safe_answer()
        elif data == "info":
            info_callback(call)
            safe_answer()
        elif data == "help_menu":
            help_menu_callback(call)
            safe_answer()
        elif data == "top_players":
            top_players_callback(call)
            safe_answer()
        elif data == "subscription_terms":
            subscription_terms_callback(call)
            safe_answer()
        elif data == "check_questions":
            check_questions_callback(call)
            safe_answer()
        elif data in ["reset_my_stats", "confirm_reset_stats", "reset_topic_progress", "confirm_reset_topic"]:
            reset_my_stats_callback(call)

        # 2. Выбор темы (упрощенный формат - ВАЖНО!)
        elif data.startswith("t_"):  # Например: t_0, t_1
            handle_topic_selection(call)
            # Не отвечаем здесь, т.к. handle_topic_selection сам отвечает

        # 3. Ответы на вопросы
        elif data.startswith("answer_"):
            check_answer_callback(call)

        # 4. Платежи
        elif data.startswith("check_payment_"):
            check_payment_callback(call)
            safe_answer("🔄 Проверяем статус оплаты...")

        # 5. Административные функции
        elif data.startswith("admin_"):
            handle_admin_callback(call)
            safe_answer()
        elif data.startswith("extend_all_hours_") or data.startswith("extend_all_days_") or \
                data.startswith("extend_user_") or data.startswith("confirm_extend_"):
            # Проверяем, является ли пользователь администратором
            user = db.get_user(call.from_user.id)
            if user and user.get('is_admin'):
                if data.startswith("extend_all_"):
                    handle_extend_all_callback(call)
                elif data.startswith("extend_user_"):
                    handle_extend_user_callback(call)
                elif data.startswith("confirm_extend_"):
                    handle_confirm_extend_callback(call)
                safe_answer()
            else:
                safe_answer("❌ Нет прав администратора!", show_alert=True)
        # 7. Перезапуск темы (упрощенный формат)
        elif data.startswith("r_"):
            handle_topic_restart(call)
            # Не отвечаем здесь, т.к. handle_topic_restart сам отвечает

        # 8. Массовая рассылка
        elif data in ["confirm_broadcast", "edit_broadcast", "cancel_broadcast", "broadcast_active_only"]:
            handle_broadcast_callback(call)

        # 9. Логи (админка)
        elif data in ["logs_last_100", "logs_stats", "logs_get_file", "logs_clear",
                      "logs_clear_confirm", "admin_db", "admin_restart", "restart_confirm",
                      "back_to_admin", "admin_stats", "admin_users", "admin_grant_sub",
                      "admin_grant_admin", "admin_broadcast", "admin_logs", "admin_extend_sub",
                      "extend_user_menu", "extend_all_menu"]:
            handle_admin_callback(call)
            safe_answer()

        # 10. Кнопка "Назад"
        elif data == "back" or data.startswith("back_to_"):
            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="↩️ Возвращаюсь назад...",
                    parse_mode='HTML'
                )
                # Через секунду обновляем
                time.sleep(0.5)
                if data == "back_to_admin":
                    back_to_admin_callback(call)
                else:
                    main_menu_callback(call)
                safe_answer()
            except:
                # Если не удалось отредактировать, отправляем новое сообщение
                bot.send_message(call.message.chat.id, "↩️ Возвращаюсь назад...")
                main_menu_callback(call)
            safe_answer()

        # 11. Неизвестный callback
        else:
            logger.warning(f"⚠️ Неизвестный callback: {data}")
            safe_answer("⚠️ Неизвестная команда", show_alert=False)

    except telebot.apihelper.ApiTelegramException as e:
        # Ошибки Telegram API
        error_msg = str(e)
        if "BUTTON_DATA_INVALID" in error_msg:
            logger.error(f"❌ НЕВЕРНЫЙ ФОРМАТ КНОПКИ: {call.data}")
            safe_answer("❌ Ошибка формата кнопки")
        elif "query is too old" in error_msg or "query ID is invalid" in error_msg:
            # Игнорируем ошибки устаревших callback
            logger.warning(f"⚠️ Callback устарел: {call.data}")
        elif "message is not modified" in error_msg:
            # Игнорируем ошибку "сообщение не изменено"
            logger.warning(f"⚠️ Сообщение не было изменено: {call.data}")
        elif "message to edit not found" in error_msg:
            logger.error(f"❌ Сообщение для редактирования не найдено: {call.data}")
            # Пытаемся отправить новое сообщение
            try:
                bot.send_message(
                    call.message.chat.id,
                    "⚠️ Произошла ошибка. Попробуйте снова.",
                    reply_markup=create_main_menu()
                )
            except:
                pass
        else:
            logger.error(f"❌ Ошибка Telegram API в callback {call.data}: {e}")

    except Exception as e:
        # Все остальные ошибки
        logger.error(f"❌ Критическая ошибка в callback обработчике: {e}")
        logger.error(traceback.format_exc())

        # Пытаемся уведомить пользователя об ошибке
        try:
            safe_answer("❌ Произошла ошибка. Попробуйте снова.", show_alert=False)
        except:
            pass
# ============================================================================
# ЗАПУСК БОТА
# ============================================================================
def setup_scheduler():
    """Настройка планировщика задач"""
    global scheduler

    try:
        scheduler = BackgroundScheduler()

        # ПРОВЕРЯЕМ, ЧТО ПЛАНИРОВЩИК НЕ ЗАПУЩЕН
        if scheduler and scheduler.running:
            logger.info("⏰ Планировщик уже запущен, пропускаем...")
            return scheduler

        # Ежедневная проверка подписок - КАЖДЫЙ ЧАС для надежности
        scheduler.add_job(
            check_and_update_subscriptions,
            trigger=CronTrigger(minute=0),  # Каждый час в 0 минут
            id='hourly_subscription_check',
            name='Проверка подписок (каждый час)',
            replace_existing=True
        )

        # Ежедневная синхронизация платежей (в 1:00 ночи)
        scheduler.add_job(
            sync_paid_subscriptions_on_startup,
            trigger=CronTrigger(hour=1, minute=0, timezone=NOVOSIBIRSK_TZ),
            id='daily_payment_sync',
            name='Синхронизация платежей',
            replace_existing=True
        )

        # Периодическая очистка памяти (каждые 30 минут)
        scheduler.add_job(
            user_data_manager.cleanup_old_data,
            trigger='interval',
            minutes=30,
            id='memory_cleanup',
            name='Очистка памяти',
            replace_existing=True
        )

        # Логирование использования памяти (каждый час)
        scheduler.add_job(
            log_memory_usage,
            trigger='interval',
            hours=1,
            id='memory_log',
            name='Логирование памяти',
            replace_existing=True
        )

        # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ПРИ ЗАПУСКЕ
        scheduler.add_job(
            check_and_update_subscriptions,
            trigger='date',
            run_date=datetime.now(pytz.UTC) + timedelta(seconds=10),
            id='startup_subscription_check',
            name='Проверка подписок при запуске',
            replace_existing=True
        )

        # ЗАПУСКАЕМ ПЛАНИРОВЩИК
        if not scheduler.running:
            scheduler.start()
            logger.info("✅ Планировщик задач ЗАПУЩЕН")
        else:
            logger.info("ℹ️ Планировщик уже работает")

        # Выводим информацию о запущенных задачах
        jobs = scheduler.get_jobs()
        logger.info(f"📋 Загружено задач: {len(jobs)}")
        for job in jobs:
            next_run = job.next_run_time.astimezone(NOVOSIBIRSK_TZ).strftime(
                '%d.%m.%Y %H:%M') if job.next_run_time else "Не запланировано"
            logger.info(f"  - {job.name}: следующий запуск {next_run}")

        return scheduler

    except Exception as e:
        logger.error(f"❌ Ошибка при настройке планировщика: {e}")
        logger.error(traceback.format_exc())
        return None




def log_memory_usage():
    """Логирование использования памяти"""
    try:
        import psutil
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024

        user_data_memory = user_data_manager.get_memory_usage()

        logger.info(f"📊 Использование памяти: {memory_mb:.2f} MB (данные пользователей: {user_data_memory:.2f} MB)")

        # Дополнительно: логируем количество активных пользователей
        active_users = len(user_data_manager.user_data)
        logger.info(f"👥 Активных пользователей в памяти: {active_users}")

    except ImportError:
        logger.info("ℹ️ psutil не установлен, логирование памяти недоступно")
    except Exception as e:
        logger.error(f"Ошибка логирования памяти: {e}")


def check_and_update_subscriptions():
    """Проверка и обновление подписок - ВСЕ В UTC"""
    try:
        # ИСПОЛЬЗУЕМ UTC, А НЕ NOVOSIBIRSK_TZ!
        current_datetime = datetime.now(pytz.UTC)

        conn = db.get_connection()
        cursor = conn.cursor()

        # Находим истекшие подписки
        cursor.execute('''
        SELECT telegram_id, username, first_name, subscription_end_date 
        FROM users 
        WHERE subscription_paid = TRUE 
        AND subscription_end_date IS NOT NULL
        ''')

        expired_users = []
        users_to_update = []

        for row in cursor.fetchall():
            user_id, username, first_name, end_date_str = row
            if not end_date_str:
                continue

            try:
                # Парсим дату из БД (предполагаем UTC)
                try:
                    end_naive = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    end_naive = datetime.strptime(end_date_str, '%Y-%m-%d')
                    end_naive = end_naive.replace(hour=23, minute=59, second=59)

                # Делаем aware (UTC)
                end_aware = pytz.UTC.localize(end_naive)

                # Сравниваем aware datetime
                if end_aware < current_datetime:
                    expired_users.append({
                        'id': user_id,
                        'username': username,
                        'first_name': first_name,
                        'end_date': end_date_str
                    })
                    users_to_update.append(user_id)

            except (ValueError, TypeError) as e:
                logger.error(f"⚠️ Ошибка парсинга даты для пользователя {user_id}: {e}")
                continue

        # Обновляем истекшие подписки
        if users_to_update:
            placeholders = ','.join('?' * len(users_to_update))
            cursor.execute(f'''
            UPDATE users 
            SET subscription_paid = FALSE,
                subscription_start_date = NULL,
                subscription_end_date = NULL
            WHERE telegram_id IN ({placeholders})
            ''', users_to_update)

            conn.commit()
            logger.info(f"✅ Обновлено {len(users_to_update)} истекших подписок")

        conn.close()

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке подписок: {e}")
        logger.error(traceback.format_exc())


def shutdown_handler(signum=None, frame=None):
    """Обработчик завершения работы"""
    logger.info("⚠️ Получен сигнал завершения работы...")
    try:
        # Проверяем состояние планировщика более надежно
        if scheduler:
            try:
                # Проверяем, запущен ли планировщик
                if hasattr(scheduler, 'running') and scheduler.running:
                    logger.info("⏰ Останавливаю планировщик...")
                    scheduler.shutdown(wait=False)
                elif hasattr(scheduler, '_stopped'):
                    # Альтернативная проверка для разных версий APScheduler
                    if not scheduler._stopped:
                        logger.info("⏰ Останавливаю планировщик...")
                        scheduler.shutdown(wait=False)
                    else:
                        logger.info("ℹ️ Планировщик уже остановлен")
                else:
                    logger.info("ℹ️ Планировщик не запущен")
            except AttributeError:
                logger.info("ℹ️ Планировщик в неопределенном состоянии")
            except Exception as e:
                logger.info(f"⚠️ Ошибка при остановке планировщика: {e}")
        else:
            logger.info("ℹ️ Планировщик не инициализирован")
    except Exception as e:
        logger.info(f"⚠️ Неожиданная ошибка: {e}")


def setup_admin_from_env():
    """Назначение администратора через переменную окружения ADMIN_IDS"""
    try:
        # Получаем список ID администраторов из переменной окружения
        admin_ids_str = os.getenv('ADMIN_IDS', '')

        if not admin_ids_str:
            logger.info("⚠️ Переменная окружения ADMIN_IDS не установлена")
            return False

        # Парсим ID администраторов (могут быть разделены запятыми или пробелами)
        admin_ids = []
        for item in admin_ids_str.replace(',', ' ').split():
            try:
                admin_id = int(item.strip())
                admin_ids.append(admin_id)
            except ValueError:
                logger.info(f"⚠️ Некорректный ID администратора: {item}")

        if not admin_ids:
            logger.info("⚠️ Не удалось распарсить ID администраторов")
            return False

        logger.info(f"👑 Настройка администраторов из переменных окружения: {admin_ids}")

        # Подключаемся к базе данных
        db_path = 'data/users.db'
        if not os.path.exists(db_path):
            logger.info(f"❌ База данных не найдена: {db_path}")
            return False

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Обновляем статус администратора для указанных ID
        updated_count = 0
        for admin_id in admin_ids:
            try:
                # Сначала проверяем, существует ли пользователь
                cursor.execute('SELECT telegram_id FROM users WHERE telegram_id = ?', (admin_id,))
                user_exists = cursor.fetchone()

                if user_exists:
                    # Обновляем существующего пользователя
                    cursor.execute('''
                    UPDATE users 
                    SET is_admin = TRUE,
                        last_activity = CURRENT_TIMESTAMP
                    WHERE telegram_id = ?
                    ''', (admin_id,))
                    logger.info(f"✅ Пользователь {admin_id} назначен администратором")
                else:
                    # Создаем нового пользователя как администратора
                    cursor.execute('''
                    INSERT INTO users (telegram_id, is_admin, registration_date, last_activity)
                    VALUES (?, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ''', (admin_id,))
                    logger.info(f"✅ Создан новый пользователь {admin_id} с правами администратора")

                updated_count += 1

            except sqlite3.Error as e:
                logger.info(f"❌ Ошибка при назначении администратора {admin_id}: {e}")

        conn.commit()
        conn.close()

        logger.info(f"✅ Успешно настроено {updated_count} администраторов")
        return True

    except Exception as e:
        logger.info(f"❌ Ошибка при настройке администраторов: {e}")
        return False


# ============================================================================
# ФУНКЦИЯ ДЛЯ ОДНОРАЗОВОГО ВЫПОЛНЕНИЯ ПРИ ЗАПУСКЕ
# ============================================================================

def run_startup_tasks():
    """Задачи, выполняемые один раз при запуске бота"""
    check_database_health()
    # Обновление схемы БД (добавление новой колонки)
    logger.info("🔄 Обновление схемы базы данных...")
    db.upgrade_database()

    # Очистка старых платежей
    logger.info("🧹 Очистка старых платежей...")
    cleaned_count = cleanup_old_payments()
    if cleaned_count > 0:
        logger.info(f"✅ Очищено {cleaned_count} старых платежей")

    # Проверка согласованности данных
    logger.info("🔍 Проверка согласованности данных...")
    check_subscription_consistency()

    # Синхронизация оплаченных подписок (только за последние 3 дня)
    logger.info("💰 Синхронизация свежих оплаченных подписок...")
    sync_result = sync_paid_subscriptions_on_startup()
    if sync_result:
        logger.info(f"✅ Синхронизация завершена: "
                    f"{sync_result['activated']}/{sync_result['total']} подписок активировано "
                    f"(проверялись платежи за {sync_result['max_days']} дня)")

    # Назначение администраторов
    if setup_admin_from_env():
        logger.info("✅ Назначение администраторов выполнено успешно")
    else:
        logger.info("⚠️ Назначение администраторов не выполнено")

    if setup_bot_commands():
        logger.info("✅ Меню команд бота настроено")
    else:
        logger.info("⚠️ Не удалось настроить меню команд бота")


def safe_polling():
    """Безопасный запуск бота с восстановлением после сбоев"""
    polling_interval = 1
    timeout = 30
    max_retries = 10
    retry_count = 0

    # Очистка вебхука перед запуском
    try:
        bot.delete_webhook()
        logger.info("✅ Вебхук удален")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось удалить вебхук: {e}")

    while retry_count < max_retries:
        try:
            logger.info(f"🚀 Запуск бота, попытка #{retry_count + 1}")

            bot.infinity_polling(
                timeout=timeout,
                long_polling_timeout=30,
                logger_level=logging.INFO
            )

        except KeyboardInterrupt:
            logger.info("👋 Завершение работы по запросу пользователя")
            break
        except telebot.apihelper.ApiException as e:
            logger.error(f"❌ Ошибка Telegram API: {e}")
            retry_count += 1

            if "Conflict" in str(e):
                logger.error("⚠️ Конфликт: другой инстанс бота уже запущен")
                time.sleep(30)
            else:
                time.sleep(5)

        except ConnectionError as e:
            logger.error(f"❌ Ошибка соединения: {e}")
            retry_count += 1
            time.sleep(10)

        except Exception as e:
            logger.error(f"❌ Непредвиденная ошибка: {e}")
            logger.error(traceback.format_exc())
            retry_count += 1
            time.sleep(15)

        finally:
            # Плавное завершение
            shutdown_handler()

    if retry_count >= max_retries:
        logger.error(f"🚫 Достигнут лимит попыток перезапуска. Бот остановлен.")


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("🚀 Запуск бота с оптимизациями...")
    logger.info("=" * 50)

    # Выполняем стартовые задачи
    run_startup_tasks()

    # Логирование загрузки вопросов
    logger.info("📂 Загрузка вопросов...")
    check_and_load_questions()

    # ПРИНУДИТЕЛЬНО ЗАПУСКАЕМ ПЛАНИРОВЩИК
    logger.info("⏰ Настройка и ЗАПУСК планировщика...")
    scheduler = setup_scheduler()

    if scheduler:
        logger.info("✅ Планировщик успешно запущен")
        # НЕМЕДЛЕННО ПРОВЕРЯЕМ ПОДПИСКИ
        check_and_update_subscriptions()
    else:
        logger.error("❌ Не удалось запустить планировщик!")

    # Настраиваем обработчики сигналов
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    atexit.register(shutdown_handler)

    # Запускаем бота в безопасном режиме
    safe_polling()

    # Финальная очистка
    logger.info("🧹 Завершение работы...")
    user_data_manager.cleanup_old_data()
    shutdown_handler()

    logger.info("👋 Бот завершил работу")