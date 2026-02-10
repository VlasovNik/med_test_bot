import os
import random
import re
import telebot
from telebot import types
import sqlite3
import atexit
import signal
import sys
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone as pytz_timezone
import logging
from logging.handlers import RotatingFileHandler
import traceback
import json
from collections import defaultdict
from typing import Optional, Dict, List, Any, Set
import shutil
import yookassa
from yookassa import Payment, Configuration
from yookassa.domain.notification import WebhookNotificationEventType, WebhookNotificationFactory
import uuid

# Загрузка переменных окружения
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# КОНСТАНТЫ И КОНФИГУРАЦИЯ
# ============================================================================
TOKEN = os.getenv('BOT_TOKEN')
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения!")
user_extend_states = {}
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
        print(f"✅ ЮKassa настроена. Цена подписки: {SUBSCRIPTION_PRICE}₽")
    except Exception as e:
        print(f"⚠️ Ошибка настройки ЮKassa: {e}")
else:
    print("⚠️ ЮKassa не настроена (отсутствуют SHOP_ID или SECRET_KEY)")

bot = telebot.TeleBot(TOKEN)
NOVOSIBIRSK_TZ = pytz_timezone('Asia/Novosibirsk')
# Глобальные переменные
questions_by_topic = {}
topics_list = []
questions_loaded = False
session_stats = {}
user_data = {}
scheduler = None


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

setup_logging()
logger = logging.getLogger(__name__)

# ============================================================================
# ДОПОЛНИТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ УДОБНОГО ЛОГИРОВАНИЯ
# ============================================================================
def log_user_action(user_id: int, action: str, details: str = ""):
    """Логирование действий пользователя"""
    user_info = db.get_user(user_id)
    username = f"@{user_info.get('username', 'нет')}" if user_info else "неизвестен"
    log_msg = f"👤 Пользователь {user_id} ({username}): {action}"
    if details:
        log_msg += f" - {details}"
    logger.info(log_msg)


# Настройка повторных попыток для requests
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

# Настройка для telebot
telebot.apihelper.API_URL = "https://api.telegram.org/bot{0}/{1}"
telebot.apihelper.SESSION_TIME_TO_LIVE = 5 * 60

# ============================================================================
# КЛАСС БАЗЫ ДАННЫХ
# ============================================================================
class Database:
    def __init__(self, db_path: str = 'data/users.db'):
        self.db_path = db_path
        self.create_data_directory()
        self.init_database()
        print(f"✅ База данных инициализирована: {self.db_path}")

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
            print(f"❌ Ошибка при создании базы данных: {e}")

    def get_connection(self) -> sqlite3.Connection:
        """Получение соединения с базой данных"""
        return sqlite3.connect(self.db_path)

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
            print(f"❌ Ошибка при добавлении пользователя: {e}")
            return False

    def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получение информации о пользователе"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM users WHERE telegram_id = ?', (telegram_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                return dict(row)
            return None

        except sqlite3.Error as e:
            print(f"❌ Ошибка при получении пользователя: {e}")
            return None

    def check_subscription(self, telegram_id: int) -> bool:
        """Проверка подписки пользователя с учетом точного времени"""
        try:
            user = self.get_user(telegram_id)
            if not user:
                return False

            # Администраторы всегда имеют доступ
            if user.get('is_admin'):
                return True

            # Проверяем оплату
            if not user.get('subscription_paid'):
                return False

            # Проверяем дату окончания подписки
            if user.get('subscription_end_date'):
                try:
                    # Парсим дату-время
                    end_datetime = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
                    if end_datetime < datetime.now():
                        return False
                except (ValueError, TypeError):
                    # Если формат старый (только дата), пытаемся распарсить
                    try:
                        end_date = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d').date()
                        if end_date < datetime.now().date():
                            return False
                    except (ValueError, TypeError):
                        return False

            return True

        except Exception as e:
            print(f"❌ Ошибка при проверке подписки: {e}")
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
            print(f"❌ Ошибка при обновлении активности: {e}")
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
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except sqlite3.Error as e:
            print(f"❌ Ошибка при получении статистики: {e}")
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
            print(f"❌ Ошибка при инициализации статистики: {e}")
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
            print(f"❌ Ошибка при обновлении статистики: {e}")
            return False

    def update_subscription(self, telegram_id: int, paid_status=True, end_datetime=None, is_trial=False) -> bool:
        """Обновление подписки с точным временем окончания - ИСПРАВЛЕННАЯ"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем текущую дату окончания подписки
            cursor.execute('''
            SELECT subscription_end_date, subscription_paid 
            FROM users 
            WHERE telegram_id = ?
            ''', (telegram_id,))

            result = cursor.fetchone()
            current_end_datetime = None

            if result and result[0] and result[1]:  # Есть активная подписка
                try:
                    current_end_datetime = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                except:
                    current_end_datetime = None

            # Определяем новую дату окончания
            if end_datetime:
                # Если передана конкретная дата
                new_end_datetime = end_datetime
            elif is_trial:
                # Пробная подписка: 1 день от текущего момента или от текущей даты окончания
                if current_end_datetime and current_end_datetime > datetime.now():
                    new_end_datetime = current_end_datetime + timedelta(days=1)
                else:
                    new_end_datetime = datetime.now() + timedelta(days=1)
            else:
                # Обычная подписка: 30 дней от текущего момента или от текущей даты окончания
                if current_end_datetime and current_end_datetime > datetime.now():
                    new_end_datetime = current_end_datetime + timedelta(days=30)
                else:
                    new_end_datetime = datetime.now() + timedelta(days=30)

            # Форматируем даты в строки для базы данных
            start_datetime = datetime.now()
            start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            end_str = new_end_datetime.strftime('%Y-%m-%d %H:%M:%S')

            if is_trial:
                cursor.execute('''
                UPDATE users 
                SET subscription_paid = ?, 
                    subscription_start_date = ?, 
                    subscription_end_date = ?,
                    is_trial_used = TRUE,
                    last_activity = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (paid_status, start_str, end_str, telegram_id))
            else:
                cursor.execute('''
                UPDATE users 
                SET subscription_paid = ?, 
                    subscription_start_date = ?, 
                    subscription_end_date = ?,
                    last_activity = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                ''', (paid_status, start_str, end_str, telegram_id))

            conn.commit()
            conn.close()

            logger.info(
                f"Подписка пользователя {telegram_id} обновлена до {end_str} (была: {result[0] if result else 'нет'})")
            return True

        except sqlite3.Error as e:
            print(f"❌ Ошибка при обновлении подписки: {e}")
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
            print(f"❌ Ошибка при получении администраторов: {e}")
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
            print(f"❌ Ошибка при получении списка пользователей: {e}")
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
            print(f"❌ Ошибка при получении всей статистики: {e}")
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
            print(f"❌ Ошибка при получении топа: {e}")
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
            print(f"✅ Статистика пользователя {telegram_id} сброшена")
            return True

        except sqlite3.Error as e:
            print(f"❌ Ошибка при сбросе статистики: {e}")
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
            print(f"✅ Пользователь {telegram_id} {status} администратором")
            return True

        except sqlite3.Error as e:
            print(f"❌ Ошибка при изменении прав администратора: {e}")
            return False

    def grant_subscription(self, telegram_id: int, days: int = 30) -> bool:
        """Выдача подписки пользователю с точным временем"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            start_datetime = datetime.now()
            end_datetime = datetime.now() + timedelta(days=days)

            start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute('''
            UPDATE users 
            SET subscription_paid = TRUE,
                subscription_start_date = ?,
                subscription_end_date = ?,
                last_activity = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            ''', (start_str, end_str, telegram_id))

            conn.commit()
            conn.close()
            print(f"✅ Пользователю {telegram_id} выдана подписка до {end_str}")
            return True

        except sqlite3.Error as e:
            print(f"❌ Ошибка при выдаче подписки: {e}")
            return False

    def extend_subscription(self, telegram_id: int, hours: int = 0, days: int = 0) -> bool:
        """Продление подписки пользователю на указанное время"""
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

            # Определяем новую дату окончания
            if current_end_date_str and subscription_paid:
                try:
                    # Если есть активная подписка, продлеваем от текущей даты окончания
                    current_end = datetime.strptime(current_end_date_str, '%Y-%m-%d %H:%M:%S')
                    new_end = current_end + timedelta(days=days, hours=hours)
                except ValueError:
                    # Если формат неверный, продлеваем от текущего момента
                    new_end = datetime.now() + timedelta(days=days, hours=hours)
            else:
                # Если подписки нет, начинаем с текущего момента
                new_end = datetime.now() + timedelta(days=days, hours=hours)

            new_end_str = new_end.strftime('%Y-%m-%d %H:%M:%S')

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

            # Логируем продление
            logger.info(f"Подписка пользователя {telegram_id} продлена до {new_end_str} (+{days} дней, +{hours} часов)")
            return True

        except Exception as e:
            logger.error(f"Ошибка при продлении подписки для {telegram_id}: {e}")
            return False

    def extend_all_active_subscriptions(self, hours: int = 0, days: int = 0) -> dict:
        """Продление подписки всем пользователям с активной подпиской - ИСПРАВЛЕННАЯ"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем всех пользователей с активной подпиской
            cursor.execute('''
            SELECT telegram_id, subscription_end_date 
            FROM users 
            WHERE subscription_paid = TRUE 
            AND subscription_end_date IS NOT NULL
            ''')

            users = cursor.fetchall()
            results = {
                'total': len(users),
                'success': 0,
                'failed': 0,
                'errors': []
            }

            for telegram_id, current_end_date_str in users:
                try:
                    if current_end_date_str:
                        try:
                            current_end = datetime.strptime(current_end_date_str, '%Y-%m-%d %H:%M:%S')
                            new_end = current_end + timedelta(days=days, hours=hours)
                        except ValueError:
                            # Если формат неверный, продлеваем от текущего момента
                            new_end = datetime.now() + timedelta(days=days, hours=hours)
                    else:
                        new_end = datetime.now() + timedelta(days=days, hours=hours)

                    new_end_str = new_end.strftime('%Y-%m-%d %H:%M:%S')

                    cursor.execute('''
                    UPDATE users 
                    SET subscription_end_date = ?,
                        last_activity = CURRENT_TIMESTAMP
                    WHERE telegram_id = ?
                    ''', (new_end_str, telegram_id))

                    results['success'] += 1
                    logger.info(f"Подписка продлена для {telegram_id} до {new_end_str}")

                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append(f"{telegram_id}: {str(e)}")
                    logger.error(f"Ошибка продления для {telegram_id}: {e}")

            conn.commit()
            conn.close()
            return results

        except Exception as e:
            logger.error(f"Ошибка при массовом продлении подписок: {e}")
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
            print(f"❌ Ошибка при создании платежа: {e}")
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
            print(f"❌ Ошибка при обновлении статуса платежа: {e}")
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
            print(f"❌ Ошибка при отметке платежа: {e}")
            return False


# ============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С ВОПРОСАМИ
# ============================================================================
def load_and_parse_questions(filename: str) -> bool:
    """Загрузка и парсинг вопросов из файла"""
    global questions_by_topic, topics_list, questions_loaded

    try:
        if not os.path.exists(filename):
            print(f"❌ Файл '{filename}' не найден!")
            return False

        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()

        questions_by_topic.clear()
        topics_list.clear()

        # Разделяем по темам (строки с "МДК")
        lines = content.split('\n')

        current_topic = None
        current_question = None
        current_answers = []
        in_question = False

        for i, line in enumerate(lines):
            line = line.strip()

            # Пропускаем пустые строки
            if not line:
                continue

            # Проверяем, является ли строка темой (начинается с МДК)
            if line.startswith('МДК'):
                # Сохраняем предыдущий вопрос, если есть
                if current_question and current_answers:
                    if current_topic:
                        # Удаляем возможные дубликаты в тексте вопроса
                        current_question = ' '.join(current_question.split())
                        questions_by_topic[current_topic].append({
                            'question': current_question,
                            'answers': current_answers.copy()
                        })

                # Начинаем новую тему
                current_topic = line
                if current_topic not in questions_by_topic:
                    questions_by_topic[current_topic] = []
                    topics_list.append(current_topic)

                current_question = None
                current_answers = []
                in_question = False
                continue

            # Проверяем, является ли строка началом вопроса (начинается с числа и точки)
            if re.match(r'^\d+\.', line):
                # Сохраняем предыдущий вопрос, если есть
                if current_question and current_answers:
                    if current_topic:
                        # Удаляем возможные дубликаты в тексте вопроса
                        current_question = ' '.join(current_question.split())
                        questions_by_topic[current_topic].append({
                            'question': current_question,
                            'answers': current_answers.copy()
                        })

                # Извлекаем номер вопроса
                match = re.match(r'^(\d+)\.\s*(.*)', line)
                if match:
                    question_number = match.group(1)
                    question_text = match.group(2).strip()

                    # Если после номера есть только "Выберите правильный ответ" или пусто,
                    # то текст вопроса может быть на следующей строке
                    if not question_text or question_text.lower() == 'выберите правильный ответ':
                        # Ищем текст вопроса на следующих непустых строках до первого ответа
                        question_lines = []
                        j = i + 1
                        while j < len(lines):
                            next_line = lines[j].strip()
                            if not next_line:
                                j += 1
                                continue

                            # Если следующая строка начинается с + или -, это ответ
                            if next_line.startswith('+') or next_line.startswith('-'):
                                break

                            # Если следующая строка начинается с числа и точки, это новый вопрос
                            if re.match(r'^\d+\.', next_line):
                                break

                            # Если следующая строка начинается с МДК, это новая тема
                            if next_line.startswith('МДК'):
                                break

                            question_lines.append(next_line)
                            j += 1

                        if question_lines:
                            question_text = ' '.join(question_lines)
                        elif not question_text:
                            question_text = f"Вопрос {question_number}"
                    else:
                        # Если есть текст вопроса, но он может быть неполным
                        # Ищем продолжение на следующих строках до ответа
                        j = i + 1
                        while j < len(lines):
                            next_line = lines[j].strip()
                            if not next_line:
                                j += 1
                                continue

                            # Если следующая строка начинается с + или -, это ответ
                            if next_line.startswith('+') or next_line.startswith('-'):
                                break

                            # Если следующая строка начинается с числа и точки, это новый вопрос
                            if re.match(r'^\d+\.', next_line):
                                break

                            # Если следующая строка начинается с МДК, это новая тема
                            if next_line.startswith('МДК'):
                                break

                            # Это продолжение текста вопроса
                            question_text += ' ' + next_line
                            j += 1

                    current_question = f"{question_number}. {question_text}"
                    current_answers = []
                    in_question = True
                continue

            # Если мы внутри вопроса и строка начинается с + или -, это ответ
            if in_question and (line.startswith('+') or line.startswith('-')):
                is_correct = line.startswith('+')
                # Извлекаем текст ответа
                # Убираем + или - и возможный пробел
                answer_text = line[1:].strip()
                # Если после знака есть пробел, убираем его
                if answer_text.startswith(' '):
                    answer_text = answer_text[1:]

                # Ищем продолжение ответа на следующих строках
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    if not next_line:
                        j += 1
                        continue

                    # Если следующая строка начинается с + или -, это новый ответ
                    if next_line.startswith('+') or next_line.startswith('-'):
                        break

                    # Если следующая строка начинается с числа и точки, это новый вопрос
                    if re.match(r'^\d+\.', next_line):
                        break

                    # Если следующая строка начинается с МДК, это новая тема
                    if next_line.startswith('МДК'):
                        break

                    # Это продолжение ответа
                    answer_text += ' ' + next_line
                    j += 1

                if answer_text:
                    current_answers.append({
                        'text': answer_text,
                        'correct': is_correct
                    })
                continue

        # Сохраняем последний вопрос
        if current_topic and current_question and current_answers:
            # Удаляем возможные дубликаты в тексте вопроса
            current_question = ' '.join(current_question.split())
            questions_by_topic[current_topic].append({
                'question': current_question,
                'answers': current_answers
            })

        # Проверяем, что все вопросы имеют хотя бы один правильный ответ
        for topic in questions_by_topic:
            for question in questions_by_topic[topic]:
                has_correct = any(answer['correct'] for answer in question['answers'])
                if not has_correct and question['answers']:
                    # Если нет правильного ответа, помечаем первый как правильный
                    question['answers'][0]['correct'] = True
                    print(f"⚠️ В теме '{topic}' вопрос без правильного ответа: {question['question'][:50]}...")

        # Добавляем опцию "Все темы"
        if topics_list:
            topics_list.append("🎲 Все темы (рандом)")
            questions_loaded = True

            print(f"\n✅ Загружено {len(topics_list) - 1} тем")
            total_questions = 0
            for topic in topics_list:
                if topic != "🎲 Все темы (рандом)":
                    topic_questions = len(questions_by_topic[topic])
                    total_questions += topic_questions
                    print(f"  - {topic}: {topic_questions} вопросов")

            print(f"📊 Всего вопросов: {total_questions}")

            # Отладочная информация - выводим несколько примеров вопросов
            print("\n🔍 Примеры загруженных вопросов:")
            for topic in list(questions_by_topic.keys())[:2]:
                print(f"\nТема: {topic}")
                for i, question in enumerate(questions_by_topic[topic][:3], 1):
                    print(f"  {i}. {question['question'][:80]}...")

            return True
        else:
            print("❌ Не удалось загрузить ни одной темы")
            return False

    except Exception as e:
        print(f"❌ Ошибка при загрузке вопросов: {e}")
        traceback.print_exc()
        return False


def get_random_question_from_topic(topic_name: str) -> Optional[Dict]:
    """Получение случайного вопроса из темы"""
    try:
        if topic_name == "🎲 Все темы (рандом)":
            all_questions = []
            for topic in questions_by_topic.keys():
                all_questions.extend(questions_by_topic[topic])

            if not all_questions:
                return None

            return random.choice(all_questions)
        elif topic_name in questions_by_topic:
            questions = questions_by_topic[topic_name]
            if questions:
                return random.choice(questions)

        return None
    except Exception as e:
        print(f"❌ Ошибка при получении вопроса: {e}")
        return None


def check_and_load_questions() -> bool:
    """Проверка и загрузка вопросов"""
    global questions_loaded

    if os.path.exists('тест.txt'):
        print("📂 Файл 'тест.txt' найден. Загружаю вопросы...")
        questions_loaded = load_and_parse_questions('тест.txt')
        if questions_loaded:
            print("✅ Вопросы успешно загружены!")
        else:
            print("❌ Не удалось загрузить вопросы")
        return questions_loaded
    else:
        print("❌ Файл 'тест.txt' не найден!")
        return False


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
db = Database()


def create_yookassa_payment(telegram_id: int) -> Optional[Dict]:
    """Создание платежа в ЮKassa - упрощенная версия"""
    try:
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
            print("❌ ЮKassa не настроена")
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
            print(f"✅ Создан платеж {payment.id} для пользователя {telegram_id}")
            return {
                'id': payment.id,
                'status': payment.status,
                'confirmation_url': payment.confirmation.confirmation_url,
                'amount': SUBSCRIPTION_PRICE,
                'description': description
            }
        else:
            print(f"❌ Не удалось сохранить платеж в БД")
            return None

    except Exception as e:
        print(f"❌ Ошибка при создании платежа: {e}")
        return None

def check_user_access(chat_id: int, send_message: bool = True) -> bool:
    """Проверка доступа пользователя"""
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

    if not db.check_subscription(chat_id):
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
        print("✅ Основные команды бота настроены")

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
                print(f"✅ Админские команды настроены для {admin_id}")
            except Exception as e:
                print(f"⚠️ Ошибка настройки админских команд для {admin_id}: {e}")

        return True

    except Exception as e:
        print(f"❌ Ошибка настройки команд бота: {e}")
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
    """Отправка вопроса с вариантами ответов"""
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

    if chat_id not in user_data or not user_data[chat_id].get('current_topic'):
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

    topic = user_data[chat_id]['current_topic']

    # Получаем случайный вопрос из темы
    question_data = get_random_question_from_topic(topic)

    if not question_data:
        if message_id:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"❌ Не удалось получить вопрос из темы '{topic}'.",
                reply_markup=create_back_button("change_topic")
            )
        else:
            bot.send_message(
                chat_id,
                f"❌ Не удалось получить вопрос из темы '{topic}'.",
                reply_markup=create_back_button("change_topic")
            )
        return

    # Сохраняем данные вопроса
    user_data[chat_id]['current_question'] = question_data['question']
    user_data[chat_id]['correct_answer'] = None
    user_data[chat_id]['numbered_answers'] = {}
    user_data[chat_id]['answers_list'] = []

    # Извлекаем правильные ответы
    correct_answers = []
    for answer in question_data['answers']:
        if answer['correct']:
            correct_answers.append(answer['text'])

    user_data[chat_id]['correct_answer'] = correct_answers

    # Перемешиваем ответы
    answers = question_data['answers'].copy()
    random.shuffle(answers)

    # Сохраняем список ответов для отображения
    answers_texts = []
    for i, answer in enumerate(answers, 1):
        answer_text = answer['text']
        answers_texts.append(f"{i}. {answer_text}")
        user_data[chat_id]['numbered_answers'][i] = answer['text']
        user_data[chat_id]['answers_list'].append(answer_text)

    # Формируем текст вопроса
    topic_display = topic
    question_text = f"📚 <b>Тема:</b> {topic_display}\n\n"

    # Добавляем статистику сессии если есть
    if chat_id in session_stats and session_stats[chat_id]['session_total'] > 0:
        session_total = session_stats[chat_id]['session_total']
        session_correct = session_stats[chat_id]['session_correct']
        session_percentage = (session_correct / session_total * 100) if session_total > 0 else 0
        question_text += f"📊 <b>Сессия:</b> {session_correct}/{session_total} ({session_percentage:.1f}%)\n\n"

    # Форматируем текст вопроса
    q_text = question_data['question']
    # Удаляем лишние пробелы и дубликаты
    q_text = ' '.join(q_text.split())
    question_text += f"❓ <b>Вопрос:</b>\n{q_text}\n\n"

    # Добавляем варианты ответов
    question_text += "📋 <b>Варианты ответов:</b>\n"
    for answer_line in answers_texts:
        question_text += f"{answer_line}\n"

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
        print(f"❌ Ошибка при отправке вопроса: {e}")
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
    """Выдача подписки пользователю"""
    chat_id = message.chat.id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.send_message(chat_id, "❌ У вас нет прав для этой команды.")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.send_message(chat_id, "❌ Использование: /grant_sub <user_id> [days=30]")
            return

        target_id = int(parts[1])
        days = 30 if len(parts) < 3 else int(parts[2])

        if db.grant_subscription(target_id, days):
            bot.send_message(chat_id, f"✅ Пользователю {target_id} выдана подписка на {days} дней")
        else:
            bot.send_message(chat_id, f"❌ Не удалось выдать подписку пользователю {target_id}")

    except ValueError:
        bot.send_message(chat_id, "❌ Неверный формат данных")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")


@bot.message_handler(commands=['checkmypayment'])
def handle_check_my_payment(message):
    """Проверка последнего платежа пользователя"""
    chat_id = message.chat.id

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
    bot.answer_callback_query(call.id)


def random_question_callback(call):
    """Обработчик случайного вопроса"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        bot.answer_callback_query(call.id, "❌ Требуется активная подписка!")
        return

    # Устанавливаем тему "Все темы"
    if chat_id not in user_data:
        user_data[chat_id] = {}

    user_data[chat_id]['current_topic'] = "🎲 Все темы (рандом)"
    user_data[chat_id]['current_question'] = None
    user_data[chat_id]['correct_answer'] = None
    user_data[chat_id]['numbered_answers'] = {}
    user_data[chat_id]['answers_list'] = []

    # Отправляем вопрос
    bot.answer_callback_query(call.id, "🎲 Загружаю случайный вопрос...")

    send_question_inline(chat_id, message_id)


def show_stats_callback(call):
    """Обработчик показа статистики"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        bot.answer_callback_query(call.id, "❌ Требуется активная подписка!")
        return

    show_stats_message(chat_id, message_id)


def change_topic_callback(call):
    """Обработчик смены темы"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        bot.answer_callback_query(call.id, "❌ Требуется активная подписка!")
        return

    # Формируем текст со списком тем
    topics_text = "📚 <b>ДОСТУПНЫЕ ТЕМЫ:</b>\n\n"
    for i, topic in enumerate(topics_list, 1):
        topics_text += f"{i}. {topic}\n"

    topics_text += "\n👇 Выберите номер темы:"

    # Создаем inline клавиатуру
    markup = types.InlineKeyboardMarkup(row_width=5)

    # Кнопки с номерами тем
    buttons = []
    for i in range(1, len(topics_list) + 1):
        buttons.append(types.InlineKeyboardButton(
            text=str(i),
            callback_data=f"topic_{i - 1}"
        ))

    # Добавляем кнопки по 5 в ряд
    for i in range(0, len(buttons), 5):
        markup.row(*buttons[i:i + 5])

    markup.row(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=topics_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    bot.answer_callback_query(call.id)


def get_question_callback(call):
    """Обработчик получения вопроса"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        bot.answer_callback_query(call.id, "❌ Требуется активная подписка!")
        return

    # Удаляем ответ на callback, чтобы не было двойных сообщений
    bot.answer_callback_query(call.id, "🔄 Загружаю вопрос...")


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

            time_left = end_datetime - datetime.now()
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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
        bot.answer_callback_query(call.id, "❌ Система оплаты недоступна")
        return

    bot.answer_callback_query(call.id, "🔄 Создаю платеж...")

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
                if end_datetime < datetime.now():
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
                    bot.answer_callback_query(call.id, "❌ Пробный доступ уже был использован!")
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
        bot.answer_callback_query(call.id, "❌ Пробный доступ уже был использован!")
        return

    # Даем пробный доступ на 1 день от текущего момента
    end_datetime = datetime.now() + timedelta(days=1)

    # Проверяем, нет ли уже активной подписки
    if user and user.get('subscription_paid') and user.get('subscription_end_date'):
        try:
            current_end = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
            if current_end > datetime.now():
                # Уже есть активная подписка
                bot.answer_callback_query(call.id, "✅ У вас уже есть активная подписка!")
                return
        except:
            pass

    db.update_subscription(chat_id, True, end_datetime, is_trial=True)

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
    bot.answer_callback_query(call.id, "✅ Пробный доступ активирован!")

def info_callback(call):
    """Обработчик информации о боте"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    info_text = f"""
ℹ️ <b>Информация о боте</b>

🤖 <b>Бот для подготовки к тестам</b>
Версия: 1.0

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

def show_stats_message(chat_id, message_id=None):
    """Показать статистику пользователя - ДОБАВЬТЕ ЭТУ ФУНКЦИЮ"""
    stats = db.get_user_statistics(chat_id)

    if not stats or stats['total_answers'] == 0:
        stats_text = "📊 Статистика еще не собрана. Начните отвечать на вопросы!"
    else:
        total_answers = stats['total_answers']
        correct_answers = stats['correct_answers']
        correct_percentage = (correct_answers / total_answers) * 100

        # Получаем статистику сессии
        session_total = session_stats.get(chat_id, {}).get('session_total', 0)
        session_correct = session_stats.get(chat_id, {}).get('session_correct', 0)
        session_percentage = (session_correct / session_total * 100) if session_total > 0 else 0

        stats_text = f"""
📊 <b>ВАША СТАТИСТИКА</b>

📈 <b>Всего отвечено вопросов:</b> {total_answers}
✅ <b>Правильных ответов:</b> {correct_answers}
❌ <b>Неправильных ответов:</b> {total_answers - correct_answers}
🎯 <b>Процент правильных ответов:</b> {correct_percentage:.1f}%

📊 <b>Статистика сессии:</b>
✅ Правильных: {session_correct}/{session_total} ({session_percentage:.1f}%)
"""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏆 Топ игроков", callback_data="top_players"))

    if user_data.get(chat_id, {}).get('current_topic'):
        markup.add(
            types.InlineKeyboardButton("🎲 Продолжить тренировку", callback_data="get_question"),
            types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic")
        )
    else:
        markup.add(
            types.InlineKeyboardButton("📚 Выбрать тему", callback_data="change_topic"),
            types.InlineKeyboardButton("🎲 Случайный вопрос", callback_data="random_question")
        )

    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

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

def admin_broadcast_callback(call):
    """Массовая рассылка через админ-панель"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Сохраняем состояние пользователя
    user_broadcast_states[chat_id] = {
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
    bot.answer_callback_query(call.id)


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
    bot.answer_callback_query(call.id)


def extend_user_menu_callback(call):
    """Меню продления подписки конкретному пользователю"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Сохраняем состояние
    user_extend_states[chat_id] = {
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
    bot.answer_callback_query(call.id)


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
    bot.answer_callback_query(call.id)


def handle_extend_all_callback(call):
    """Обработка выбора срока продления для всех"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Парсим данные из callback (формат: extend_all_[тип]_[значение])
        parts = call.data.split('_')

        if len(parts) < 4:
            bot.answer_callback_query(call.id, "❌ Неверный формат команды")
            return

        time_type = parts[2]  # hours или days
        value = int(parts[3])  # значение

        # Устанавливаем часы и дни
        hours = value if time_type == 'hours' else 0
        days = value if time_type == 'days' else 0

        # Создаем подтверждающее сообщение
        time_text = ""
        if hours > 0:
            time_text = f"{hours} час(ов)"
        elif days > 0:
            time_text = f"{days} день(ей)"

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(f"✅ Да, продлить на {time_text}",
                                       callback_data=f"confirm_extend_all_{hours}_{days}"),
            types.InlineKeyboardButton("❌ Нет, отмена", callback_data="extend_all_menu")
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
        bot.answer_callback_query(call.id)

    except Exception as e:
        logger.error(f"Ошибка в handle_extend_all_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка обработки запроса")


def handle_confirm_extend_callback(call):
    """Подтверждение и выполнение продления"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        if call.data.startswith("confirm_extend_all_"):
            # Продление всем
            _, _, _, hours_str, days_str = call.data.split('_')
            hours = int(hours_str)
            days = int(days_str)

            bot.answer_callback_query(call.id, "⏳ Продлеваю подписки...")

            # Выполняем продление
            result = db.extend_all_active_subscriptions(hours=hours, days=days)

            # Формируем отчет
            time_text = ""
            if hours > 0 and days > 0:
                time_text = f"{hours} час(ов) и {days} день(ей)"
            elif hours > 0:
                time_text = f"{hours} час(ов)"
            elif days > 0:
                time_text = f"{days} день(ей)"

            report = f"✅ <b>Продление завершено!</b>\n\n"
            report += f"📅 Срок: {time_text}\n"
            report += f"👥 Всего пользователей: {result['total']}\n"
            report += f"✅ Успешно: {result['success']}\n"
            report += f"❌ Ошибок: {result['failed']}\n"

            if result['errors']:
                report += f"\n📝 Ошибки (первые 5):\n"
                for error in result['errors'][:5]:
                    report += f"• {error}\n"
                if len(result['errors']) > 5:
                    report += f"... и еще {len(result['errors']) - 5} ошибок"

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("↩️ Назад к продлению", callback_data="admin_extend_sub"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=report,
                parse_mode='HTML',
                reply_markup=markup
            )

        elif call.data.startswith("confirm_extend_user_"):
            # Продление конкретному пользователю
            parts = call.data.split('_')
            user_id = int(parts[3])
            hours = int(parts[4])
            days = int(parts[5])

            bot.answer_callback_query(call.id, "⏳ Продлеваю подписку...")

            # Выполняем продление
            if db.extend_subscription(user_id, hours=hours, days=days):
                time_text = ""
                if hours > 0 and days > 0:
                    time_text = f"{hours} час(ов) и {days} день(ей)"
                elif hours > 0:
                    time_text = f"{hours} час(ов)"
                elif days > 0:
                    time_text = f"{days} день(ей)"

                # Получаем обновленную информацию о пользователе
                user = db.get_user(user_id)
                end_date = user.get('subscription_end_date', 'неизвестно')

                report = f"✅ <b>Подписка продлена!</b>\n\n"
                report += f"👤 Пользователь ID: {user_id}\n"
                report += f"📅 Срок: {time_text}\n"
                report += f"🕐 Действует до: {end_date}\n"

                # Отправляем уведомление пользователю
                try:
                    notification = f"🎉 <b>Ваша подписка продлена!</b>\n\n"
                    notification += f"Администратор продлил вашу подписку на {time_text}.\n"
                    notification += f"Теперь она действует до: {end_date}"

                    bot.send_message(user_id, notification, parse_mode='HTML')
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
                    report += f"\n⚠️ Не удалось отправить уведомление пользователю"

            else:
                report = f"❌ <b>Не удалось продлить подписку</b>\n\n"
                report += f"Пользователь ID: {user_id}\n"
                report += f"Возможно, пользователь не найден или произошла ошибка."

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("↩️ Назад к продлению", callback_data="admin_extend_sub"))

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=report,
                parse_mode='HTML',
                reply_markup=markup
            )

    except Exception as e:
        logger.error(f"Ошибка в handle_confirm_extend_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при продлении")

def handle_admin_callback(call):
    """Обработка административных callback-запросов"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    user = db.get_user(chat_id)
    if not user or not user.get('is_admin'):
        bot.answer_callback_query(call.id, "❌ Нет прав администратора!")
        return

    if call.data == "admin_stats":
        admin_stats_callback(call)
    elif call.data == "admin_users":
        admin_users_callback(call)
    elif call.data == "admin_grant_sub":
        admin_grant_sub_callback(call)
    elif call.data == "admin_extend_sub":
        admin_extend_sub_callback(call)
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
    elif call.data.startswith("confirm_extend_"):  # Подтверждение продления
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
        bot.answer_callback_query(call.id, "❌ Неизвестная команда администратора")

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

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
    bot.answer_callback_query(call.id)

def check_payment_callback(call):
    """Проверка статуса платежа (без вебхуков) - ИСПРАВЛЕННАЯ"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    payment_id = call.data.split('_')[2]

    bot.answer_callback_query(call.id, "🔄 Проверяем статус оплаты...")

    # Проверяем статус платежа
    try:
        payment = Payment.find_one(payment_id)

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
                bot.answer_callback_query(call.id, "✅ Платеж уже был обработан ранее")

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
            telegram_id = payment.metadata.get('telegram_id', chat_id)

            # Проверяем, не была ли уже активирована подписка по этому платежу
            user = db.get_user(telegram_id)
            if user and user.get('subscription_paid'):
                # У пользователя уже есть активная подписка
                # Продлеваем от текущей даты окончания
                current_end = None
                if user.get('subscription_end_date'):
                    try:
                        current_end = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
                    except:
                        current_end = datetime.now()

                if current_end:
                    end_datetime = current_end + timedelta(days=30)
                else:
                    end_datetime = datetime.now() + timedelta(days=30)
            else:
                # Активируем новую подписку на 30 дней
                end_datetime = datetime.now() + timedelta(days=30)

            db.update_subscription(telegram_id, True, end_datetime)

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
                text="⏳ <b>Оплата в обработке</b>\n\nПлатеж получен, но еще не подтвержден.\nПодождите 1-2 минуты и проверьте снова.",
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
                'rejected': 'отклонен',
                'waiting_for_capture': 'ожидает подтверждения'
            }.get(payment.status, payment.status)

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"❌ <b>Платеж {status_text}</b>\n\nПожалуйста, попробуйте снова или обратитесь в поддержку.",
                parse_mode='HTML',
                reply_markup=markup
            )

    except Exception as e:
        print(f"❌ Ошибка при проверке платежа: {e}")
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_payment_{payment_id}"))
        markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ <b>Не удалось проверить статус платежа</b>\n\nПожалуйста, попробуйте позже или обратитесь в поддержку.",
            parse_mode='HTML',
            reply_markup=markup
        )


# ============================================================================
# МАССОВАЯ РАССЫЛКА СООБЩЕНИЙ ВСЕМ ПОЛЬЗОВАТЕЛЯМ
# ============================================================================

# Словарь для хранения состояний пользователей при массовой рассылке
user_broadcast_states = {}


# Добавьте обработчик сообщений
@bot.message_handler(func=lambda message: message.chat.id in user_extend_states and
                                          user_extend_states[message.chat.id]['state'] == 'waiting_for_user_id')
def handle_extend_user_id(message):
    """Обработка ввода ID пользователя для продления"""
    chat_id = message.chat.id
    user_state = user_extend_states[chat_id]

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
            del user_extend_states[chat_id]
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

        del user_extend_states[chat_id]

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
        del user_extend_states[chat_id]
    except Exception as e:
        logger.error(f"Ошибка в handle_extend_user_id: {e}")
        bot.send_message(chat_id, f"❌ Ошибка: {e}")
        if chat_id in user_extend_states:
            del user_extend_states[chat_id]


def handle_extend_user_callback(call):
    """Обработка выбора срока продления для конкретного пользователя"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Парсим данные из callback (формат: extend_user_[user_id]_[тип]_[значение])
        parts = call.data.split('_')

        if len(parts) < 5:
            bot.answer_callback_query(call.id, "❌ Неверный формат команды")
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
            bot.answer_callback_query(call.id, "❌ Пользователь не найден")
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
        bot.answer_callback_query(call.id)

    except Exception as e:
        logger.error(f"Ошибка в handle_extend_user_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка обработки запроса")

@bot.message_handler(commands=['send_all_users'])
def handle_send_all_users(call):
    """Запуск массовой рассылки сообщений всем пользователям через callback"""
    chat_id = call.message.chat.id  # Исправлено: получаем chat_id из call.message.chat
    message_id = call.message.message_id
    user = db.get_user(chat_id)

    if not user or not user.get('is_admin'):
        bot.answer_callback_query(call.id, "❌ У вас нет прав администратора для этой команды.")
        return

    # Сохраняем состояние пользователя
    user_broadcast_states[chat_id] = {
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
    bot.answer_callback_query(call.id)


@bot.message_handler(func=lambda message: message.chat.id in user_broadcast_states and
                                          user_broadcast_states[message.chat.id]['state'] == 'waiting_for_message')
def handle_broadcast_message(message):
    """Обработка сообщения для рассылки"""
    chat_id = message.chat.id
    user_state = user_broadcast_states[chat_id]

    # Сохраняем сообщение
    user_state['state'] = 'waiting_for_confirmation'
    user_state['message'] = message.text or message.caption
    user_state['message_type'] = message.content_type
    user_state['message_id'] = message.message_id

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
        user_broadcast_states[chat_id]['state'] = 'waiting_for_message'


@bot.callback_query_handler(func=lambda call: call.data in ['confirm_broadcast', 'edit_broadcast',
                                                            'cancel_broadcast', 'broadcast_active_only'])
def handle_broadcast_callback(call):
    """Обработка callback для массовой рассылки"""
    chat_id = call.message.chat.id

    if chat_id not in user_broadcast_states:
        bot.answer_callback_query(call.id, "❌ Сессия рассылки устарела")
        return

    user_state = user_broadcast_states[chat_id]

    if call.data == 'confirm_broadcast':
        # Подтверждение отправки всем пользователям
        bot.answer_callback_query(call.id, "🚀 Начинаю рассылку...")
        send_broadcast_to_all(chat_id, user_state, call.message.message_id, active_only=False)

    elif call.data == 'broadcast_active_only':
        # Отправка только активным пользователям
        bot.answer_callback_query(call.id, "🚀 Начинаю рассылку активным пользователям...")
        send_broadcast_to_all(chat_id, user_state, call.message.message_id, active_only=True)

    elif call.data == 'edit_broadcast':
        # Редактирование сообщения
        bot.answer_callback_query(call.id, "✏️ Отправьте новое сообщение")
        user_broadcast_states[chat_id]['state'] = 'waiting_for_message'

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
        bot.answer_callback_query(call.id, "❌ Рассылка отменена")
        if chat_id in user_broadcast_states:
            del user_broadcast_states[chat_id]

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
        if admin_chat_id in user_broadcast_states:
            del user_broadcast_states[admin_chat_id]

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
        if admin_chat_id in user_broadcast_states:
            del user_broadcast_states[admin_chat_id]

def select_topic_callback(call):
    """Обработчик выбора темы"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        bot.answer_callback_query(call.id, "❌ Требуется активная подписка!")
        return

    try:
        topic_num = int(call.data.split('_')[1])

        if 0 <= topic_num < len(topics_list):
            selected_topic = topics_list[topic_num]

            # Инициализируем данные пользователя если их нет
            if chat_id not in user_data:
                user_data[chat_id] = {}

            # Сохраняем выбранную тему
            user_data[chat_id]['current_topic'] = selected_topic
            user_data[chat_id]['current_question'] = None
            user_data[chat_id]['correct_answer'] = None
            user_data[chat_id]['numbered_answers'] = {}
            user_data[chat_id]['answers_list'] = []

            # Получаем количество вопросов в теме
            if selected_topic == "🎲 Все темы (рандом)":
                topic_questions_count = sum(len(q) for q in questions_by_topic.values())
            elif selected_topic in questions_by_topic:
                topic_questions_count = len(questions_by_topic[selected_topic])
            else:
                topic_questions_count = 0

            topic_info = f"""
✅ <b>Выбрана тема:</b> {selected_topic}
📊 <b>Вопросов в теме:</b> {topic_questions_count}

👇 Выберите действие:
            """

            # Создаем кнопки
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("🎲 Получить вопрос", callback_data="get_question"),
                types.InlineKeyboardButton("📊 Показать статистику", callback_data="show_stats")
            )
            markup.add(
                types.InlineKeyboardButton("🔄 Выбрать другую тему", callback_data="change_topic"),
                types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            )

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=topic_info,
                parse_mode='HTML',
                reply_markup=markup
            )

            bot.answer_callback_query(call.id, f"Выбрана тема: {selected_topic}")
        else:
            bot.answer_callback_query(call.id, "❌ Неверный номер темы.")

    except (ValueError, IndexError) as e:
        print(f"❌ Ошибка выбора темы: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при выборе темы.")


def check_answer_callback(call):
    """Обработчик проверки ответа"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем доступ
    if not check_user_access(chat_id, send_message=False):
        bot.answer_callback_query(call.id, "❌ Требуется активная подписка!")
        return

    if chat_id not in user_data:
        bot.answer_callback_query(call.id, "⚠️ Сначала выберите тему!")
        return

    if not user_data[chat_id]['current_question']:
        bot.answer_callback_query(call.id, "⚠️ Нет активного вопроса!")
        return

    try:
        answer_number = int(call.data.split('_')[1])

        if answer_number not in user_data[chat_id]['numbered_answers']:
            bot.answer_callback_query(call.id, "❌ Неверный номер ответа!")
            return

        selected_answer = user_data[chat_id]['numbered_answers'][answer_number]
        correct_answers = user_data[chat_id]['correct_answer']

        # Проверяем ответ
        is_correct = selected_answer in correct_answers

        # Обновляем статистику в базе данных
        db.update_statistics(chat_id, is_correct)

        # Обновляем статистику сессии
        if chat_id not in session_stats:
            session_stats[chat_id] = {
                'session_total': 0,
                'session_correct': 0
            }

        session_stats[chat_id]['session_total'] += 1
        if is_correct:
            session_stats[chat_id]['session_correct'] += 1

        # Получаем общую статистику
        total_stats = db.get_user_statistics(chat_id)

        # Получаем статистику текущей сессии
        session_total = session_stats[chat_id]['session_total']
        session_correct = session_stats[chat_id]['session_correct']
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
            bot.answer_callback_query(call.id, "✅ Правильно!")
        else:
            bot.answer_callback_query(call.id, "❌ Неправильно!")

    except (ValueError, IndexError) as e:
        print(f"❌ Ошибка обработки ответа: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при обработке ответа.")


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
        bot.answer_callback_query(call.id, f"❌ Ошибка: {e}")


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
        bot.answer_callback_query(call.id, f"❌ Ошибка: {e}")


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
            bot.answer_callback_query(call.id, "❌ Файл логов не найден")
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Ошибка: {e}")

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
            backup_file = f'bot.log.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
            shutil.copy2(log_file, backup_file)

            # Очищаем файл
            open(log_file, 'w').close()

            bot.answer_callback_query(call.id, "✅ Логи очищены, создана резервная копия")

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
            bot.answer_callback_query(call.id, "❌ Файл логов не найден")
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Ошибка: {e}")


def admin_db_callback(call):
    """Скачать базу данных"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        db_file = 'data/users.db'
        if os.path.exists(db_file):
            # Создаем временную копию для безопасности
            temp_file = f'users_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
            shutil.copy2(db_file, temp_file)

            with open(temp_file, 'rb') as f:
                bot.send_document(chat_id, f, caption="📁 Резервная копия базы данных")

            # Удаляем временный файл
            os.remove(temp_file)

            bot.answer_callback_query(call.id, "✅ Файл базы данных отправлен")
        else:
            bot.answer_callback_query(call.id, "❌ Файл базы данных не найден")
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Ошибка: {e}")

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
        bot.answer_callback_query(call.id, "✅ Команда на перезагрузку отправлена")

    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Ошибка: {e}")

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
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    """Обработчик всех callback-запросов"""
    try:
        chat_id = call.message.chat.id
        message_id = call.message.message_id

        print(f"🔄 Callback: {call.data} от {chat_id}")

        # Маршрутизация по типам callback
        if call.data == "main_menu":
            main_menu_callback(call)
        elif call.data == "random_question":
            random_question_callback(call)
        elif call.data == "show_stats":
            show_stats_callback(call)
        elif call.data == "change_topic":
            change_topic_callback(call)
        elif call.data == "get_question":
            get_question_callback(call)
        elif call.data == "subscribe_info":
            subscribe_info_callback(call)
        elif call.data == "subscribe":
            subscribe_callback(call)
        elif call.data == "trial":
            trial_callback(call)
        elif call.data == "info":
            info_callback(call)
        elif call.data == "help_menu":
            help_menu_callback(call)
        elif call.data == "check_questions":
            check_questions_callback(call)
        elif call.data.startswith('topic_'):
            select_topic_callback(call)
        elif call.data.startswith('answer_'):
            check_answer_callback(call)
        elif call.data == "top_players":
            top_players_callback(call)
        elif call.data == "subscription_terms":
            subscription_terms_callback(call)
        elif call.data == "pay_now":
            pay_now_callback(call)
        elif call.data.startswith('check_payment_'):
            check_payment_callback(call)
        elif call.data == "payment_instructions":
            payment_instructions_callback(call)
        # Админские callback-ы
        elif any(call.data.startswith(prefix) for prefix in ['admin_', 'logs_', 'restart_', 'back_to_admin',
                                                              'confirm_extend_']):
            handle_admin_callback(call)
        # Обработчики продления подписки (отдельно)
        elif call.data == "extend_user_menu":
            extend_user_menu_callback(call)
        elif call.data == "extend_all_menu":
            extend_all_menu_callback(call)
        elif call.data.startswith('extend_user_') and not call.data.startswith('extend_user_menu'):
            handle_extend_user_callback(call)
        elif call.data.startswith('extend_all_') and not call.data.startswith('extend_all_menu'):
            handle_extend_all_callback(call)
        else:
            bot.answer_callback_query(call.id, "❌ Неизвестная команда")

    except Exception as e:
        print(f"❌ Ошибка в обработчике callback: {e}")
        traceback.print_exc()
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка")
        except:
            pass

@bot.message_handler(commands=['start'])
def handle_start(message):
    """Обработчик команды /start"""
    chat_id = message.chat.id
    user = message.from_user

    print(f"📨 Получен /start от {user.first_name} (ID: {chat_id})")

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

    # Инициализируем данные пользователя
    user_data[chat_id] = {
        'current_topic': None,
        'current_question': None,
        'correct_answer': None,
        'numbered_answers': {},
        'answers_list': []
    }

    # Инициализируем статистику сессии
    session_stats[chat_id] = {
        'session_total': 0,
        'session_correct': 0
    }

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

# ============================================================================
# ЗАПУСК БОТА
# ============================================================================
def setup_scheduler():
    """Настройка планировщика задач"""
    global scheduler

    try:
        scheduler = BackgroundScheduler()

        # Ежедневная проверка подписок
        scheduler.add_job(
            check_and_update_subscriptions,
            trigger=CronTrigger(hour=0, minute=0, timezone=NOVOSIBIRSK_TZ),
            id='daily_subscription_check',
            name='Проверка подписок'
        )

        scheduler.start()
        print("✅ Планировщик задач запущен")

        # Выводим информацию о запущенных задачах
        jobs = scheduler.get_jobs()
        print(f"📋 Загружено задач: {len(jobs)}")
        for job in jobs:
            next_run = job.next_run_time.astimezone(NOVOSIBIRSK_TZ).strftime(
                '%d.%m.%Y %H:%M') if job.next_run_time else "Не запланировано"
            print(f"  - {job.name}: следующий запуск {next_run}")

        return scheduler

    except Exception as e:
        print(f"❌ Ошибка при настройке планировщика: {e}")
        return None


def check_and_update_subscriptions():
    """Проверка и обновление подписок с учетом точного времени"""
    try:
        current_datetime = datetime.now(NOVOSIBIRSK_TZ)

        conn = db.get_connection()
        cursor = conn.cursor()

        # Находим истекшие подписки (используем TIMESTAMP сравнение)
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
                # Пытаемся парсить с точным временем
                try:
                    end_datetime = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    # Если старый формат, добавляем время 23:59:59
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                    end_datetime = datetime.combine(end_date, datetime.max.time())

                if end_datetime < current_datetime:
                    expired_users.append({
                        'id': user_id,
                        'username': username,
                        'first_name': first_name,
                        'end_date': end_date_str
                    })
                    users_to_update.append(user_id)
            except (ValueError, TypeError) as e:
                print(f"⚠️ Ошибка парсинга даты для пользователя {user_id}: {e}")
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
            print(f"✅ Обновлено {len(users_to_update)} истекших подписок")

        conn.close()

    except Exception as e:
        print(f"❌ Ошибка при проверке подписок: {e}")


def shutdown_handler(signum=None, frame=None):
    """Обработчик завершения работы"""
    logger.info("⚠️ Получен сигнал завершения работы...")
    try:
        # Проверяем состояние планировщика более надежно
        if scheduler:
            try:
                # Проверяем, запущен ли планировщик
                if hasattr(scheduler, 'running') and scheduler.running:
                    print("⏰ Останавливаю планировщик...")
                    scheduler.shutdown(wait=False)
                elif hasattr(scheduler, '_stopped'):
                    # Альтернативная проверка для разных версий APScheduler
                    if not scheduler._stopped:
                        print("⏰ Останавливаю планировщик...")
                        scheduler.shutdown(wait=False)
                    else:
                        print("ℹ️ Планировщик уже остановлен")
                else:
                    print("ℹ️ Планировщик не запущен")
            except AttributeError:
                print("ℹ️ Планировщик в неопределенном состоянии")
            except Exception as e:
                print(f"⚠️ Ошибка при остановке планировщика: {e}")
        else:
            print("ℹ️ Планировщик не инициализирован")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка: {e}")

    print("👋 Завершение работы бота")
    sys.exit(0)


def setup_admin_from_env():
    """Назначение администратора через переменную окружения ADMIN_IDS"""
    try:
        # Получаем список ID администраторов из переменной окружения
        admin_ids_str = os.getenv('ADMIN_IDS', '')

        if not admin_ids_str:
            print("⚠️ Переменная окружения ADMIN_IDS не установлена")
            return False

        # Парсим ID администраторов (могут быть разделены запятыми или пробелами)
        admin_ids = []
        for item in admin_ids_str.replace(',', ' ').split():
            try:
                admin_id = int(item.strip())
                admin_ids.append(admin_id)
            except ValueError:
                print(f"⚠️ Некорректный ID администратора: {item}")

        if not admin_ids:
            print("⚠️ Не удалось распарсить ID администраторов")
            return False

        print(f"👑 Настройка администраторов из переменных окружения: {admin_ids}")

        # Подключаемся к базе данных
        db_path = 'data/users.db'
        if not os.path.exists(db_path):
            print(f"❌ База данных не найдена: {db_path}")
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
                    print(f"✅ Пользователь {admin_id} назначен администратором")
                else:
                    # Создаем нового пользователя как администратора
                    cursor.execute('''
                    INSERT INTO users (telegram_id, is_admin, registration_date, last_activity)
                    VALUES (?, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ''', (admin_id,))
                    print(f"✅ Создан новый пользователь {admin_id} с правами администратора")

                updated_count += 1

            except sqlite3.Error as e:
                print(f"❌ Ошибка при назначении администратора {admin_id}: {e}")

        conn.commit()
        conn.close()

        print(f"✅ Успешно настроено {updated_count} администраторов")
        return True

    except Exception as e:
        print(f"❌ Ошибка при настройке администраторов: {e}")
        return False


# ============================================================================
# ФУНКЦИЯ ДЛЯ ОДНОРАЗОВОГО ВЫПОЛНЕНИЯ ПРИ ЗАПУСКЕ
# ============================================================================

def run_startup_tasks():
    """Задачи, выполняемые один раз при запуске бота"""

    # Назначение администраторов из переменных окружения
    if setup_admin_from_env():
        print("✅ Назначение администраторов выполнено успешно")
    else:
        print("⚠️ Назначение администраторов не выполнено")

    if setup_bot_commands():
        print("✅ Меню команд бота настроено")
    else:
        print("⚠️ Не удалось настроить меню команд бота")


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("🚀 Запуск бота...")
    logger.info("=" * 50)

    # Выполняем стартовые задачи
    run_startup_tasks()

    # Логирование загрузки вопросов
    logger.info("📂 Загрузка вопросов...")
    check_and_load_questions()

    # Логирование запуска планировщика
    logger.info("⏰ Настройка планировщика...")
    # Запускаем планировщик
    setup_scheduler()

    # Настраиваем обработчики сигналов
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    atexit.register(shutdown_handler)

    logger.info("✅ Все системы запущены. Ожидание сообщений...")
    logger.info("=" * 50)

    # Запускаем бота
    try:
        bot.polling(none_stop=True, interval=1, timeout=30)
    except Exception as e:
        print(f"❌ Ошибка при запуске бота: {e}")
        traceback.print_exc()

        # Останавливаем планировщик при ошибке
        if scheduler:
            scheduler.shutdown()