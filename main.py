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

# Загрузка переменных окружения
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# КОНСТАНТЫ И КОНФИГУРАЦИЯ
# ============================================================================
TOKEN = os.getenv('BOT_TOKEN')
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения!")

bot = telebot.TeleBot(TOKEN)
NOVOSIBIRSK_TZ = pytz_timezone('Asia/Novosibirsk')

# Глобальные переменные
questions_by_topic = {}
topics_list = []
questions_loaded = False
session_stats = {}
user_data = {}
scheduler = None
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
        """Обновление подписки с точным временем окончания"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            start_datetime = datetime.now()

            if not end_datetime:
                if is_trial:
                    # Пробная подписка: 1 день от текущего момента
                    end_datetime = datetime.now() + timedelta(days=1)
                else:
                    # Обычная подписка: 30 дней от текущего момента
                    end_datetime = datetime.now() + timedelta(days=30)

            # Форматируем даты в строки для базы данных
            start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

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
            print(f"✅ Подписка пользователя {telegram_id} обновлена до {end_str}")
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
# ОСНОВНЫЕ ОБРАБОТЧИКИ СООБЩЕНИЙ (ВКЛЮЧАЯ АДМИНИСТРАТИВНЫЕ)
# ============================================================================


@bot.message_handler(commands=['help'])
def handle_help(message):
    """Обработчик команды /help"""
    chat_id = message.chat.id

    help_text = """
🆘 <b>Доступные команды:</b>

/start - Главное меню
/help - Эта справка
/stats - Ваша статистика
/myinfo - Информация о вас

📞 <b>Поддержка:</b> @ZlotaR
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
        types.InlineKeyboardButton("👑 Назначить админа", callback_data="admin_grant_admin")
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
    time.sleep(0.1)  # Небольшая задержка

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

    # Небольшая задержка для предотвращения двойного срабатывания
    time.sleep(0.1)

    send_question_inline(chat_id, message_id)


def subscribe_info_callback(call):
    """Обработчик информации о подписке с точным временем"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    user = db.get_user(chat_id)
    has_subscription = db.check_subscription(chat_id)

    if has_subscription and user and user.get('subscription_end_date'):
        try:
            # Пытаемся парсить с точным временем
            end_datetime = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
            end_str = end_datetime.strftime("%d.%m.%Y в %H:%M")

            # Рассчитываем оставшееся время
            time_left = end_datetime - datetime.now()

            if time_left.total_seconds() > 0:
                days = time_left.days
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60

                if days > 0:
                    time_left_str = f"{days} дн. {hours} ч."
                elif hours > 0:
                    time_left_str = f"{hours} ч. {minutes} мин."
                else:
                    time_left_str = f"{minutes} мин."

                status_text = f"✅ <b>Подписка активна</b>\nДействует до: {end_str}\nОсталось: {time_left_str}"
            else:
                status_text = "❌ <b>Подписка истекла</b>"

        except ValueError:
            # Если старый формат (только дата)
            try:
                end_date = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d').date()
                end_str = end_date.strftime("%d.%m.%Y")
                days_left = (end_date - datetime.now().date()).days

                if days_left > 0:
                    status_text = f"✅ <b>Подписка активна</b>\nДействует до: {end_str}\nОсталось дней: {days_left}"
                else:
                    status_text = "❌ <b>Подписка истекла</b>"
            except:
                status_text = "✅ <b>Подписка активна</b>"
    else:
        status_text = "❌ <b>Подписка не активна</b>"

    markup = types.InlineKeyboardMarkup()
    if not has_subscription:
        markup.add(
            types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"),
            types.InlineKeyboardButton("🎁 Пробный доступ", callback_data="trial")
        )
    markup.add(types.InlineKeyboardButton("📋 Условия подписки", callback_data="subscription_terms"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="main_menu"))

    info_text = f"""
💳 <b>Информация о подписке</b>

{status_text}

📋 <b>Тарифы:</b>
• 1 месяц - 299₽
• 3 месяца - 807₽ (скидка 10%)
• 6 месяцев - 1435₽ (скидка 20%)

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
    """Обработчик оформления подписки"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("💳 1 месяц - 299₽", callback_data="pay_1month"),
        types.InlineKeyboardButton("💳 3 месяца - 807₽", callback_data="pay_3months")
    )
    markup.add(
        types.InlineKeyboardButton("💳 6 месяцев - 1435₽", callback_data="pay_6months"),
        types.InlineKeyboardButton("📋 Инструкция по оплате", callback_data="payment_instructions")
    )
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe_info"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="💳 <b>Оформление подписки</b>\n\nВыберите тариф:",
        parse_mode='HTML',
        reply_markup=markup
    )
    bot.answer_callback_query(call.id)


def trial_callback(call):
    """Обработчик пробного доступа с точным временем"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем, использовал ли уже пробный доступ
    user = db.get_user(chat_id)
    if user and user.get('is_trial_used'):
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

👨‍💻 <b>Разработчик:</b> Ваша команда
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
• Обратитесь в поддержку @your_support
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
    elif call.data == "admin_grant_admin":
        admin_grant_admin_callback(call)
    elif call.data == "admin_logs":
        admin_logs_callback(call)
    elif call.data == "admin_restart":
        admin_restart_callback(call)
    elif call.data == "admin_db":
        admin_db_callback(call)
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
        types.InlineKeyboardButton("👑 Назначить админа", callback_data="admin_grant_admin")
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
    """Условия подписки"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    terms_text = """
📋 <b>Условия подписки</b>

✅ <b>Что входит в подписку:</b>
• Полный доступ ко всем темам
• Неограниченное количество вопросов
• Статистика ответов
• Поддержка 24/7
• Регулярное обновление базы вопросов

⏱️ <b>Срок действия:</b>
• Подписка активируется после оплаты
• Действует 30 дней с момента активации
• Автопродление не предусмотрено

💰 <b>Стоимость:</b>
• 1 месяц - 299₽
• 3 месяца - 807₽ (экономия 90₽)
• 6 месяцев - 1435₽ (экономия 359₽)

🔄 <b>Возврат средств:</b>
• Возврат возможен в течение 24 часов после оплаты
• Для возврата обратитесь в поддержку

📞 <b>Контакты поддержки:</b>
• Telegram: @your_support
• Email: support@example.com
• Ответ в течение 24 часов
    """

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Оформить подписку", callback_data="subscribe"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe_info"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=terms_text,
        parse_mode='HTML',
        reply_markup=markup
    )
    bot.answer_callback_query(call.id)


def pay_callback(call):
    """Оплата подписки"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    plan = call.data.split('_')[1]
    plans = {
        '1month': {'price': 299, 'days': 30, 'name': '1 месяц'},
        '3months': {'price': 807, 'days': 90, 'name': '3 месяца'},
        '6months': {'price': 1435, 'days': 180, 'name': '6 месяцев'}
    }

    if plan not in plans:
        bot.answer_callback_query(call.id, "❌ Неверный тариф!")
        return

    plan_info = plans[plan]

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Перейти к оплате", url=f"https://your_payment_link.com?plan={plan}"))
    markup.add(types.InlineKeyboardButton("📞 Поддержка", url="https://t.me/ZlotaR"))
    markup.add(types.InlineKeyboardButton("↩️ Назад", callback_data="subscribe"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"💳 <b>Оплата подписки: {plan_info['name']}</b>\n\nСумма: {plan_info['price']}₽\nСрок: {plan_info['days']} дней",
        parse_mode='HTML',
        reply_markup=markup
    )
    bot.answer_callback_query(call.id)


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
        log_file = 'bot.log'
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
        log_file = 'bot.log'
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
        log_file = 'bot.log'
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
        log_file = 'bot.log'
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

        # Добавляем задержку для предотвращения двойных нажатий
        time.sleep(0.05)

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
        elif call.data.startswith('pay_'):
            pay_callback(call)
        elif call.data == "payment_instructions":
            payment_instructions_callback(call)
        # Добавляем обработку админских callback-ов
        elif any(call.data.startswith(prefix) for prefix in ['admin_', 'logs_', 'restart_', 'back_to_admin']):
            handle_admin_callback(call)
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
    print("\n⚠️ Получен сигнал завершения работы...")

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
    print("=" * 50)
    print("🚀 Выполнение стартовых задач...")
    print("=" * 50)

    # Назначение администраторов из переменных окружения
    if setup_admin_from_env():
        print("✅ Назначение администраторов выполнено успешно")
    else:
        print("⚠️ Назначение администраторов не выполнено")

    # Здесь можно добавить другие стартовые задачи
    # Например, проверку структуры базы данных, создание необходимых таблиц и т.д.

    print("=" * 50)
    print("✅ Стартовые задачи выполнены")
    print("=" * 50)


if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Запуск бота...")
    print("=" * 50)

    # Выполняем стартовые задачи
    run_startup_tasks()
    # Загружаем вопросы
    check_and_load_questions()

    # Запускаем планировщик
    setup_scheduler()

    # Настраиваем обработчики сигналов
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    atexit.register(shutdown_handler)

    print("\n✅ Все системы запущены. Ожидание сообщений...")
    print("=" * 50)

    # Запускаем бота
    try:
        bot.polling(none_stop=True, interval=1, timeout=30)
    except Exception as e:
        print(f"❌ Ошибка при запуске бота: {e}")
        traceback.print_exc()

        # Останавливаем планировщик при ошибке
        if scheduler:
            scheduler.shutdown()