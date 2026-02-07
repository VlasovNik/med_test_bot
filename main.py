import os
from dotenv import load_dotenv
import random
import time
import re
import telebot
from telebot import types
from collections import defaultdict

load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)
user_data = {}
questions_by_topic = {}
topics_list = []
questions_loaded = False

def wait_for_questions_file(filename, check_interval=15):
    """
    Ожидает появления файла с вопросами
    """
    print(f"⏳ Ожидание файла '{filename}'...")
    
    while not os.path.exists(filename):
        print(f"Файл '{filename}' не найден. Повторная проверка через {check_interval} секунд...")
        time.sleep(check_interval)
    
    print(f"✅ Файл '{filename}' найден!")
    return True

def load_and_parse_questions(filename):
    """
    Загружает вопросы из файла и группирует их по темам
    """
    try:
        # Проверяем существование файла
        if not os.path.exists(filename):
            print(f"❌ Файл '{filename}' не найден!")
            return False

        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Очищаем предыдущие данные
        questions_by_topic.clear()
        topics_list.clear()

        current_topic = None
        current_question_number = None
        current_question_text = None
        current_answers = []
        in_question = False

        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            # Проверяем, является ли строка заголовком темы
            if line.startswith('МДК'):
                # Это новая тема
                if current_topic and current_question_number and current_question_text and current_answers:
                    # Сохраняем последний вопрос предыдущей темы
                    full_question = f"{current_question_number}. {current_question_text}"
                    if current_topic not in questions_by_topic:
                        questions_by_topic[current_topic] = []
                    questions_by_topic[current_topic].append({
                        'question': full_question,
                        'answers': current_answers
                    })

                # Начинаем новую тему
                current_topic = line
                if current_topic not in questions_by_topic:
                    questions_by_topic[current_topic] = []
                    topics_list.append(current_topic)

                current_question_number = None
                current_question_text = None
                current_answers = []
                in_question = False
                continue

            # Проверяем, является ли строка номером вопроса
            if re.match(r'^\d+\.', line):
                # Это номер вопроса
                if current_question_number and current_question_text and current_answers:
                    # Сохраняем предыдущий вопрос
                    full_question = f"{current_question_number}. {current_question_text}"
                    questions_by_topic[current_topic].append({
                        'question': full_question,
                        'answers': current_answers
                    })

                # Извлекаем номер вопроса
                match = re.match(r'^(\d+)\.', line)
                if match:
                    current_question_number = match.group(1)
                    # Берем текст вопроса из следующей строки
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        # Если следующая строка не начинается с + или - и не пустая
                        if (next_line and not next_line.startswith('+')
                            and not next_line.startswith('-')
                            and not next_line.startswith('МДК')
                            and not re.match(r'^\d+\.', next_line)):
                            current_question_text = next_line
                        else:
                            # Если вопроса нет на следующей строке, используем текст после номера
                            question_text = line[len(match.group(0)):].strip()
                            current_question_text = question_text if question_text else "Вопрос не указан"
                    else:
                        # Если это последняя строка
                        question_text = line[len(match.group(0)):].strip()
                        current_question_text = question_text if question_text else "Вопрос не указан"

                current_answers = []
                in_question = True
                continue

            # Проверяем, является ли строка ответом
            if in_question and (line.startswith('+') or line.startswith('-')):
                is_correct = line.startswith('+')
                # Убираем знак + или - и следующий пробел если есть
                answer_text = line[1:].strip() if line[1] == ' ' else line[2:].strip()

                # Очищаем ответ от лишних пробелов
                answer_text = ' '.join(answer_text.split())

                current_answers.append({
                    'text': answer_text,
                    'correct': is_correct
                })

        # Не забываем сохранить последний вопрос
        if current_topic and current_question_number and current_question_text and current_answers:
            full_question = f"{current_question_number}. {current_question_text}"
            if current_topic not in questions_by_topic:
                questions_by_topic[current_topic] = []
            questions_by_topic[current_topic].append({
                'question': full_question,
                'answers': current_answers
            })

        # Отладочная информация
        print(f"✅ Загружено {len(topics_list)} тем:")
        for topic in topics_list:
            print(f"  - {topic}: {len(questions_by_topic[topic])} вопросов")

        # Добавляем опцию "Все темы"
        topics_list.append("🎲 Все темы (рандом)")

        return True

    except Exception as e:
        print(f"❌ Ошибка при загрузке вопросов: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_random_question_from_topic(topic_name):
    """
    Возвращает случайный вопрос из указанной темы
    """
    if topic_name == "🎲 Все темы (рандом)":
        # Собираем все вопросы из всех тем
        all_questions = []
        for topic in questions_by_topic.keys():
            all_questions.extend(questions_by_topic[topic])

        if not all_questions:
            print("❌ Нет вопросов для выбора!")
            return None

        return random.choice(all_questions)
    elif topic_name in questions_by_topic:
        questions = questions_by_topic[topic_name]
        if questions:
            return random.choice(questions)
        else:
            print(f"❌ В теме '{topic_name}' нет вопросов!")

    print(f"❌ Тема '{topic_name}' не найдена!")
    return None

def check_and_load_questions():
    """
    Проверяет и загружает вопросы из файла
    """
    global questions_loaded
    
    if os.path.exists('тест.txt'):
        print("📂 Файл 'тест.txt' найден. Загружаю вопросы...")
        questions_loaded = load_and_parse_questions('тест.txt')
        print(f"📊 Загрузка завершена: {'✅ Успешно' if questions_loaded else '❌ Ошибка'}")
        return questions_loaded
    else:
        print("❌ Файл 'тест.txt' не найден!")
        return False

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    chat_id = message.chat.id

    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.send_message(
            chat_id,
            "⏳ Вопросы еще не загружены. Пожалуйста, подождите...\n\n"
            "Если файл с вопросами отсутствует, создайте файл 'тест.txt' в папке с ботом."
        )
        return

    # Инициализируем данные пользователя с статистикой
    user_data[chat_id] = {
        'current_topic': None,
        'current_question': None,
        'correct_answer': None,
        'numbered_answers': {},
        'answers_list': [],
        'stats': {
            'total_answered': 0,
            'correct_answers': 0,
            'incorrect_answers': 0,
            'start_time': None
        }
    }

    # Проверяем загружены ли вопросы
    if not topics_list:
        bot.send_message(chat_id, "❌ Не удалось загрузить вопросы. Проверьте файл с вопросами.")
        print("❌ Ошибка: вопросы не загружены!")
        return

    # Формируем текст со списком тем
    topics_text = "📚 ДОСТУПНЫЕ ТЕМЫ:\n\n"
    for i, topic in enumerate(topics_list, 1):
        topics_text += f"{i}. {topic}\n"
    
    topics_text += "\nВыберите номер темы:"

    # Создаем inline клавиатуру только с номерами тем
    markup = types.InlineKeyboardMarkup(row_width=5)
    
    # Создаем кнопки с номерами тем
    buttons = []
    for i in range(1, len(topics_list) + 1):
        buttons.append(types.InlineKeyboardButton(
            text=str(i),
            callback_data=f"topic_{i-1}"
        ))
    
    # Добавляем кнопки в несколько строк по 5 в каждой
    for i in range(0, len(buttons), 5):
        markup.row(*buttons[i:i+5])

    # Добавляем кнопку для выхода
    markup.row(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))

    welcome_text = f"""
👋 Привет! Я бот для подготовки к тестам.

📚 Загружено {len(topics_list)-1} тем.

🎯 Выберите тему для начала тренировки:
    """

    # Отправляем сначала приветственное сообщение
    bot.send_message(chat_id, welcome_text)
    
    # Затем отправляем отдельное сообщение со списком тем и кнопками
    bot.send_message(chat_id, topics_text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('topic_'))
def select_topic_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    try:
        # Проверяем загружены ли вопросы
        global questions_loaded
        if not questions_loaded:
            bot.answer_callback_query(call.id, "❌ Вопросы еще не загружены!")
            return

        # Извлекаем номер темы из callback_data
        topic_num = int(call.data.split('_')[1])

        if 0 <= topic_num < len(topics_list):
            selected_topic = topics_list[topic_num]

            # Сохраняем выбранную тему
            user_data[chat_id]['current_topic'] = selected_topic
            user_data[chat_id]['current_question'] = None
            user_data[chat_id]['correct_answer'] = None
            user_data[chat_id]['numbered_answers'] = {}
            user_data[chat_id]['answers_list'] = []

            # Инициализируем статистику если ее нет
            if 'stats' not in user_data[chat_id]:
                user_data[chat_id]['stats'] = {
                    'total_answered': 0,
                    'correct_answers': 0,
                    'incorrect_answers': 0,
                    'start_time': None
                }

            # Проверяем, есть ли вопросы в этой теме
            if selected_topic == "🎲 Все темы (рандом)":
                topic_questions_count = sum(len(q) for q in questions_by_topic.values())
            elif selected_topic in questions_by_topic:
                topic_questions_count = len(questions_by_topic[selected_topic])
            else:
                topic_questions_count = 0

            if topic_questions_count == 0:
                bot.answer_callback_query(call.id, f"❌ В теме '{selected_topic}' нет вопросов.")
                return

            # Обновляем сообщение с информацией о выбранной теме
            topic_info = f"""
✅ Выбрана тема: {selected_topic}
📊 Вопросов в теме: {topic_questions_count}
            """

            # Добавляем статистику если есть
            stats = user_data[chat_id]['stats']
            if stats['total_answered'] > 0:
                correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100
                topic_info += f"\n📈 Ваша статистика: {stats['correct_answers']}/{stats['total_answered']} ({correct_percentage:.1f}%)"

            topic_info += "\n\nНажмите кнопку ниже, чтобы получить вопрос 🎲"

            # Создаем inline кнопку для получения вопроса
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("🎲 Получить вопрос", callback_data="get_question"),
                types.InlineKeyboardButton("📊 Показать статистику", callback_data="show_stats"),
                types.InlineKeyboardButton("🔄 Выбрать другую тему", callback_data="change_topic")
            )

            # Редактируем исходное сообщение
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=topic_info,
                reply_markup=markup
            )

            bot.answer_callback_query(call.id, f"Выбрана тема: {selected_topic}")
        else:
            bot.answer_callback_query(call.id, "❌ Неверный номер темы.")

    except (ValueError, IndexError) as e:
        bot.answer_callback_query(call.id, "❌ Ошибка при выборе темы.")
        print(f"❌ Ошибка выбора темы: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "cancel")
def cancel_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text="❌ Выбор темы отменен. Нажмите /start для начала.",
    )
    bot.answer_callback_query(call.id, "Отменено")

@bot.callback_query_handler(func=lambda call: call.data == "get_question")
def get_question_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    send_question_inline(chat_id, message_id)

def send_question_inline(chat_id, message_id):
    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="❌ Вопросы еще не загружены. Пожалуйста, подождите...",
        )
        return

    if chat_id not in user_data or not user_data[chat_id]['current_topic']:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ Сначала выберите тему! Нажмите /start",
        )
        return

    topic = user_data[chat_id]['current_topic']

    # Получаем случайный вопрос из темы
    question_data = get_random_question_from_topic(topic)

    if not question_data:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Не удалось получить вопрос из темы '{topic}'.\nПопробуйте выбрать другую тему."
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

    # Создаем текст вопросов с вариантами ответов
    topic_display = topic
    question_text = f"📚 Тема: {topic_display}\n\n"

    # Добавляем статистику в заголовок если есть
    stats = user_data[chat_id]['stats']
    if stats['total_answered'] > 0:
        correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100
        question_text += f"📈 Статистика: {stats['correct_answers']}/{stats['total_answered']} ({correct_percentage:.1f}%)\n\n"

    # Форматируем текст вопроса
    q_text = question_data['question']
    question_text += f"❓ {q_text}\n\n"
    
    # Добавляем варианты ответов в текст сообщения
    question_text += "📋 Варианты ответов:\n"
    for answer_line in answers_texts:
        question_text += f"{answer_line}\n"
    
    question_text += "\nВыберите номер правильного ответа:"

    # Создаем inline клавиатуру только с номерами ответов
    markup = types.InlineKeyboardMarkup(row_width=4)
    
    # Создаем кнопки с номерами ответов
    buttons = []
    for i in range(1, len(answers) + 1):
        buttons.append(types.InlineKeyboardButton(
            text=str(i),
            callback_data=f"answer_{i}"
        ))
    
    # Добавляем кнопки в несколько строк по 4 в каждой
    for i in range(0, len(buttons), 4):
        markup.row(*buttons[i:i+4])

    # Добавляем дополнительные кнопки
    markup.row(
        types.InlineKeyboardButton("📊 Статистика", callback_data="show_stats"),
        types.InlineKeyboardButton("🔄 Другой вопрос", callback_data="get_question")
    )
    markup.row(
        types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic"),
        types.InlineKeyboardButton("❌ Завершить", callback_data="end_session")
    )

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=question_text,
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('answer_'))
def check_answer_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.answer_callback_query(call.id, "❌ Вопросы еще не загружены!")
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

        # Обновляем статистику
        if 'stats' not in user_data[chat_id]:
            user_data[chat_id]['stats'] = {
                'total_answered': 0,
                'correct_answers': 0,
                'incorrect_answers': 0,
                'start_time': None
            }

        user_data[chat_id]['stats']['total_answered'] += 1

        # Проверяем ответ
        if selected_answer in correct_answers:
            user_data[chat_id]['stats']['correct_answers'] += 1
            is_correct = True
        else:
            user_data[chat_id]['stats']['incorrect_answers'] += 1
            is_correct = False

        # Создаем текст результата
        result_text = ""
        if is_correct:
            result_text += "✅ Правильно!\n\n"
        else:
            result_text += f"❌ Неправильно!\nВы выбрали: {selected_answer}\n\n"

        # Показываем правильный ответ
        if correct_answers:
            if len(correct_answers) == 1:
                result_text += f"📖 Правильный ответ: {correct_answers[0]}"
            else:
                result_text += "📖 Правильные ответы:\n"
                for i, ans in enumerate(correct_answers, 1):
                    result_text += f"{i}. {ans}\n"

        # Добавляем текущую статистику
        stats = user_data[chat_id]['stats']
        correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100 if stats['total_answered'] > 0 else 0
        result_text += f"\n📊 Текущая статистика: {stats['correct_answers']}/{stats['total_answered']} ({correct_percentage:.1f}%)"

        # Создаем кнопку для следующего вопроса
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("➡️ Следующий вопрос", callback_data="get_question"),
            types.InlineKeyboardButton("📊 Статистика", callback_data="show_stats")
        )
        markup.add(
            types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic"),
            types.InlineKeyboardButton("❌ Завершить", callback_data="end_session")
        )

        # Показываем результат и обновляем сообщение
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=result_text,
            reply_markup=markup
        )

        # Отправляем уведомление
        if is_correct:
            bot.answer_callback_query(call.id, "✅ Правильно!")
        else:
            bot.answer_callback_query(call.id, "❌ Неправильно!")

    except (ValueError, IndexError) as e:
        bot.answer_callback_query(call.id, "❌ Ошибка при обработке ответа.")
        print(f"❌ Ошибка обработки ответа: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "show_stats")
def show_stats_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.answer_callback_query(call.id, "❌ Вопросы еще не загружены!")
        return

    if chat_id not in user_data or 'stats' not in user_data[chat_id]:
        stats_text = "📊 Статистика еще не собрана. Начните отвечать на вопросы!"
    else:
        stats = user_data[chat_id]['stats']

        if stats['total_answered'] == 0:
            stats_text = "📊 Вы еще не ответили ни на один вопрос."
        else:
            # Рассчитываем проценты
            correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100

            stats_text = f"""
📊 ВАША СТАТИСТИКА

📈 Всего отвечено вопросов: {stats['total_answered']}
✅ Правильных ответов: {stats['correct_answers']}
❌ Неправильных ответов: {stats['incorrect_answers']}
🎯 Процент правильных ответов: {correct_percentage:.1f}%

{'⭐ Отличный результат!' if correct_percentage >= 80 else
  '👍 Хороший результат!' if correct_percentage >= 60 else
  '📚 Продолжайте тренироваться!'}
"""

    # Создаем кнопки для навигации
    markup = types.InlineKeyboardMarkup()

    if user_data.get(chat_id, {}).get('current_topic'):
        markup.add(
            types.InlineKeyboardButton("🎲 Продолжить тренировку", callback_data="get_question"),
            types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic")
        )
    else:
        markup.add(
            types.InlineKeyboardButton("🏠 В главное меню", callback_data="change_topic")
        )

    markup.add(
        types.InlineKeyboardButton("❌ Завершить сессию", callback_data="end_session")
    )

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=stats_text,
        reply_markup=markup
    )

    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "change_topic")
def change_topic_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.answer_callback_query(call.id, "❌ Вопросы еще не загружены!")
        return

    # Формируем текст со списком тем
    topics_text = "📚 ДОСТУПНЫЕ ТЕМЫ:\n\n"
    for i, topic in enumerate(topics_list, 1):
        topics_text += f"{i}. {topic}\n"
    
    topics_text += "\nВыберите номер темы:"

    # Создаем inline клавиатуру только с номерами тем
    markup = types.InlineKeyboardMarkup(row_width=5)
    
    # Создаем кнопки с номерами тем
    buttons = []
    for i in range(1, len(topics_list) + 1):
        buttons.append(types.InlineKeyboardButton(
            text=str(i),
            callback_data=f"topic_{i-1}"
        ))
    
    # Добавляем кнопки в несколько строк по 5 в каждой
    for i in range(0, len(buttons), 5):
        markup.row(*buttons[i:i+5])

    # Добавляем кнопку для отмены
    markup.row(types.InlineKeyboardButton("↩️ Назад", callback_data="back"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=topics_text,
        reply_markup=markup
    )

    bot.answer_callback_query(call.id, "Выберите тему")

@bot.callback_query_handler(func=lambda call: call.data == "back")
def back_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    if chat_id not in user_data or not user_data[chat_id]['current_topic']:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ Сначала выберите тему! Нажмите /start",
        )
        return
    
    topic = user_data[chat_id]['current_topic']
    topic_info = f"""
✅ Текущая тема: {topic}
    """

    # Добавляем статистику если есть
    stats = user_data[chat_id]['stats']
    if stats['total_answered'] > 0:
        correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100
        topic_info += f"\n📈 Ваша статистика: {stats['correct_answers']}/{stats['total_answered']} ({correct_percentage:.1f}%)"

    topic_info += "\n\nНажмите кнопку ниже, чтобы получить вопрос 🎲"

    # Создаем inline кнопку для получения вопроса
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🎲 Получить вопрос", callback_data="get_question"),
        types.InlineKeyboardButton("📊 Показать статистику", callback_data="show_stats"),
        types.InlineKeyboardButton("🔄 Выбрать другую тему", callback_data="change_topic")
    )

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=topic_info,
        reply_markup=markup
    )
    
    bot.answer_callback_query(call.id, "Возврат")

@bot.callback_query_handler(func=lambda call: call.data == "end_session")
def end_session_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Получаем статистику перед очисткой
    stats_text = ""
    if chat_id in user_data and 'stats' in user_data[chat_id]:
        stats = user_data[chat_id]['stats']
        if stats['total_answered'] > 0:
            correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100
            stats_text = f"""

📊 ИТОГОВАЯ СТАТИСТИКА СЕССИИ:

📈 Всего отвечено вопросов: {stats['total_answered']}
✅ Правильных ответов: {stats['correct_answers']}
❌ Неправильных ответов: {stats['incorrect_answers']}
🎯 Процент правильных ответов: {correct_percentage:.1f}%

{'🏆 Отличная работа! Продолжайте в том же духе!' if correct_percentage >= 80 else
  '👍 Хорошо поработали! Есть куда стремиться!' if correct_percentage >= 60 else
  '📚 Нужно больше практики! Возвращайтесь для тренировки!'}
"""

    # Очищаем данные пользователя
    if chat_id in user_data:
        user_data.pop(chat_id, None)

    end_message = f"✅ Сессия завершена.{stats_text}\n\nНажмите /start для начала новой сессии."

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🚀 Начать новую сессию", callback_data="new_session"))

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=end_message,
        reply_markup=markup
    )

    bot.answer_callback_query(call.id, "Сессия завершена")

@bot.callback_query_handler(func=lambda call: call.data == "new_session")
def new_session_callback(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.answer_callback_query(call.id, "❌ Вопросы еще не загружены!")
        return

    # Инициализируем новую сессию
    user_data[chat_id] = {
        'current_topic': None,
        'current_question': None,
        'correct_answer': None,
        'numbered_answers': {},
        'answers_list': [],
        'stats': {
            'total_answered': 0,
            'correct_answers': 0,
            'incorrect_answers': 0,
            'start_time': None
        }
    }

    # Формируем текст со списком тем
    topics_text = "📚 ДОСТУПНЫЕ ТЕМЫ:\n\n"
    for i, topic in enumerate(topics_list, 1):
        topics_text += f"{i}. {topic}\n"
    
    topics_text += "\nВыберите номер темы:"

    # Создаем inline клавиатуру только с номерами тем
    markup = types.InlineKeyboardMarkup(row_width=5)
    
    # Создаем кнопки с номерами тем
    buttons = []
    for i in range(1, len(topics_list) + 1):
        buttons.append(types.InlineKeyboardButton(
            text=str(i),
            callback_data=f"topic_{i-1}"
        ))
    
    # Добавляем кнопки в несколько строк по 5 в каждой
    for i in range(0, len(buttons), 5):
        markup.row(*buttons[i:i+5])

    # Добавляем кнопку для выхода
    markup.row(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))

    welcome_text = f"""
👋 Новая сессия начата!

📚 Загружено {len(topics_list)-1} тем.

🎯 Выберите тему для начала тренировки:
    """

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=welcome_text + "\n\n" + topics_text,
        reply_markup=markup
    )

    bot.answer_callback_query(call.id, "Новая сессия начата")

# Обработчики для текстовых команд (на всякий случай)
@bot.message_handler(func=lambda message: message.text == "/stats" or message.text == "📊 Статистика")
def show_stats_message(message):
    chat_id = message.chat.id

    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.send_message(chat_id, "❌ Вопросы еще не загружены!")
        return

    if chat_id not in user_data or 'stats' not in user_data[chat_id]:
        bot.send_message(chat_id, "📊 Статистика еще не собрана. Начните отвечать на вопросы!")
        return

    stats = user_data[chat_id]['stats']

    if stats['total_answered'] == 0:
        bot.send_message(chat_id, "📊 Вы еще не ответили ни на один вопрос.")
        return

    # Рассчитываем проценты
    correct_percentage = (stats['correct_answers'] / stats['total_answered']) * 100

    stat_text = f"""
📊 ВАША СТАТИСТИКА

📈 Всего отвечено вопросов: {stats['total_answered']}
✅ Правильных ответов: {stats['correct_answers']}
❌ Неправильных ответов: {stats['incorrect_answers']}
🎯 Процент правильных ответов: {correct_percentage:.1f}%

{'⭐ Отличный результат!' if correct_percentage >= 80 else
  '👍 Хороший результат!' if correct_percentage >= 60 else
  '📚 Продолжайте тренироваться!'}
"""

    markup = types.InlineKeyboardMarkup()
    if user_data[chat_id].get('current_topic'):
        markup.add(
            types.InlineKeyboardButton("🎲 Продолжить тренировку", callback_data="get_question"),
            types.InlineKeyboardButton("📚 Сменить тему", callback_data="change_topic")
        )
    else:
        markup.add(
            types.InlineKeyboardButton("🏠 В главное меню", callback_data="change_topic")
        )

    bot.send_message(chat_id, stat_text, reply_markup=markup)

@bot.message_handler(commands=['stop'])
def stop_command(message):
    chat_id = message.chat.id
    end_session_callback(type('Callback', (), {'message': type('Message', (), {'chat': type('Chat', (), {'id': chat_id}), 'message_id': None})()})())

@bot.message_handler(commands=['reload'])
def reload_questions_command(message):
    """
    Команда для ручной перезагрузки вопросов
    """
    chat_id = message.chat.id
    
    bot.send_message(chat_id, "🔄 Перезагружаю вопросы из файла...")
    
    global questions_loaded
    questions_loaded = check_and_load_questions()
    
    if questions_loaded:
        bot.send_message(chat_id, f"✅ Вопросы успешно перезагружены!\nЗагружено тем: {len(topics_list)-1}")
    else:
        bot.send_message(chat_id, "❌ Не удалось загрузить вопросы. Проверьте файл 'тест.txt'")

@bot.message_handler(func=lambda message: True)
def handle_other_messages(message):
    """
    Обработчик для всех остальных сообщений
    """
    chat_id = message.chat.id
    
    # Проверяем загружены ли вопросы
    global questions_loaded
    if not questions_loaded:
        bot.send_message(
            chat_id,
            "⏳ Вопросы еще не загружены. Бот ожидает файл 'тест.txt'...\n\n"
            "Создайте файл 'тест.txt' в папке с ботом или используйте команду /reload для загрузки."
        )
        return
    
    # Если вопросы загружены, отправляем приглашение
    bot.send_message(chat_id, "Для начала работы используйте команду /start")

if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 Запуск бота...")
    print("="*50)
    
    # Пытаемся загрузить вопросы при запуске
    if os.path.exists('тест.txt'):
        print("📂 Файл 'тест.txt' найден. Загружаю вопросы...")
        questions_loaded = check_and_load_questions()
    else:
        print("❌ Файл 'тест.txt' не найден!")
        print("⏳ Запускаю бота в режиме ожидания файла...")
        print("ℹ️ Бот будет работать, но вопросы будут недоступны до загрузки файла")
        print("ℹ️ Создайте файл 'тест.txt' в папке с ботом и используйте команду /reload")
    
    print("\n" + "="*50)
    print("🤖 Бот запущен. Ожидание сообщений...")
    
    if questions_loaded and topics_list:
        print("\n✅ Доступные темы:")
        for i, topic in enumerate(topics_list, 1):
            print(f"{i}. {topic}")
        print("="*50)
    
    # Запускаем бота
    try:
        bot.polling(none_stop=True, interval=0)
    except Exception as e:
        print(f"❌ Ошибка при запуске бота: {e}")
        print("="*50)