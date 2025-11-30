import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.utils import get_random_id
import sqlite3
import datetime
import logging
import os
import sys
import re
import time
import shutil
from threading import Thread, Lock
from flask import Flask
import json

# ========== НАСТРОЙКИ ==========
GROUP_TOKEN = os.environ.get('VK_TOKEN')
GROUP_ID = os.environ.get('VK_GROUP_ID')
PASSWORD = "050607"

# Проверяем обязательные настройки
if not GROUP_TOKEN or not GROUP_ID:
    print("❌ ОШИБКА: Не установлены переменные окружения VK_TOKEN и VK_GROUP_ID!")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ========== БАЗА ДАННЫХ ==========
db_lock = Lock()

def init_database():
    """Инициализация базы данных SQLite"""
    try:
        with db_lock:
            conn = sqlite3.connect('bot_database.db', check_same_thread=False, timeout=30)
            conn.execute('PRAGMA journal_mode=WAL')
            cursor = conn.cursor()
            
            # Таблица для заявок от родителей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    user_name TEXT,
                    group_name TEXT NOT NULL,
                    date TEXT NOT NULL,
                    child_name TEXT NOT NULL,
                    video_attachment TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица для настроек пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER PRIMARY KEY,
                    use_bot BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON submissions(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON submissions(user_id)')
            
            conn.commit()
            logger.info("✅ База данных инициализирована")
            return conn
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        sys.exit(1)

# Инициализируем базу данных
db_connection = init_database()

def save_submission(user_id, user_name, group_name, date, child_name, video_attachment):
    """Сохранение заявки в базу данных"""
    try:
        with db_lock:
            cursor = db_connection.cursor()
            cursor.execute('''
                INSERT INTO submissions (user_id, user_name, group_name, date, child_name, video_attachment)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, user_name, group_name, date, child_name, video_attachment))
            db_connection.commit()
            logger.info(f"✅ Сохранена заявка: {child_name}, группа: {group_name}")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения заявки: {e}")
        return False

def get_submissions_by_date(date):
    """Получение всех заявок за определенную дату"""
    try:
        with db_lock:
            cursor = db_connection.cursor()
            cursor.execute('''
                SELECT child_name, video_attachment, group_name, user_name 
                FROM submissions 
                WHERE date = ?
                ORDER BY created_at DESC
            ''', (date,))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"❌ Ошибка получения заявок: {e}")
        return []

def get_user_setting(user_id):
    """Получение настроек пользователя"""
    try:
        with db_lock:
            cursor = db_connection.cursor()
            cursor.execute('SELECT use_bot FROM user_settings WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            return result[0] if result else True
    except Exception as e:
        logger.error(f"❌ Ошибка получения настроек пользователя: {e}")
        return True

def set_user_setting(user_id, use_bot):
    """Сохранение настроек пользователя"""
    try:
        with db_lock:
            cursor = db_connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_settings (user_id, use_bot, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, use_bot))
            db_connection.commit()
            logger.info(f"✅ Настройки пользователя {user_id} обновлены: use_bot={use_bot}")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения настроек: {e}")
        return False

# ========== СИСТЕМА СОСТОЯНИЙ ==========
user_states = {}
state_lock = Lock()

class UserState:
    START = 0
    CHOOSE_MODE = 1
    CHOOSE_ROLE = 2
    PARENT_CHOOSE_GROUP = 3
    PARENT_ENTER_DATE = 4
    PARENT_ENTER_NAME = 5
    PARENT_SEND_VIDEO = 6
    TEACHER_ENTER_PASSWORD = 7
    TEACHER_ENTER_DATE = 8

# Список групп
GROUPS = [
    "Земля", "Альтаир", "Планета", "Комета", "Орион", 
    "Юпитер", "Кассиопея", "Венера", "Аврора", "Вега", 
    "Медведица", "Пегас/альфа/сириус", "Макси"
]

# ========== КЛАВИАТУРЫ ==========
def create_main_menu_keyboard():
    """Главное меню выбора режима"""
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button('🤖 Общаться с ботом', color=VkKeyboardColor.PRIMARY)
    keyboard.add_button('💬 Писать сообщения', color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def create_start_keyboard():
    """Клавиатура для начала работы"""
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button('Старт', color=VkKeyboardColor.POSITIVE)
    keyboard.add_button('⚙️ Настройки', color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def create_settings_keyboard():
    """Клавиатура настроек"""
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button('🤖 Режим бота', color=VkKeyboardColor.POSITIVE)
    keyboard.add_button('💬 Обычные сообщения', color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button('🔙 Назад', color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def create_role_keyboard():
    """Клавиатура выбора роли"""
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button('Родитель', color=VkKeyboardColor.POSITIVE)
    keyboard.add_button('Педагог', color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button('🔙 Назад', color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def create_groups_keyboard():
    """Клавиатура выбора группы"""
    keyboard = VkKeyboard(one_time=True)
    
    # Создаем 2 колонки
    for i in range(0, len(GROUPS), 2):
        keyboard.add_button(GROUPS[i], color=VkKeyboardColor.SECONDARY)
        if i + 1 < len(GROUPS):
            keyboard.add_button(GROUPS[i + 1], color=VkKeyboardColor.SECONDARY)
        if i + 2 < len(GROUPS):
            keyboard.add_line()
    
    keyboard.add_line()
    keyboard.add_button('🔙 Назад', color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def create_restart_keyboard():
    """Клавиатура для перезапуска"""
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button('🔄 Начать ещё раз', color=VkKeyboardColor.POSITIVE)
    keyboard.add_button('⚙️ Настройки', color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def create_teacher_restart_keyboard():
    """Клавиатура для педагога"""
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button('🔄 Рестарт', color=VkKeyboardColor.POSITIVE)
    keyboard.add_button('⚙️ Настройки', color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

# ========== ОСНОВНЫЕ ФУНКЦИИ ==========
def send_message(user_id, message, keyboard=None, attachment=None):
    """Отправка сообщения пользователю"""
    try:
        params = {
            'user_id': user_id,
            'message': message,
            'random_id': get_random_id()
        }
        
        if keyboard:
            params['keyboard'] = keyboard
        if attachment:
            params['attachment'] = attachment
            
        vk.messages.send(**params)
        return True
    except vk_api.exceptions.ApiError as e:
        logger.error(f"❌ Ошибка VK API при отправке сообщения {user_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Общая ошибка отправки сообщения {user_id}: {e}")
        return False

def validate_date(date_text):
    """Проверка корректности даты"""
    try:
        date_obj = datetime.datetime.strptime(date_text, '%d.%m.%Y')
        # Проверяем, что дата не в будущем
        if date_obj.date() > datetime.datetime.now().date():
            return False, "❌ Дата не может быть в будущем!"
        return True, "✅ Дата корректна"
    except ValueError:
        return False, "❌ Неверный формат даты! Используйте 'дд.мм.гггг'"

def validate_name(name_text):
    """Проверка корректности имени"""
    if not name_text or not name_text.strip():
        return False, "❌ Имя не может быть пустым!"
    
    name = name_text.strip()
    if len(name) < 2:
        return False, "❌ Имя слишком короткое!"
    
    if len(name) > 100:
        return False, "❌ Имя слишком длинное!"
    
    # Проверяем на допустимые символы
    if not re.match(r'^[a-zA-Zа-яА-ЯёЁ\s\-]+$', name):
        return False, "❌ Имя содержит недопустимые символы!"
    
    return True, "✅ Имя корректно"

def reset_user_state(user_id):
    """Сброс состояния пользователя"""
    with state_lock:
        if user_id in user_states:
            del user_states[user_id]

def get_user_display_name(user_id, user_info):
    """Получение имени пользователя для логов"""
    try:
        if user_info:
            first_name = user_info.get('first_name', '')
            last_name = user_info.get('last_name', '')
            return f"{first_name} {last_name}".strip() or f"Пользователь {user_id}"
        return f"Пользователь {user_id}"
    except:
        return f"Пользователь {user_id}"

def is_video_attachment(attachments):
    """Проверка, что вложения содержат видео"""
    for attachment in attachments:
        if attachment.get('type') == 'video':
            return True
    return False

def get_video_attachment(attachments):
    """Получение видео из вложений"""
    for attachment in attachments:
        if attachment.get('type') == 'video':
            video_data = attachment['video']
            return f"video{video_data['owner_id']}_{video_data['id']}"
    return None

# ========== ОБРАБОТЧИКИ СОСТОЯНИЙ ==========
def handle_main_menu(user_id, user_info):
    """Обработка главного меню"""
    with state_lock:
        user_states[user_id] = {'state': UserState.CHOOSE_MODE}
    
    send_message(user_id,
                "👋 Добро пожаловать! Выберите режим работы:\n\n"
                "🤖 **Общаться с ботом** - автоматический сбор видео\n"
                "💬 **Писать сообщения** - обычная переписка с администратором",
                create_main_menu_keyboard())

def handle_mode_selection(user_id, text, user_info):
    """Обработка выбора режима"""
    if 'бот' in text.lower():
        set_user_setting(user_id, True)
        handle_bot_start(user_id, user_info)
    elif 'сообщен' in text.lower() or 'писать' in text.lower():
        set_user_setting(user_id, False)
        send_message(user_id,
                    "💬 Вы перешли в режим обычных сообщений.\n"
                    "Теперь ваши сообщения будут приходить администраторам группы.\n\n"
                    "Чтобы вернуться к боту, отправьте любое сообщение или нажмите кнопку:",
                    create_start_keyboard())
        reset_user_state(user_id)
    else:
        send_message(user_id,
                    "❌ Пожалуйста, выберите режим с помощью кнопок:",
                    create_main_menu_keyboard())

def handle_bot_start(user_id, user_info):
    """Начало работы с ботом"""
    with state_lock:
        user_states[user_id] = {'state': UserState.CHOOSE_ROLE}
    
    send_message(user_id,
                "🤖 Режим бота активирован!\n\n"
                "Выберите вашу роль:",
                create_role_keyboard())

def handle_settings(user_id):
    """Обработка настроек"""
    current_mode = get_user_setting(user_id)
    mode_text = "🤖 Режим бота" if current_mode else "💬 Обычные сообщения"
    
    send_message(user_id,
                f"⚙️ **Настройки**\n\n"
                f"Текущий режим: {mode_text}\n\n"
                f"Выберите новый режим:",
                create_settings_keyboard())

def handle_role_selection(user_id, text, user_info):
    """Обработка выбора роли"""
    if text == 'Родитель':
        with state_lock:
            user_states[user_id] = {
                'state': UserState.PARENT_CHOOSE_GROUP,
                'role': 'parent',
                'user_name': get_user_display_name(user_id, user_info)
            }
        send_message(user_id,
                    "👨‍👩‍👧 Выберите группу вашего ребенка:",
                    create_groups_keyboard())
    
    elif text == 'Педагог':
        with state_lock:
            user_states[user_id] = {
                'state': UserState.TEACHER_ENTER_PASSWORD,
                'role': 'teacher'
            }
        send_message(user_id, "👩‍🏫 Введите пароль учётной записи педагога:")
    
    elif text == '🔙 Назад':
        handle_main_menu(user_id, user_info)
    
    else:
        send_message(user_id,
                    "❌ Пожалуйста, выберите роль с помощью кнопок:",
                    create_role_keyboard())

def handle_parent_group(user_id, text):
    """Обработка выбора группы родителем"""
    if text in GROUPS:
        with state_lock:
            user_states[user_id]['group'] = text
            user_states[user_id]['state'] = UserState.PARENT_ENTER_DATE
        
        send_message(user_id,
                    f"✅ Отлично, группа '{text}' выбрана!\n\n"
                    "Теперь напишите дату проведения данного этапа в формате 'дд.мм.гггг' (например, 01.12.2025):")
    
    elif text == '🔙 Назад':
        with state_lock:
            user_states[user_id] = {'state': UserState.CHOOSE_ROLE}
        send_message(user_id, "Выберите вашу роль:", create_role_keyboard())
    
    else:
        send_message(user_id,
                    "❌ Пожалуйста, выберите группу из списка:",
                    create_groups_keyboard())

def handle_parent_date(user_id, text):
    """Обработка даты от родителя"""
    if text == '🔙 Назад':
        with state_lock:
            user_states[user_id]['state'] = UserState.PARENT_CHOOSE_GROUP
        send_message(user_id, "Выберите группу:", create_groups_keyboard())
        return
    
    is_valid, message = validate_date(text)
    if is_valid:
        with state_lock:
            user_states[user_id]['date'] = text
            user_states[user_id]['state'] = UserState.PARENT_ENTER_NAME
        
        send_message(user_id, "📅 Дата сохранена! Теперь укажите имя и фамилию ребёнка:")
    else:
        send_message(user_id, message)

def handle_parent_name(user_id, text):
    """Обработка имени ребенка"""
    if text == '🔙 Назад':
        with state_lock:
            user_states[user_id]['state'] = UserState.PARENT_ENTER_DATE
        send_message(user_id, "Введите дату проведения этапа (дд.мм.гггг):")
        return
    
    is_valid, message = validate_name(text)
    if is_valid:
        with state_lock:
            user_states[user_id]['child_name'] = text.strip()
            user_states[user_id]['state'] = UserState.PARENT_SEND_VIDEO
        
        send_message(user_id,
                    f"👶 Отлично! Имя '{text.strip()}' сохранено.\n\n"
                    "Теперь пришлите видео:")
    else:
        send_message(user_id, message)

def handle_parent_video(user_id, attachments, user_info):
    """Обработка видео от родителя"""
    if not is_video_attachment(attachments):
        send_message(user_id,
                    "❌ Видео не обнаружено. Пожалуйста, прикрепите видеофайл к сообщению.\n\n"
                    "📹 **Как отправить видео:**\n"
                    "1. Нажмите на скрепку 📎\n"
                    "2. Выберите 'Видео'\n"
                    "3. Выберите файл с вашего устройства")
        return

    video_attachment = get_video_attachment(attachments)
    if video_attachment:
        # Сохраняем заявку в базу данных
        with state_lock:
            user_data = user_states[user_id]
        
        success = save_submission(
            user_id=user_id,
            user_name=user_data.get('user_name', ''),
            group_name=user_data['group'],
            date=user_data['date'],
            child_name=user_data['child_name'],
            video_attachment=video_attachment
        )
        
        if success:
            send_message(user_id,
                        "✅ Видео загружено! Спасибо за участие!\n\n"
                        f"📋 **Ваши данные:**\n"
                        f"• Ребёнок: {user_data['child_name']}\n"
                        f"• Группа: {user_data['group']}\n"
                        f"• Дата: {user_data['date']}",
                        create_restart_keyboard())
        else:
            send_message(user_id,
                        "❌ Ошибка сохранения. Попробуйте еще раз.",
                        create_restart_keyboard())
        
        reset_user_state(user_id)
    else:
        send_message(user_id,
                    "❌ Ошибка обработки видео. Попробуйте отправить видео еще раз.")

def handle_teacher_password(user_id, text):
    """Обработка пароля педагога"""
    if text == '🔙 Назад':
        with state_lock:
            user_states[user_id] = {'state': UserState.CHOOSE_ROLE}
        send_message(user_id, "Выберите вашу роль:", create_role_keyboard())
        return
    
    if text == PASSWORD:
        with state_lock:
            user_states[user_id]['state'] = UserState.TEACHER_ENTER_DATE
        
        send_message(user_id,
                    "✅ Успешно! Теперь укажите дату этапа, с которого вы хотите получить материалов "
                    "(в формате дд.мм.гггг):")
    else:
        send_message(user_id,
                    "❌ Неверный пароль. Попробуйте еще раз или вернитесь назад.",
                    create_role_keyboard())

def handle_teacher_date(user_id, text):
    """Обработка даты для педагога"""
    if text == '🔙 Назад':
        with state_lock:
            user_states[user_id]['state'] = UserState.TEACHER_ENTER_PASSWORD
        send_message(user_id, "Введите пароль педагога:")
        return
    
    is_valid, message = validate_date(text)
    if is_valid:
        # Ищем все заявки за указанную дату
        target_date = text
        found_submissions = get_submissions_by_date(target_date)
        
        if found_submissions:
            send_message(user_id,
                        f"📦 Найдено {len(found_submissions)} материалов за дату {target_date}:")
            
            # Пересылаем каждую заявку
            sent_count = 0
            for submission in found_submissions:
                child_name, video_attachment, group_name, user_name = submission
                message_text = (f"👶 **{child_name}**\n"
                              f"🏫 Группа: {group_name}\n"
                              f"👤 От: {user_name or 'Неизвестно'}")
                
                try:
                    # Отправляем сообщение с именем и видео
                    if send_message(user_id, message_text, attachment=video_attachment):
                        sent_count += 1
                    time.sleep(0.5)  # Задержка между отправками
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки материала: {e}")
            
            send_message(user_id,
                        f"✅ Готово! Обработано {sent_count} из {len(found_submissions)} материалов.",
                        create_teacher_restart_keyboard())
        else:
            send_message(user_id,
                        f"❌ Материалы за дату {target_date} не найдены.",
                        create_teacher_restart_keyboard())
        
        reset_user_state(user_id)
    else:
        send_message(user_id, message)

# ========== ГЛАВНЫЙ ОБРАБОТЧИК ==========
def handle_message(user_id, text, attachments, user_info):
    """Основной обработчик сообщений"""
    try:
        # Проверяем режим пользователя
        use_bot = get_user_setting(user_id)
        
        # Если пользователь не в режиме бота, просто выходим (сообщение пойдет админам)
        if not use_bot:
            logger.info(f"💬 Сообщение от {get_user_display_name(user_id, user_info)} в обычном режиме: {text}")
            return

        # Получаем текущее состояние пользователя
        with state_lock:
            current_state = user_states.get(user_id, {}).get('state', UserState.START)
        
        # Логируем входящее сообщение
        logger.info(f"🤖 Сообщение от {get_user_display_name(user_id, user_info)}: {text}")
        
        # Обработка команды "Старт" или любого сообщения если состояние START
        if text.lower() == 'старт' or current_state == UserState.START:
            handle_main_menu(user_id, user_info)
            return
        
        # Обработка настроек
        if 'настройк' in text.lower() or '⚙️' in text:
            handle_settings(user_id)
            return
        
        # Обработка команды "Начать ещё раз" или "Рестарт"
        if any(word in text.lower() for word in ['начать ещё раз', 'рестарт', 'сначала']):
            handle_main_menu(user_id, user_info)
            return
        
        # Обработка "Назад"
        if text == '🔙 Назад':
            handle_main_menu(user_id, user_info)
            return
        
        # Обработка по состояниям
        if current_state == UserState.CHOOSE_MODE:
            handle_mode_selection(user_id, text, user_info)
        
        elif current_state == UserState.CHOOSE_ROLE:
            handle_role_selection(user_id, text, user_info)
        
        elif current_state == UserState.PARENT_CHOOSE_GROUP:
            handle_parent_group(user_id, text)
        
        elif current_state == UserState.PARENT_ENTER_DATE:
            handle_parent_date(user_id, text)
        
        elif current_state == UserState.PARENT_ENTER_NAME:
            handle_parent_name(user_id, text)
        
        elif current_state == UserState.PARENT_SEND_VIDEO:
            handle_parent_video(user_id, attachments, user_info)
        
        elif current_state == UserState.TEACHER_ENTER_PASSWORD:
            handle_teacher_password(user_id, text)
        
        elif current_state == UserState.TEACHER_ENTER_DATE:
            handle_teacher_date(user_id, text)
        
        else:
            # Если состояние неизвестно - начинаем сначала
            send_message(user_id,
                        "Не понимаю команду. Давайте начнем сначала:",
                        create_start_keyboard())
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка обработки сообщения: {e}")
        send_message(user_id,
                    "⚠️ Произошла ошибка. Давайте начнем заново.",
                    create_start_keyboard())
        reset_user_state(user_id)

# ========== ВЕБ-СЕРВЕР ДЛЯ PING ==========
app = Flask(__name__)

@app.route('/')
def home():
    """Главная страница статуса бота"""
    try:
        submissions_count = len(get_submissions_by_date(datetime.datetime.now().strftime('%d.%m.%Y')))
    except:
        submissions_count = 0
        
    return f"""
    <html>
        <head>
            <title>VK Video Bot</title>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .status {{ color: green; font-weight: bold; }}
            </style>
        </head>
        <body>
            <h1>✅ Бот для ВКонтакте работает!</h1>
            <p>Время сервера: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            <p>Статус: <span class="status">🟢 Активен</span></p>
            <p>Заявок сегодня: {submissions_count}</p>
            <p><a href="/health">Проверка здоровья</a> | <a href="/stats">Статистика</a> | <a href="/backup">Создать бэкап</a></p>
        </body>
    </html>
    """

@app.route('/health')
def health():
    """Проверка здоровья бота"""
    return "OK"

@app.route('/stats')
def stats():
    """Статистика бота"""
    try:
        with db_lock:
            cursor = db_connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM submissions')
            total_submissions = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM user_settings')
            total_users = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM user_settings WHERE use_bot = FALSE')
            message_mode_users = cursor.fetchone()[0]
            
            # Последние 5 заявок
            cursor.execute('''
                SELECT child_name, group_name, date, created_at 
                FROM submissions 
                ORDER BY created_at DESC 
                LIMIT 5
            ''')
            recent_submissions = cursor.fetchall()
        
        stats_data = {
            'status': 'active',
            'total_submissions': total_submissions,
            'total_users': total_users,
            'message_mode_users': message_mode_users,
            'recent_submissions': [
                {
                    'child_name': sub[0],
                    'group': sub[1],
                    'date': sub[2],
                    'time': sub[3]
                } for sub in recent_submissions
            ],
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        return json.dumps(stats_data, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({'error': str(e)})

@app.route('/backup')
def create_backup():
    """Создание резервной копии базы данных"""
    try:
        backup_database()
        return "✅ Бэкап создан успешно!"
    except Exception as e:
        return f"❌ Ошибка создания бэкапа: {e}"

def backup_database():
    """Создание резервной копии базы данных"""
    try:
        backup_dir = "backups"
        
        # Создаем папку для бэкапов если её нет
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Имя файла с датой
        backup_file = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        backup_path = os.path.join(backup_dir, backup_file)
        
        # Копируем базу данных
        with db_lock:
            shutil.copy2('bot_database.db', backup_path)
        
        logger.info(f"✅ Создан бэкап: {backup_file}")
        
        # Удаляем старые бэкапы (оставляем последние 5)
        try:
            backup_files = sorted([f for f in os.listdir(backup_dir) if f.startswith('backup_')])
            if len(backup_files) > 5:
                for old_backup in backup_files[:-5]:
                    os.remove(os.path.join(backup_dir, old_backup))
                    logger.info(f"🗑️ Удален старый бэкап: {old_backup}")
        except Exception as e:
            logger.error(f"❌ Ошибка очистки старых бэкапов: {e}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка создания бэкапа: {e}")

def run_web_server():
    """Запуск веб-сервера для мониторинга"""
    try:
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        logger.error(f"❌ Ошибка веб-сервера: {e}")

# ========== ЗАПУСК БОТА ==========
if __name__ == '__main__':
    logger.info("🚀 Запуск улучшенного бота...")
    
    try:
        # Инициализация VK API
        vk_session = vk_api.VkApi(token=GROUP_TOKEN)
        vk = vk_session.get_api()
        longpoll = VkBotLongPoll(vk_session, GROUP_ID)
        logger.info("✅ VK API инициализирован")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации VK API: {e}")
        sys.exit(1)
    
    try:
        # Запускаем веб-сервер в отдельном потоке
        web_thread = Thread(target=run_web_server, daemon=True)
        web_thread.start()
        logger.info("🌐 Веб-сервер запущен")
        
        logger.info("✅ Бот успешно запущен и ожидает сообщений...")
        logger.info(f"📊 Статистика доступна по /stats")
        
        # Основной цикл бота с переподключением при ошибках
        while True:
            try:
                for event in longpoll.listen():
                    if event.type == VkBotEventType.MESSAGE_NEW:
                        try:
                            user_id = event.message.from_id
                            text = event.message.text
                            attachments = event.message.attachments
                            
                            # Получаем информацию о пользователе
                            user_info = vk.users.get(user_ids=user_id, fields='first_name,last_name')[0]
                            
                            # Обрабатываем сообщение
                            handle_message(user_id, text, attachments, user_info)
                            
                        except Exception as e:
                            logger.error(f"❌ Ошибка обработки события: {e}")
                            try:
                                send_message(user_id,
                                            "⚠️ Произошла ошибка. Давайте начнем заново.",
                                            create_start_keyboard())
                            except:
                                pass
                
            except vk_api.exceptions.ApiError as e:
                logger.error(f"❌ Ошибка VK API: {e}")
                time.sleep(10)  # Ждем перед переподключением
            except Exception as e:
                logger.error(f"❌ Общая ошибка в основном цикле: {e}")
                time.sleep(10)
    
    except KeyboardInterrupt:
        logger.info("⏹️ Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка бота: {e}")
    finally:
        # Закрываем соединение с БД
        if db_connection:
            db_connection.close()
        logger.info("🔚 Работа бота завершена")
