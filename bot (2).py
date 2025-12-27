import asyncio
import sqlite3
import json
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import pytz

MONTHS_RU = [
    'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
    'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
]

def format_date_ru(date_obj, include_year=True):
    """Форматирование даты на русском языке"""
    if include_year:
        return f"{date_obj.day} {MONTHS_RU[date_obj.month-1]} {date_obj.year}"
    return f"{date_obj.day} {MONTHS_RU[date_obj.month-1]}"

MOSCOW_TZ = pytz.timezone('Europe/Moscow')
DB_PATH = 'fitness.db'

class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        with self.conn:
            self.conn.executescript('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    joined_date TIMESTAMP,
                    notifications INTEGER DEFAULT 1,
                    is_active INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS goals (
                    goal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    name TEXT NOT NULL,
                    target INTEGER NOT NULL,
                    goal_type TEXT CHECK(goal_type IN ('daily', 'monthly')),
                    created_by INTEGER,
                    created_at TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS user_progress (
                    user_id INTEGER,
                    goal_id INTEGER,
                    date DATE,
                    value INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, goal_id, date)
                );
                CREATE TABLE IF NOT EXISTS activities (
                    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    user_id INTEGER,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS chat_menu (
                    chat_id INTEGER PRIMARY KEY,
                    menu_message_id INTEGER
                );
                CREATE TABLE IF NOT EXISTS active_session (
                    chat_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    state TEXT,
                    data TEXT,
                    started_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS temp_data (
                    user_id INTEGER PRIMARY KEY,
                    data TEXT
                );
            ''')
    
    def get_chat_menu(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT menu_message_id FROM chat_menu WHERE chat_id = ?', (chat_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def set_chat_menu(self, chat_id, message_id):
        with self.conn:
            self.conn.execute('INSERT OR REPLACE INTO chat_menu (chat_id, menu_message_id) VALUES (?, ?)', (chat_id, message_id))
    
    def get_active_session(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT user_id, state, data, started_at FROM active_session WHERE chat_id = ?', (chat_id,))
        result = cursor.fetchone()
        if result:
            user_id, state, data_json, started_at = result
            started = datetime.fromisoformat(started_at)
            if (datetime.now() - started).total_seconds() > 300:
                self.clear_active_session(chat_id)
                return None
            data = json.loads(data_json) if data_json else {}
            return {'user_id': user_id, 'state': state, 'data': data}
        return None
    
    def set_active_session(self, chat_id, user_id, state, data=None):
        data_json = json.dumps(data) if data else None
        with self.conn:
            self.conn.execute('INSERT OR REPLACE INTO active_session (chat_id, user_id, state, data, started_at) VALUES (?, ?, ?, ?, ?)',
                            (chat_id, user_id, state, data_json, datetime.now().isoformat()))
    
    def clear_active_session(self, chat_id):
        with self.conn:
            self.conn.execute('DELETE FROM active_session WHERE chat_id = ?', (chat_id,))
    
    def is_user_registered(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM users WHERE user_id = ? AND is_active = 1', (user_id,))
        return cursor.fetchone() is not None
    
    def get_user(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT name, joined_date, notifications FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return result if result else (None, None, None)
    
    def add_user(self, user_id, name):
        with self.conn:
            self.conn.execute('INSERT INTO users (user_id, name, joined_date) VALUES (?, ?, ?)',
                            (user_id, name, datetime.now().isoformat()))
    
    def update_user_name(self, user_id, new_name):
        with self.conn:
            self.conn.execute('UPDATE users SET name = ? WHERE user_id = ?', (new_name, user_id))
    
    def toggle_notifications(self, user_id):
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute('SELECT notifications FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            if not result:
                return 1
            current = result[0]
            new = 0 if current == 1 else 1
            cursor.execute('UPDATE users SET notifications = ? WHERE user_id = ?', (new, user_id))
            return new
    
    def deactivate_user(self, user_id):
        with self.conn:
            self.conn.execute('UPDATE users SET is_active = 0 WHERE user_id = ?', (user_id,))
            self.conn.execute('DELETE FROM user_progress WHERE user_id = ?', (user_id,))
    
    def get_goals(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT goal_id, name, target, goal_type, created_by FROM goals WHERE chat_id = ? AND is_active = 1 ORDER BY created_at DESC', (chat_id,))
        return cursor.fetchall()
    
    def get_goal(self, goal_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT name, target, goal_type, created_by FROM goals WHERE goal_id = ?', (goal_id,))
        return cursor.fetchone()
    
    def add_goal(self, chat_id, name, target, goal_type, created_by):
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute('INSERT INTO goals (chat_id, name, target, goal_type, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)',
                          (chat_id, name, target, goal_type, created_by, datetime.now().isoformat()))
            return cursor.lastrowid
    
    def get_today_progress(self, user_id, goal_id):
        today = datetime.now().date().isoformat()
        cursor = self.conn.cursor()
        cursor.execute('SELECT value FROM user_progress WHERE user_id = ? AND goal_id = ? AND date = ?',
                      (user_id, goal_id, today))
        result = cursor.fetchone()
        return result[0] if result else 0
    
    def update_progress(self, user_id, goal_id, value):
        today = datetime.now().date().isoformat()
        with self.conn:
            self.conn.execute('INSERT OR REPLACE INTO user_progress (user_id, goal_id, date, value) VALUES (?, ?, ?, ?)',
                            (user_id, goal_id, today, value))
    
    def add_to_progress(self, user_id, goal_id, amount):
        today = datetime.now().date().isoformat()
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute('SELECT value FROM user_progress WHERE user_id = ? AND goal_id = ? AND date = ?',
                          (user_id, goal_id, today))
            result = cursor.fetchone()
            current = result[0] if result else 0
            new_value = current + amount
            cursor.execute('INSERT OR REPLACE INTO user_progress (user_id, goal_id, date, value) VALUES (?, ?, ?, ?)',
                          (user_id, goal_id, today, new_value))
            return new_value
    
    def get_active_users(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT user_id, name FROM users WHERE is_active = 1 ORDER BY joined_date')
        return cursor.fetchall()
    
    def log_activity(self, chat_id, user_id, action, details):
        with self.conn:
            self.conn.execute('INSERT INTO activities (chat_id, user_id, action, details, timestamp) VALUES (?, ?, ?, ?, ?)',
                            (chat_id, user_id, action, details, datetime.now().isoformat()))
    
    def get_recent_activities(self, chat_id, limit=5):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT u.name, a.action, a.details, a.timestamp 
            FROM activities a 
            JOIN users u ON a.user_id = u.user_id 
            WHERE a.chat_id = ? 
            ORDER BY a.timestamp DESC 
            LIMIT ?
        ''', (chat_id, limit))
        return cursor.fetchall()
    
    def delete_goal(self, goal_id):
        with self.conn:
            self.conn.execute('UPDATE goals SET is_active = 0 WHERE goal_id = ?', (goal_id,))
    
    def get_week_stats(self, chat_id, start_date):
        end_date = start_date + timedelta(days=6)
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT date, COALESCE(SUM(value), 0) as total, COUNT(DISTINCT up.user_id) as participants
            FROM user_progress up
            JOIN goals g ON up.goal_id = g.goal_id
            WHERE g.chat_id = ? AND date BETWEEN ? AND ?
            GROUP BY date
            ORDER BY date DESC
        ''', (chat_id, start_date.isoformat(), end_date.isoformat()))
        return cursor.fetchall()
    
    def get_day_stats(self, chat_id, date):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT g.goal_id, g.name, g.target, u.user_id, u.name, COALESCE(up.value, 0) as value
            FROM goals g
            CROSS JOIN users u
            LEFT JOIN user_progress up ON g.goal_id = up.goal_id AND u.user_id = up.user_id AND up.date = ?
            WHERE g.chat_id = ? AND g.is_active = 1 AND u.is_active = 1
            ORDER BY g.created_at, u.joined_date
        ''', (date.isoformat(), chat_id))
        return cursor.fetchall()
    
    def get_user_stats(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(DISTINCT date) FROM user_progress WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        active_days = result[0] if result else 0
        
        today = datetime.now().date().isoformat()
        cursor.execute('SELECT COALESCE(SUM(value), 0) FROM user_progress WHERE user_id = ? AND date = ?', (user_id, today))
        result = cursor.fetchone()
        today_total = result[0] if result else 0
        
        cursor.execute('SELECT COALESCE(SUM(value), 0) FROM user_progress WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        total = result[0] if result else 0
        
        return active_days, today_total, total
    
    def reset_user_progress(self, user_id, reset_type):
        today = datetime.now().date()
        with self.conn:
            if reset_type == 'today':
                self.conn.execute('DELETE FROM user_progress WHERE user_id = ? AND date = ?', (user_id, today.isoformat()))
            elif reset_type == 'week':
                week_start = today - timedelta(days=today.weekday())
                self.conn.execute('DELETE FROM user_progress WHERE user_id = ? AND date >= ?', (user_id, week_start.isoformat()))
            elif reset_type == 'all':
                self.conn.execute('DELETE FROM user_progress WHERE user_id = ?', (user_id,))
    
    def set_temp_data(self, user_id, key, value):
        cursor = self.conn.cursor()
        cursor.execute('SELECT data FROM temp_data WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        data = json.loads(result[0]) if result else {}
        data[key] = value
        with self.conn:
            self.conn.execute('INSERT OR REPLACE INTO temp_data (user_id, data) VALUES (?, ?)', (user_id, json.dumps(data)))
    
    def get_temp_data(self, user_id, key):
        cursor = self.conn.cursor()
        cursor.execute('SELECT data FROM temp_data WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            data = json.loads(result[0])
            return data.get(key)
        return None
    
    def clear_temp_data(self, user_id):
        with self.conn:
            self.conn.execute('DELETE FROM temp_data WHERE user_id = ?', (user_id,))

class FitnessBot:
    def __init__(self, token):
        self.bot = Bot(token=token)
        self.dp = Dispatcher()
        self.db = Database()
        self.setup_handlers()
    
    def setup_handlers(self):
        self.dp.message.register(self.start_command, Command('start'))
        self.dp.message.register(self.help_command, Command('help'))
        self.dp.message.register(self.handle_text_message, F.text)
        self.dp.callback_query.register(self.handle_callback)
    
    def create_progress_bar(self, percentage, width=10, is_main=False):
        percentage = min(100, max(0, percentage))
        filled = int(percentage * width / 100)
        
        if is_main:
            # Для главного статус-бара используем полные блоки Unicode
            # Полный блок: "█" (U+2588), пустой: "░" (U+2591)
            return "█" * filled + "░" * (width - filled)
        else:
            # Для маленьких статус-баров используем вертикальные прямоугольники
            # Заполненный: "▰" (U+25B0), пустой: "▱" (U+25B1)
            return "▰" * filled + "▱" * (width - filled)
    
    async def ensure_menu(self, chat_id, text, keyboard):
        menu_id = self.db.get_chat_menu(chat_id)
        try:
            if menu_id:
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=menu_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode='HTML'
                )
                return menu_id
        except:
            pass
        
        msg = await self.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        self.db.set_chat_menu(chat_id, msg.message_id)
        return msg.message_id
    
    async def show_popup(self, callback, text):
        await callback.answer(text, show_alert=True)
    
    async def generate_main_menu(self, chat_id):
        today = datetime.now(MOSCOW_TZ)
        today_str = format_date_ru(today)
        
        users = self.db.get_active_users()
        goals = self.db.get_goals(chat_id)
        
        menu_text = f"<b>ФИТНЕС-ГРУППА</b>\n{today_str}\n\n"
        menu_text += f"👥 {len(users)} участника • 🎯 {len(goals)} цели\n\n"
        
        if goals:
            menu_text += "<b>ЦЕЛИ ГРУППЫ</b>\n"
            for goal in goals:
                goal_id, name, target, goal_type, created_by = goal
                icon = "📅" if goal_type == 'daily' else "📆"
                
                total_today = 0
                user_progress = []
                
                for user_id, user_name in users:
                    progress = self.db.get_today_progress(user_id, goal_id)
                    total_today += progress
                    percent = min(100, int(progress / target * 100)) if target > 0 else 0
                    bar = self.create_progress_bar(percent, 10, False)
                    user_progress.append(f"{user_name}: {bar} {percent}% ({progress}/{target})")
                
                total_percent = min(100, int(total_today / (target * len(users)) * 100)) if len(users) > 0 and target > 0 else 0
                total_bar = self.create_progress_bar(total_percent, 10, True)  # 10 символов
                
                # Форматирование процентов с фиксированной шириной
                percent_str = f"{total_percent:3d}%"
                
                menu_text += f"\n{icon} {name}\n"
                # Верхняя рамка: 16 символов (┏ + 10 штрихов + ┓ + 4 пробела)
                menu_text += "┏" + "━" * 10 + "┓    \n"
                # Средняя строка: 16 символов (┃ + 10 статус-бар + ┃ + 4 процента)
                menu_text += f"┃{total_bar}┃{percent_str}\n"
                # Нижняя рамка: 16 символов (┗ + 10 штрихов + ┛ + 4 пробела)
                menu_text += "┗" + "━" * 10 + "┛    \n"
                menu_text += "\n".join(user_progress) + "\n"
        else:
            menu_text += "<b>ЦЕЛИ ГРУППЫ</b>\n\n"
            menu_text += "Пока нет активных целей\n\n"
        
        activities = self.db.get_recent_activities(chat_id, 5)
        if activities:
            menu_text += "<b>ПОСЛЕДНИЕ ДЕЙСТВИЯ</b>\n"
            for name, action, details, timestamp in activities:
                time = datetime.fromisoformat(timestamp).astimezone(MOSCOW_TZ).strftime('%H:%M')
                menu_text += f"\n{time} - {name}: {details}"
        
        return menu_text
    
    async def generate_main_keyboard(self, chat_id, user_id=None):
        builder = InlineKeyboardBuilder()
        
        if user_id and self.db.is_user_registered(user_id):
            # У зарегистрированного пользователя есть все кнопки
            builder.row(
                InlineKeyboardButton(text="➕ ДОБАВИТЬ ЦЕЛЬ", callback_data="add_goal"),
                InlineKeyboardButton(text="✅ ОТМЕТИТЬ", callback_data="mark_progress")
            )
            builder.row(
                InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="statistics"),
                InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="settings")
            )
            
            goals = self.db.get_goals(chat_id)
            if goals:
                builder.row(InlineKeyboardButton(text="🗑️ УДАЛИТЬ ЦЕЛЬ", callback_data="delete_goal"))
            
            # Добавляем кнопку создания профиля для других пользователей
            builder.row(InlineKeyboardButton(text="👤 СОЗДАТЬ ПРОФИЛЬ", callback_data="create_profile"))
        else:
            # У незарегистрированного пользователя только базовые кнопки
            builder.row(
                InlineKeyboardButton(text="👤 СОЗДАТЬ ПРОФИЛЬ", callback_data="create_profile"),
                InlineKeyboardButton(text="❓ ПОМОЩЬ", callback_data="help")
            )
        
        return builder.as_markup()
    
    async def start_command(self, message: Message):
        chat_id = message.chat.id
        
        # Удаляем сообщение с командой
        try:
            await message.delete()
        except:
            pass
        
        menu_text = await self.generate_main_menu(chat_id)
        keyboard = await self.generate_main_keyboard(chat_id, message.from_user.id)
        
        menu_id = await self.ensure_menu(chat_id, menu_text, keyboard)
        
        self.db.log_activity(chat_id, message.from_user.id, 'start', 'Бот активирован')
    
    async def help_command(self, message: Message):
        # Удаляем сообщение с командой
        try:
            await message.delete()
        except:
            pass
        
        help_text = """<b>🏋️‍♂️ ПОМОЩЬ ПО БОТУ:</b>

<b>Основные функции:</b>
• /start - активировать бота
• 👤 СОЗДАТЬ ПРОФИЛЬ - создать профиль
• ➕ ДОБАВИТЬ ЦЕЛЬ - создать новую цель
• ✅ ОТМЕТИТЬ - отметить выполнение цели
• 📊 СТАТИСТИКА - просмотр истории
• ⚙️ НАСТРОЙКИ - личные настройки

<b>Как работает:</b>
1. Напишите /start в чате
2. Создайте профиль
3. Добавьте цели для группы
4. Отмечайте ежедневный прогресс

<b>Поддержка:</b> @support_contact"""
        
        # Отправляем помощь как отдельное сообщение (как было раньше)
        await message.answer(help_text, parse_mode='HTML')
    
    async def handle_text_message(self, message: Message):
        if message.from_user.is_bot:
            return
            
        chat_id = message.chat.id
        user_id = message.from_user.id
        text = message.text.strip()
        
        # Удаляем сообщение пользователя сразу
        try:
            await message.delete()
        except:
            pass  # Игнорируем ошибки удаления
        
        # Удаляем команды тоже
        if text.startswith('/'):
            return
        
        session = self.db.get_active_session(chat_id)
        if not session or session['user_id'] != user_id:
            return
        
        state = session['state']
        
        if state == 'waiting_for_name':
            if not 1 <= len(text) <= 20:
                # Показываем ошибку в меню
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                ]])
                await self.ensure_menu(chat_id, 
                    "<b>РЕГИСТРАЦИЯ НОВОГО УЧАСТНИКА</b>\n\n"
                    f"<b>⚠️ Имя должно быть от 1 до 20 символов</b>\n\n"
                    f"Вы ввели: '{text}' ({len(text)} символов)\n\n"
                    "Введите ваше имя в чат:", 
                    keyboard
                )
                return
            
            self.db.add_user(user_id, text)
            self.db.clear_active_session(chat_id)
            self.db.log_activity(chat_id, user_id, 'register', 'зарегистрировался')
            
            # Показываем уведомление в меню
            await self.show_temporary_notification(chat_id, user_id, f"✅ {text} зарегистрирован!")
        
        elif state == 'waiting_for_new_name':
            if not 1 <= len(text) <= 20:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                ]])
                await self.ensure_menu(chat_id, 
                    "<b>✏️ ИЗМЕНИТЬ ИМЯ</b>\n\n"
                    f"<b>⚠️ Имя должно быть от 1 до 20 символов</b>\n\n"
                    f"Вы ввели: '{text}' ({len(text)} символов)\n\n"
                    "Введите новое имя в чат:", 
                    keyboard
                )
                return
            
            old_name = self.db.get_user(user_id)[0]
            self.db.update_user_name(user_id, text)
            self.db.clear_active_session(chat_id)
            self.db.log_activity(chat_id, user_id, 'update_name', f'изменил имя с {old_name} на {text}')
            
            # Показываем уведомление в меню
            await self.show_temporary_notification(chat_id, user_id, f"✅ Имя изменено на '{text}'")
        
        elif state == 'waiting_for_goal_name':
            if not 1 <= len(text) <= 30:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                ]])
                await self.ensure_menu(chat_id, 
                    "<b>🎯 ДОБАВЛЕНИЕ ЦЕЛИ</b>\n\n"
                    f"<b>⚠️ Название должно быть от 1 до 30 символов</b>\n\n"
                    f"Вы ввели: '{text}' ({len(text)} символов)\n\n"
                    "Введите название цели в чат:", 
                    keyboard
                )
                return
            
            self.db.set_active_session(chat_id, user_id, 'waiting_for_goal_target', {'goal_name': text})
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
            ]])
            
            await self.ensure_menu(chat_id, 
                f"<b>🎯 ДОБАВЛЕНИЕ ЦЕЛИ</b>\n\n"
                f"Цель: <b>{text}</b>\n\n"
                "Введите целевое число в чат:", 
                keyboard
            )
        
        elif state == 'waiting_for_goal_target':
            try:
                target = int(text)
                if not 1 <= target <= 10000:
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                    ]])
                    await self.ensure_menu(chat_id, 
                        f"<b>🎯 ДОБАВЛЕНИЕ ЦЕЛИ</b>\n\n"
                        f"Цель: <b>{session['data']['goal_name']}</b>\n\n"
                        f"<b>⚠️ Число должно быть от 1 до 10000</b>\n\n"
                        f"Вы ввели: {text}\n\n"
                        "Введите целевое число в чат:", 
                        keyboard
                    )
                    return
            except:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                ]])
                await self.ensure_menu(chat_id, 
                    f"<b>🎯 ДОБАВЛЕНИЕ ЦЕЛИ</b>\n\n"
                    f"Цель: <b>{session['data']['goal_name']}</b>\n\n"
                    f"<b>⚠️ Введите число (например: 100)</b>\n\n"
                    f"Вы ввели: '{text}'\n\n"
                    "Введите целевое число в чат:", 
                    keyboard
                )
                return
            
            goal_name = session['data']['goal_name']
            self.db.set_active_session(chat_id, user_id, 'waiting_for_goal_type', 
                                     {'goal_name': goal_name, 'goal_target': target})
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📅 ДНЕВНАЯ", callback_data="goal_type_daily"),
                 InlineKeyboardButton(text="📆 МЕСЯЧНАЯ", callback_data="goal_type_monthly")],
                [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")]
            ])
            
            await self.ensure_menu(chat_id, 
                f"<b>🎯 ДОБАВЛЕНИЕ ЦЕЛИ</b>\n\n"
                f"Цель: <b>{goal_name}</b>\n"
                f"Целевое значение: <b>{target}</b>\n\n"
                "Выберите тип цели:", 
                keyboard
            )
        
        elif state == 'waiting_for_complete_number':
            try:
                amount = int(text)
                if amount <= 0:
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                    ]])
                    await self.ensure_menu(chat_id, 
                        f"<b>✅ ОТМЕТИТЬ ВЫПОЛНЕНИЕ</b>\n\n"
                        f"Цель: <b>{session['data']['goal_name']}</b>\n\n"
                        f"<b>⚠️ Введите положительное число</b>\n\n"
                        f"Вы ввели: {text}\n\n"
                        "Введите количество в чат:", 
                        keyboard
                    )
                    return
            except:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
                ]])
                await self.ensure_menu(chat_id, 
                    f"<b>✅ ОТМЕТИТЬ ВЫПОЛНЕНИЕ</b>\n\n"
                    f"Цель: <b>{session['data']['goal_name']}</b>\n\n"
                    f"<b>⚠️ Введите число</b>\n\n"
                    f"Вы ввели: '{text}'\n\n"
                    "Введите количество в чат:", 
                    keyboard
                )
                return
            
            goal_id = session['data']['goal_id']
            goal_name = session['data']['goal_name']
            
            new_value = self.db.add_to_progress(user_id, goal_id, amount)
            self.db.clear_active_session(chat_id)
            self.db.log_activity(chat_id, user_id, 'progress', f'+{amount} {goal_name}')
            
            # Показываем уведомление в меню
            await self.show_temporary_notification(chat_id, user_id, f"✅ +{amount} {goal_name} отмечено!")
    
    async def handle_callback(self, callback: CallbackQuery):
        chat_id = callback.message.chat.id
        user_id = callback.from_user.id
        data = callback.data
        
        session = self.db.get_active_session(chat_id)
        if session and session['user_id'] != user_id:
            await self.show_popup(callback, "⏳ Пожалуйста, подождите!")
            return
        
        if data == 'create_profile':
            if self.db.is_user_registered(user_id):
                await self.show_popup(callback, "ℹ️ У вас уже есть профиль!")
                return
            
            self.db.set_active_session(chat_id, user_id, 'waiting_for_name')
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
            ]])
            await self.ensure_menu(chat_id, "<b>РЕГИСТРАЦИЯ НОВОГО УЧАСТНИКА</b>\n\nВведите ваше имя в чат:", keyboard)
            await callback.answer()
        
        elif data == 'add_goal':
            if not self.db.is_user_registered(user_id):
                await self.show_popup(callback, "⚠️ Сначала создайте профиль!")
                return
            
            self.db.set_active_session(chat_id, user_id, 'waiting_for_goal_name')
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
            ]])
            await self.ensure_menu(chat_id, "<b>🎯 ДОБАВЛЕНИЕ ЦЕЛИ</b>\n\nВведите название цели в чат:", keyboard)
            await callback.answer()
        
        elif data == 'mark_progress':
            if not self.db.is_user_registered(user_id):
                await self.show_popup(callback, "⚠️ Сначала создайте профиль!")
                return
            
            goals = self.db.get_goals(chat_id)
            if not goals:
                await self.show_popup(callback, "⚠️ Нет активных целей для отметки!")
                return
            
            builder = InlineKeyboardBuilder()
            for goal in goals:
                goal_id, name, target, goal_type, _ = goal
                progress = self.db.get_today_progress(user_id, goal_id)
                percent = min(100, int(progress / target * 100)) if target > 0 else 0
                bar = self.create_progress_bar(percent, 10, False)
                builder.row(InlineKeyboardButton(
                    text=f"🎯 {name} {bar} {progress}/{target}",
                    callback_data=f"select_goal_{goal_id}"
                ))
            
            builder.row(InlineKeyboardButton(text="🔙 В ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
            
            await self.ensure_menu(chat_id, "<b>✅ ОТМЕТИТЬ ВЫПОЛНЕНИЕ</b>\n\nВыберите цель для отметки:", builder.as_markup())
            await callback.answer()
        
        elif data.startswith('select_goal_'):
            goal_id = int(data.split('_')[2])
            
            goal_info = self.db.get_goal(goal_id)
            if not goal_info:
                await self.show_popup(callback, "⚠️ Цель не найдена!")
                return
            
            goal_name, target, goal_type, created_by = goal_info
            progress = self.db.get_today_progress(user_id, goal_id)
            
            self.db.set_active_session(chat_id, user_id, 'waiting_for_complete_number',
                                     {'goal_id': goal_id, 'goal_name': goal_name})
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
            ]])
            
            await self.ensure_menu(chat_id, f"<b>✅ ОТМЕТИТЬ ВЫПОЛНЕНИЕ</b>\n\nЦель: {goal_name}\nТекущий прогресс: {progress}/{target}\n\nВведите количество в чат:", keyboard)
            await callback.answer()
        
        elif data in ['goal_type_daily', 'goal_type_monthly']:
            session = self.db.get_active_session(chat_id)
            if not session or session['state'] != 'waiting_for_goal_type':
                await self.show_popup(callback, "⚠️ Сессия истекла!")
                return
            
            goal_name = session['data']['goal_name']
            target = session['data']['goal_target']
            goal_type = 'daily' if data == 'goal_type_daily' else 'monthly'
            
            goal_id = self.db.add_goal(chat_id, goal_name, target, goal_type, user_id)
            
            self.db.clear_active_session(chat_id)
            self.db.log_activity(chat_id, user_id, 'create_goal', f'создал цель {goal_name}')
            
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
            
            await self.show_popup(callback, f"✅ Цель '{goal_name}' добавлена")
        
        elif data == 'statistics':
            if not self.db.is_user_registered(user_id):
                await self.show_popup(callback, "⚠️ Сначала создайте профиль!")
                return
            
            self.db.set_temp_data(user_id, 'stats_page', 0)
            await self.show_statistics_page(chat_id, user_id, 0)
            await callback.answer()
        
        elif data == 'statistics_prev':
            page = self.db.get_temp_data(user_id, 'stats_page') or 0
            if page > 0:
                self.db.set_temp_data(user_id, 'stats_page', page - 1)
                await self.show_statistics_page(chat_id, user_id, page - 1)
            await callback.answer()
        
        elif data == 'statistics_next':
            page = self.db.get_temp_data(user_id, 'stats_page') or 0
            if page < 3:  # Сохраняем ограничение 4 страниц как в исходном коде
                self.db.set_temp_data(user_id, 'stats_page', page + 1)
                await self.show_statistics_page(chat_id, user_id, page + 1)
            await callback.answer()
        
        elif data == 'statistics_today':
            self.db.set_temp_data(user_id, 'stats_page', 0)
            await self.show_statistics_page(chat_id, user_id, 0)
            await callback.answer()
        
        elif data.startswith('stats_day_'):
            # Новый формат: stats_day_2024-03-11
            date_str = data[10:]  # Убираем 'stats_day_'
            try:
                target_date = datetime.fromisoformat(date_str).date()
                await self.show_day_statistics(chat_id, target_date)
            except ValueError as e:
                # Если ошибка, возвращаемся к статистике
                page = self.db.get_temp_data(user_id, 'stats_page') or 0
                await self.show_statistics_page(chat_id, user_id, page)
            await callback.answer()
        
        elif data == 'statistics_back':
            page = self.db.get_temp_data(user_id, 'stats_page') or 0
            await self.show_statistics_page(chat_id, user_id, page)
            await callback.answer()
        
        elif data == 'stats_back':
            # Обработчик для кнопки НАЗАД из детальной статистики
            page = self.db.get_temp_data(user_id, 'stats_page') or 0
            await self.show_statistics_page(chat_id, user_id, page)
            await callback.answer()
        
        elif data == 'settings':
            if not self.db.is_user_registered(user_id):
                await self.show_popup(callback, "⚠️ Сначала создайте профиль!")
                return
            
            user_info = self.db.get_user(user_id)
            name, joined_date, notifications = user_info
            
            if not name:
                await self.show_popup(callback, "⚠️ Профиль не найден!")
                return
            
            joined = datetime.fromisoformat(joined_date).strftime('%d.%m.%Y') if joined_date else "Неизвестно"
            active_days, today_total, total = self.db.get_user_stats(user_id)
            
            settings_text = f"<b>⚙️ НАСТРОЙКИ</b>\n\n"
            settings_text += f"<b>👤 ВАШ ПРОФИЛЬ:</b>\n"
            settings_text += f"• Имя: {name}\n"
            settings_text += f"• В группе с: {joined}\n\n"
            settings_text += f"<b>🎯 ВАША СТАТИСТИКА:</b>\n"
            settings_text += f"• Сегодня: {today_total}\n"
            settings_text += f"• Всего: {total}\n"
            settings_text += f"• Дней активности: {active_days}\n\n"
            settings_text += f"<b>🔔 УВЕДОМЛЕНИЯ:</b>\n"
            settings_text += f"• Статус: {'✅ ВКЛ' if notifications == 1 else '❌ ВЫКЛ'}\n"
            
            builder = InlineKeyboardBuilder()
            builder.row(InlineKeyboardButton(text="✏️ ИЗМЕНИТЬ ИМЯ", callback_data="change_name"))
            builder.row(InlineKeyboardButton(text=f"🔔 УВЕДОМЛЕНИЯ {'✅' if notifications == 1 else '❌'}", 
                                           callback_data="toggle_notifications"))
            builder.row(InlineKeyboardButton(text="🔄 СБРОС", callback_data="reset_menu"))
            builder.row(InlineKeyboardButton(text="🔙 В ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
            
            await self.ensure_menu(chat_id, settings_text, builder.as_markup())
            await callback.answer()
        
        elif data == 'change_name':
            self.db.set_active_session(chat_id, user_id, 'waiting_for_new_name')
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel")
            ]])
            await self.ensure_menu(chat_id, "<b>✏️ ИЗМЕНИТЬ ИМЯ</b>\n\nВведите новое имя в чат:", keyboard)
            await callback.answer()
        
        elif data == 'toggle_notifications':
            new_status = self.db.toggle_notifications(user_id)
            await self.show_popup(callback, f"✅ Уведомления {'включены' if new_status == 1 else 'отключены'}")
            
            fake_callback = CallbackQuery(
                id=callback.id,
                from_user=callback.from_user,
                chat_instance=callback.chat_instance,
                message=callback.message,
                data='settings'
            )
            await self.handle_callback(fake_callback)
        
        elif data == 'reset_menu':
            builder = InlineKeyboardBuilder()
            builder.row(InlineKeyboardButton(text="🗑️ ТОЛЬКО СЕГОДНЯ", callback_data="reset_today"))
            builder.row(InlineKeyboardButton(text="🗑️ ВСЮ НЕДЕЛЮ", callback_data="reset_week"))
            builder.row(InlineKeyboardButton(text="🗑️ ВЕСЬ ПРОГРЕСС", callback_data="reset_all"))
            builder.row(InlineKeyboardButton(text="🗑️ УДАЛИТЬ ПРОФИЛЬ", callback_data="delete_profile"))
            builder.row(InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel"))
            
            await self.ensure_menu(chat_id, "<b>🔄 СБРОС ПРОГРЕССА</b>\n\n⚠️ ВНИМАНИЕ: Это действие необратимо!\n\nВыберите что сбросить:", builder.as_markup())
            await callback.answer()
        
        elif data == 'reset_today':
            self.db.reset_user_progress(user_id, 'today')
            await self.show_popup(callback, "✅ Сброшен только сегодняшний прогресс")
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
        
        elif data == 'reset_week':
            self.db.reset_user_progress(user_id, 'week')
            await self.show_popup(callback, "✅ Сброшен весь прогресс за неделю")
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
        
        elif data == 'reset_all':
            self.db.reset_user_progress(user_id, 'all')
            await self.show_popup(callback, "✅ Сброшен весь ваш прогресс")
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
        
        elif data == 'delete_profile':
            self.db.deactivate_user(user_id)
            await self.show_popup(callback, "✅ Удалён ваш профиль полностью")
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
        
        elif data == 'delete_goal':
            goals = self.db.get_goals(chat_id)
            if not goals:
                await self.show_popup(callback, "⚠️ Нет активных целей для удаления!")
                return
            
            builder = InlineKeyboardBuilder()
            for goal in goals:
                goal_id, name, _, _, _ = goal
                builder.row(InlineKeyboardButton(
                    text=f"🗑️ {name}",
                    callback_data=f"confirm_delete_{goal_id}"
                ))
            
            builder.row(InlineKeyboardButton(text="🔙 В ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
            
            await self.ensure_menu(chat_id, "<b>🗑️ УДАЛИТЬ ЦЕЛЬ</b>\n\nВыберите цель для удаления:", builder.as_markup())
            await callback.answer()
        
        elif data.startswith('confirm_delete_'):
            goal_id = int(data.split('_')[2])
            goal_info = self.db.get_goal(goal_id)
            if not goal_info:
                await self.show_popup(callback, "⚠️ Цель не найдена!")
                return
            
            goal_name = goal_info[0]
            
            builder = InlineKeyboardBuilder()
            builder.row(InlineKeyboardButton(text=f"✅ ДА, удалить '{goal_name}'", 
                                           callback_data=f"execute_delete_{goal_id}"))
            builder.row(InlineKeyboardButton(text="❌ НЕТ, отменить", callback_data="delete_goal"))
            
            await self.ensure_menu(chat_id, f"<b>🗑️ УДАЛИТЬ ЦЕЛЬ</b>\n\nВы уверены, что хотите удалить цель '{goal_name}'?", builder.as_markup())
            await callback.answer()
        
        elif data.startswith('execute_delete_'):
            goal_id = int(data.split('_')[2])
            goal_info = self.db.get_goal(goal_id)
            if not goal_info:
                await self.show_popup(callback, "⚠️ Цель не найдена!")
                return
            
            goal_name = goal_info[0]
            
            self.db.delete_goal(goal_id)
            self.db.log_activity(chat_id, user_id, 'delete_goal', f'удалил цель {goal_name}')
            
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
            
            await self.show_popup(callback, f"✅ Цель '{goal_name}' удалена")
        
        elif data == 'help':
            help_text = """<b>🏋️‍♂️ ПОМОЩЬ ПО БОТУ:</b>
    
    <b>Основные функции:</b>
    • /start - активировать бота
    • 👤 СОЗДАТЬ ПРОФИЛЬ - создать профиль
    • ➕ ДОБАВИТЬ ЦЕЛЬ - создать новую цель
    • ✅ ОТМЕТИТЬ - отметить выполнение цели
    • 📊 СТАТИСТИКА - просмотр истории
    • ⚙️ НАСТРОЙКИ - личные настройки
    
    <b>Как работает:</b>
    1. Напишите /start в чате
    2. Создайте профиль
    3. Добавьте цели для группы
    4. Отмечайте ежедневный прогресс
    
    <b>Поддержка:</b> @support_contact"""
            
            await callback.message.answer(help_text, parse_mode='HTML')
            await callback.answer()
        
        elif data == 'cancel':
            self.db.clear_active_session(chat_id)
            self.db.clear_temp_data(user_id)
            
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
            
            await callback.answer()
        
        elif data == 'main_menu':
            self.db.clear_temp_data(user_id)
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
            await callback.answer()
        
        elif data == 'noop':
            await callback.answer()
    
    async def show_statistics_page(self, chat_id, user_id, page):
        """Показать статистику за неделю с новым форматом"""
        today = datetime.now(MOSCOW_TZ).date()
        
        # Начинаем с понедельника текущей недели
        days_since_monday = today.weekday()  # 0 = понедельник
        start_date = today - timedelta(days=days_since_monday + page * 7)
        end_date = start_date + timedelta(days=6)
        
        week_stats = self.db.get_week_stats(chat_id, start_date)
        stats_dict = {datetime.fromisoformat(date).date(): (total, participants) for date, total, participants in week_stats}
        
        # Получаем максимально возможное значение за день
        total_participants = len(self.db.get_active_users())
        goals = self.db.get_goals(chat_id)
        
        # Рассчитываем максимально возможный результат за день
        max_possible_per_day = 0
        if goals and total_participants > 0:
            for goal in goals:
                goal_id, name, target, goal_type, created_by = goal
                max_possible_per_day += target * total_participants
        
        # Формируем текст
        stats_text = f"<b>📊 ИСТОРИЯ СТАТИСТИКИ</b>\n"
        
        # Добавляем месяц и год
        month_year = f"{MONTHS_RU[start_date.month-1].upper()} {start_date.year}"
        stats_text += f"🗓️ {month_year}\n\n"
        
        # Диапазон недели
        week_range = f"{start_date.day}-{end_date.day} {MONTHS_RU[end_date.month-1].upper()}"
        stats_text += f"▶️ НЕДЕЛЯ {week_range} ◀️\n\n"
        
        # Дни недели с прогресс-барами
        days_ru = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
        total_week = 0
        active_days = 0
        
        for i in range(7):
            current_date = start_date + timedelta(days=i)
            date_str = f"{days_ru[i]} {current_date.day}"
            
            if current_date in stats_dict:
                total, participants = stats_dict[current_date]
                total_week += total
                
                if total > 0:
                    active_days += 1
                
                # Рассчитываем процент от максимально возможного
                percent = min(100, int(total / max_possible_per_day * 100)) if max_possible_per_day > 0 else 0
                bar = self.create_progress_bar(percent, 10, False)
                check = " ✓" if percent >= 100 else ""
                
                stats_text += f"{date_str}: {bar} {percent}% ({total}){check}\n"
            else:
                stats_text += f"{date_str}: ▱▱▱▱▱▱▱▱▱▱ 0% (0)\n"
        
        stats_text += f"\n📈 ОБЩИЙ ПРОГРЕСС: {total_week}\n"
        stats_text += f"👥 АКТИВНЫХ УЧАСТНИКОВ: {active_days}/7 дней\n"
        
        # Создаем клавиатуру
        builder = InlineKeyboardBuilder()
        
        # Кнопки с числами дней
        days_buttons = []
        for i in range(7):
            current_date = start_date + timedelta(days=i)
            day_number = current_date.day
            
            # Показываем кнопку если есть данные или если это сегодня/прошлые дни
            if current_date in stats_dict or current_date <= today:
                days_buttons.append(InlineKeyboardButton(
                    text=f"{day_number}", 
                    callback_data=f"stats_day_{current_date.isoformat()}"
                ))
            else:
                # Для будущих дней без данных показываем серую кнопку
                days_buttons.append(InlineKeyboardButton(
                    text=f"{day_number}", 
                    callback_data="noop"
                ))
        
        if days_buttons:
            builder.row(*days_buttons)
        
        # Кнопки навигации
        nav_buttons = []
        
        # Кнопка "Пред. неделя"
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ ПРЕД. НЕДЕЛЯ", callback_data="statistics_prev"))
        
        # Кнопка "Сегодня" если не на текущей неделе
        if page != 0:
            nav_buttons.append(InlineKeyboardButton(text="СЕГОДНЯ", callback_data="statistics_today"))
        else:
            # На текущей неделе показываем статическую кнопку
            nav_buttons.append(InlineKeyboardButton(text=f"[{page+1}/4]", callback_data="noop"))
        
        # Кнопка "След. неделя" с ограничением 4 страниц как в исходном коде
        if page < 3:
            nav_buttons.append(InlineKeyboardButton(text="СЛЕД. НЕДЕЛЯ ▶️", callback_data="statistics_next"))
        
        if nav_buttons:
            builder.row(*nav_buttons)
        
        # Кнопка возврата
        builder.row(InlineKeyboardButton(text="🔙 В ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
        
        await self.ensure_menu(chat_id, stats_text, builder.as_markup())
    
    async def show_day_statistics(self, chat_id, date):
        """Показать детальную статистику за конкретный день"""
        day_stats = self.db.get_day_stats(chat_id, date)
        
        stats_text = f"<b>📊 ДЕТАЛЬНАЯ СТАТИСТИКА</b>\n"
        stats_text += f"🗓️ {format_date_ru(date)}\n\n"
        
        if not day_stats:
            stats_text += "Нет данных за этот день."
        else:
            # Группируем данные по целям
            goals_data = {}
            all_users = set()
            active_users = set()
            total_value_day = 0
            total_target_day = 0
            
            for goal_id, goal_name, target, user_id, user_name, value in day_stats:
                all_users.add((user_id, user_name))
                
                if goal_id not in goals_data:
                    goals_data[goal_id] = {
                        'name': goal_name,
                        'target': target,
                        'users': []
                    }
                
                goals_data[goal_id]['users'].append((user_name, value, target))
                
                total_value_day += value
                total_target_day += target
                
                if value > 0:
                    active_users.add((user_id, user_name))
            
            # Считаем выполненные цели
            completed_goals = 0
            total_goals = len(goals_data)
            
            for goal_id, goal_data in goals_data.items():
                goal_name = goal_data['name']
                goal_target = goal_data['target']
                
                stats_text += f"<b>🎯 {goal_name}:</b>\n"
                goal_total = 0
                goal_target_total = 0
                goal_completed = True
                
                for user_name, value, user_target in goal_data['users']:
                    percent = min(100, int(value / user_target * 100)) if user_target > 0 else 0
                    check = "✓" if percent >= 100 else ""
                    stats_text += f"• {user_name}: {value}/{user_target} ({percent}%) {check}\n"
                    goal_total += value
                    goal_target_total += user_target
                    
                    if percent < 100:
                        goal_completed = False
                
                goal_percent = min(100, int(goal_total / goal_target_total * 100)) if goal_target_total > 0 else 0
                
                if goal_completed and goal_total > 0:
                    completed_goals += 1
                    stats_text += f"═ ОБЩИЙ: {goal_total}/{goal_target_total} ({goal_percent}%) ✓\n\n"
                else:
                    stats_text += f"═ ОБЩИЙ: {goal_total}/{goal_target_total} ({goal_percent}%)\n\n"
            
            # Итоги дня
            total_percent = min(100, int(total_value_day / total_target_day * 100)) if total_target_day > 0 else 0
            
            stats_text += f"<b>📊 ИТОГИ ДНЯ:</b>\n"
            stats_text += f"• Общий прогресс: {total_value_day}/{total_target_day} ({total_percent}%)\n"
            stats_text += f"• Участников: {len(active_users)}/{len(all_users)}\n"
            stats_text += f"• Выполнено целей: {completed_goals}/{total_goals}\n"
            
            if total_percent >= 100:
                stats_text += f"\n🏆 <b>ОТЛИЧНЫЙ ДЕНЬ! Все цели выполнены!</b>\n"
        
        # Создаем клавиатуру
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="◀️ НАЗАД", callback_data="statistics_back"))
        builder.row(InlineKeyboardButton(text="🔙 В ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
        
        await self.ensure_menu(chat_id, stats_text, builder.as_markup())
    
    async def show_temporary_notification(self, chat_id, user_id, notification_text, delay=2):
        """Показать временное уведомление в меню и вернуться к главному"""
        # Сохраняем текущее состояние меню
        current_menu = self.db.get_chat_menu(chat_id)
        
        if not current_menu:
            # Если меню нет, создаем новое
            menu_text = await self.generate_main_menu(chat_id)
            keyboard = await self.generate_main_keyboard(chat_id, user_id)
            await self.ensure_menu(chat_id, menu_text, keyboard)
            return
        
        # Показываем уведомление
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⏳", callback_data="noop")
        ]])
        
        try:
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_menu,
                text=f"<b>{notification_text}</b>",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except:
            # Если не удалось отредактировать, создаем новое меню
            msg = await self.bot.send_message(
                chat_id=chat_id,
                text=f"<b>{notification_text}</b>",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
            self.db.set_chat_menu(chat_id, msg.message_id)
            current_menu = msg.message_id
        
        # Ждем и возвращаем главное меню
        await asyncio.sleep(delay)
        
        menu_text = await self.generate_main_menu(chat_id)
        keyboard = await self.generate_main_keyboard(chat_id, user_id)
        await self.ensure_menu(chat_id, menu_text, keyboard)
    
    async def run(self):
        await self.dp.start_polling(self.bot)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Использование: python bot.py <BOT_TOKEN>")
        sys.exit(1)
    
    bot_token = sys.argv[1]
    bot = FitnessBot(bot_token)
    
    asyncio.run(bot.run())