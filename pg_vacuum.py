# pg_vacuum
# Скрипт для очистки баз postgresql с помощью команды VACUUM ANALYZE.
# Version: 1.00
# Authors: Miller777, ChatGPT (https://trychatgpt.ru)
# Date: 2024-10-13

import os
import subprocess
import datetime
import json
import socket

def load_settings(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        msg = f"Failed to load settings: {e}"
        print(msg)  # Выводим в консоль
        log_message(msg, error=True)  # Записываем ошибку в лог
        return None  # Возвращаем None в случае ошибки

def load_databases(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

def log_message(message, error=False):
    log_file = "db_cleanup.log"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'a') as f:
        if error:
            f.write(f"{timestamp} ERROR: {message}\n")
        else:
            f.write(f"{timestamp} INFO: {message}\n")
        f.write("\n")  # Добавляем пустую строку между записями

def send_telegram_notification(script_path, status, message):
    command = [script_path, status, message]
    subprocess.run(command)
    log_message(f"Sent Telegram notification: {message}")  # Логируем отправку уведомления

def cleanup_database(pg_cleanup_path, host, port, username, db_name, verbose):
    command = [
        pg_cleanup_path,
        "--host", host,
        "--port", port,
        "--username", username,
        "--dbname", db_name,
        "--command", "VACUUM FULL ANALYZE"
    ]

    if verbose:
        print(f"Executing command: {' '.join(command)}")

    try:
        subprocess.run(command, check=True)
        msg = f"Cleanup for database '{db_name}' completed successfully."
        log_message(msg)  # Записываем в лог краткую информацию
        if verbose:
            print(msg)
        return True
    except subprocess.CalledProcessError as e:
        msg = f"Error cleaning up database '{db_name}': {e.output.decode() if hasattr(e, 'output') else str(e)}"
        log_message(msg, error=True)  # Записываем ошибку в лог с подробной информацией
        if verbose:
            print(msg)
        return False

def main():
    verbose = True  # Устанавливаем verbose в True для ручного запуска

    settings_file = "settings.json"
    database_file = "databases.txt"
    host_name = socket.gethostname()  # Получаем имя компьютера

    settings = load_settings(settings_file)
    if settings is None:  # Проверяем, успешно ли загружены настройки
        return  # Если нет, завершаем выполнение программы

    databases = load_databases(database_file)

    # Выполнение очистки для каждой базы данных
    for db in databases:
        success = cleanup_database(settings['pg_cleanup_path'], settings['host'],
                                   settings['port'], settings['username'], db, verbose)

        if success:
            if settings.get('telegram_notifications_enabled'):
                send_telegram_notification(settings['telegram_script_path'], "r",
                                            f"{settings.get('prefix', '')} {host_name}. Cleanup for '{db}' completed successfully.")
        else:
            if settings.get('telegram_notifications_enabled'):
                send_telegram_notification(settings['telegram_script_path'], "a",
                                            f"{settings.get('prefix', '')} {host_name}. Cleanup for '{db}' failed.")

if __name__ == "__main__":
    main()