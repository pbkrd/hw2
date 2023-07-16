import logging
import sqlite3 as sql

HEADERS = {'User-agent': 'Mozilla/5.0'}

DB_NAME = 'hw2.db'
TABLE_NAME = 'vacancies'

logging.basicConfig(filename='hw2.log',
                    encoding='utf-8',
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")


def init_db(del_old=False):
    new = None
    try:
        conn = sql.connect(DB_NAME)
        cursor = conn.cursor()

        if del_old:
            conn.execute(f'DROP TABLE IF EXISTS {TABLE_NAME}')
            logging.info(f'Таблица {TABLE_NAME} успешно удалена!')
            new = True

        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                            title TEXT,
                            company_name TEXT,
                            description TEXT, 
                            skills TEXT
                        )''')

        if new:
            logging.info(f'Таблица {TABLE_NAME} успешно добавлена!')
        else:
            logging.info(f'Добавление в существующую таблицу {TABLE_NAME}!')
        return conn, cursor
    except sql.Error as error:
        logging.error(
            f'Не удалось подключиться к базе данных'
            f'Исключение: {error.__class__}',
        )
        return None, None


def dump_vacancies(connection, cursor, values):
    insert_val = f'INSERT INTO {TABLE_NAME}(title, company_name, description, skills) VALUES(?, ?, ?, ?);'
    cursor.executemany(insert_val, values)
    connection.commit()
    logging.info(f'Загружено {len(insert_val)} вакансий')
