import asyncio
import aiosqlite
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY,
    birth_year TEXT,
    eye_color TEXT,
    gender TEXT,
    hair_color TEXT,
    homeworld TEXT,
    mass TEXT,
    name TEXT UNIQUE,
    skin_color TEXT
);
CREATE INDEX IF NOT EXISTS idx_people_name ON people(name);
"""

async def run_migration():
    logging.info("Подключение к базе данных SQLite...")
    try:
        async with aiosqlite.connect('swapi.db') as db:
            logging.info("Выполнение миграции...")
            await db.executescript(CREATE_TABLE_SQL)
            await db.commit()
            logging.info("Таблица 'people' успешно создана или уже существует.")
    except Exception as e:
        logging.error(f"Ошибка миграции: {e}")

if __name__ == "__main__":
    asyncio.run(run_migration())