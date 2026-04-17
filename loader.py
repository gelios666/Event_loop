import asyncio
import aiosqlite
import aiohttp
import logging
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://swapi.dev/api/people/"


async def fetch_character(session: aiohttp.ClientSession, character_id: int) -> Dict[str, Any]:


# ... (код функции fetch_character остаётся без изменений) ...

async def get_all_character_ids() -> List[int]:


# ... (код функции get_all_character_ids остаётся без изменений) ...

async def save_character(db: aiosqlite.Connection, character: Dict[str, Any]):
    insert_sql = """
    INSERT INTO people (id, birth_year, eye_color, gender, hair_color, homeworld, mass, name, skin_color)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        birth_year = excluded.birth_year,
        eye_color = excluded.eye_color,
        gender = excluded.gender,
        hair_color = excluded.hair_color,
        homeworld = excluded.homeworld,
        mass = excluded.mass,
        name = excluded.name,
        skin_color = excluded.skin_color;
    """
    try:
        await db.execute(insert_sql,
                         character['id'], character['birth_year'], character['eye_color'],
                         character['gender'], character['hair_color'], character['homeworld'],
                         character['mass'], character['name'], character['skin_color'])
        await db.commit()
        logging.debug(f"Сохранён персонаж: {character['name']}")
    except Exception as e:
        logging.error(f"Ошибка сохранения персонажа {character.get('name')}: {e}")


async def main():
    logging.info("Подключение к базе данных SQLite...")
    async with aiosqlite.connect('swapi.db') as db:
        # Включаем поддержку внешних ключей для SQLite (хорошая практика)
        await db.execute("PRAGMA foreign_keys = ON")

        logging.info("Получение списка ID персонажей...")
        character_ids = await get_all_character_ids()

        logging.info("Загрузка данных из API...")
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_character(session, cid) for cid in character_ids]
            characters_data = await asyncio.gather(*tasks)

        characters = [ch for ch in characters_data if ch is not None]
        logging.info(f"Успешно загружено: {len(characters)} персонажей")

        logging.info("Сохранение данных в базу...")
        save_tasks = [save_character(db, ch) for ch in characters]
        await asyncio.gather(*save_tasks)

    logging.info("Загрузка завершена.")


if __name__ == "__main__":
    asyncio.run(main())