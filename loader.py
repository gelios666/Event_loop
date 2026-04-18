import asyncio
import aiosqlite
import aiohttp
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://swapi.dev/api/people/"
CONCURRENT_REQUESTS = 10


# ---------------------------
# Получение всех ID персонажей
# ---------------------------
async def get_all_character_ids(session):
    url = BASE_URL
    ids = []

    while url:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logging.error(f"Ошибка получения списка: {resp.status}")
                    break

                data = await resp.json()

                for person in data.get('results', []):
                    person_url = person['url']
                    person_id = int(person_url.rstrip('/').split('/')[-1])
                    ids.append(person_id)

                url = data.get('next')

        except Exception as e:
            logging.error(f"Ошибка при получении ID: {e}")
            break

    return ids


# ---------------------------
# Загрузка персонажа
# ---------------------------
async def fetch_character(session, character_id, semaphore):
    url = f"{BASE_URL}{character_id}/"

    async with semaphore:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logging.warning(f"Не удалось получить персонажа {character_id}")
                    return None

                return await resp.json()

        except Exception as e:
            logging.error(f"Ошибка загрузки {character_id}: {e}")
            return None


# ---------------------------
# Получение имени планеты
# ---------------------------
async def get_homeworld_name(session, url):
    if not url:
        return None

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None

            data = await resp.json()
            return data.get('name')

    except Exception:
        return None


# ---------------------------
# Нормализация данных
# ---------------------------
async def normalize_character(session, raw):
    if not raw:
        return None

    homeworld = await get_homeworld_name(session, raw.get('homeworld'))

    def to_float(val):
        try:
            return float(val)
        except:
            return None

    return {
        "id": int(raw['url'].rstrip('/').split('/')[-1]),
        "name": raw.get('name'),
        "birth_year": raw.get('birth_year'),
        "eye_color": raw.get('eye_color'),
        "gender": raw.get('gender'),
        "hair_color": raw.get('hair_color'),
        "homeworld": homeworld,
        "mass": to_float(raw.get('mass')),
        "skin_color": raw.get('skin_color'),
    }


# ---------------------------
# Сохранение в БД
# ---------------------------
async def save_character(db, character):
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

    await db.execute(insert_sql, (
        character['id'],
        character['birth_year'],
        character['eye_color'],
        character['gender'],
        character['hair_color'],
        character['homeworld'],
        character['mass'],
        character['name'],
        character['skin_color'],
    ))


# ---------------------------
# Основной процесс
# ---------------------------
async def main():
    async with aiosqlite.connect('swapi.db') as db:
        await db.execute("PRAGMA foreign_keys = ON")

        async with aiohttp.ClientSession() as session:
            logging.info("Получение списка ID...")
            ids = await get_all_character_ids(session)

            logging.info(f"Найдено персонажей: {len(ids)}")

            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

            # загрузка персонажей
            fetch_tasks = [
                fetch_character(session, cid, semaphore)
                for cid in ids
            ]
            raw_data = await asyncio.gather(*fetch_tasks)

            # нормализация
            normalize_tasks = [
                normalize_character(session, ch)
                for ch in raw_data if ch
            ]
            characters = await asyncio.gather(*normalize_tasks)

        logging.info("Сохранение в БД...")

        for ch in characters:
            if ch:
                await save_character(db, ch)

        await db.commit()

    logging.info("Загрузка завершена.")


if __name__ == "__main__":
    asyncio.run(main())