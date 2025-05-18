import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
import requests
from .recommendations import Recommendations

logger = logging.getLogger("uvicorn.error")

# Инициализация хранилища рекомендаций
rec_store = Recommendations()

# Адреса сервисов
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Загрузка рекомендаций при старте сервиса
    logger.info("Loading recommendations...")
    
    # Пути к файлам с рекомендациями
    personal_path = './models/recommendations.parquet'
    default_path = './models/top_popular.parquet'
    
    try:
        rec_store.load(
            "personal",
            personal_path,
            columns=["user_id", "track_id", "rank"]
        )
        rec_store.load(
            "default",
            default_path,
            columns=["track_id", "listen_pop_score"] # popularity_score
        )
    except Exception as e:
        logger.error(f"Failed to load recommendations: {str(e)}")
    
    logger.info("Service started")
    yield
    logger.info("Service stopped")

# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список офлайн-рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id, k)
    return {"recs": recs}

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    на основе трех последних событий
    """
    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # Получаем три последних события пользователя
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json().get("events", [])

    # Собираем рекомендации для каждого из последних событий
    items = []
    scores = []
    for track_id in events:
        # Получаем похожие товары для каждого события
        params = {"track_id": track_id, "k": k*2}  # Берем больше, чтобы после дедубликации хватило
        resp = requests.post(features_store_url + "/similar_items", 
                           headers=headers, 
                           params=params)
        item_similar_items = resp.json()
        
        # Добавляем в общий пул
        items += item_similar_items.get("track_id_2", [])
        scores += item_similar_items.get("score", [])

    # Сортируем по score (убывание) и дедублицируем
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]
    recs = dedup_ids(combined)[:k]  # Берем топ-k после дедубликации

    return {"recs": recs}

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает смешанный список рекомендаций длиной k для пользователя user_id
    Чередует онлайн- и офлайн-рекомендации (нечетные - онлайн, четные - офлайн)
    """
    # Получаем оба типа рекомендаций
    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []
    min_length = min(len(recs_offline), len(recs_online))
    
    # Чередуем элементы: нечетные позиции - онлайн, четные - офлайн
    for i in range(min_length):
        if i % 2 == 0:  # Нечетная позиция (индексация с 0)
            recs_blended.append(recs_online[i])
        else:  # Четная позиция
            recs_blended.append(recs_offline[i])
    
    # Добавляем оставшиеся рекомендации
    if len(recs_offline) > min_length:
        recs_blended.extend(recs_offline[min_length:])
    elif len(recs_online) > min_length:
        recs_blended.extend(recs_online[min_length:])
    
    # Удаляем дубликаты с сохранением порядка
    recs_blended = dedup_ids(recs_blended)
    
    # Оставляем только k рекомендаций
    recs_blended = recs_blended[:k]
    
    return {"recs": recs_blended}