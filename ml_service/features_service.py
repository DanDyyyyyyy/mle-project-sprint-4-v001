import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")

class SimilarItems:

    def __init__(self):

        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Loading data, type: {type}")
         # 1. Загружаем данные из parquet-файла
        self._similar_items = pd.read_parquet(path, **kwargs)
        # 2. Индексируем по track_id_enc_1 для быстрого доступа через .loc
        self._similar_items = self._similar_items.set_index("track_id_enc_1")

        self._similar_items = self._similar_items.rename(columns={"track_id_enc_1": "track_id_1", "track_id_enc_2": "track_id_2"})

        logger.info(f"Loaded")

    def get(self, track_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[track_id].head(k)
            i2i = i2i[["track_id_2", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("No recommendations found")
            i2i = {"track_id_2": [], "score": {}}

        return i2i

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    data_path = './models/similar.parquet'
    sim_items_store.load(
        
        data_path,
        columns=["score", "track_id_enc_1", "track_id_enc_2"],
    )
    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)

@app.post("/similar_items")
async def recommendations(track_id: int, k: int = 10):
    """
    Возвращает список похожих объектов длиной k для track_id
    """

    i2i = sim_items_store.get(track_id, k)

    return i2i