from fastapi import FastAPI

class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, track_id):
        """
        Сохраняет событие
        """

        user_events = self.events.get(user_id, []) 
        self.events[user_id] = [track_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events.get(user_id, []) 

        return user_events

events_store = EventStore(max_events_per_user=10) 

# создаём приложение FastAPI
app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, track_id: int):
    """
    Сохраняет событие для user_id, track_id
    """

    events_store.put(user_id, track_id)

    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """

    events = events_store.get(user_id, k)

    return {"events": events}