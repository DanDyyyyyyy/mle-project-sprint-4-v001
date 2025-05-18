import requests
import logging
import sys

recommendations_url = "http://127.0.0.1:8000"
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

logging.basicConfig(filename='test_service.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def get_recommendations(user_id):
    params = {"user_id": user_id, 'k': 10}
    try:
        resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
        resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
        resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

        recs_offline = resp_offline.json()["recs"]
        recs_online = resp_online.json()["recs"]
        recs_blended = resp_blended.json()["recs"]

        print(recs_offline)
        print(recs_online)
        print(recs_blended)

        logging.info(f'Оффлайн-рекомендации: {recs_offline}')
        logging.info(f'Онлайн-рекомендации: {recs_online}')
        logging.info(f'Смешанные рекомендации: {recs_blended}')

    except Exception as ex:
        logging.error(f'Ошибка: {ex}')
        sys.exit() 


### без персональных
logging.info('---------- Без персональных')
get_recommendations(830)

### c персональными но без онлайн истории
logging.info('---------- С персональными но без онлайн истории')
get_recommendations(1374582)

### c персональными но с добавлением истории
user_id = 1374582
event_track_ids = [99262, 94815761, 590262, 135]

try:
    for event_track_id in event_track_ids:
        resp = requests.post(events_store_url + "/put", 
                            headers=headers, 
                            params={"user_id": user_id, "track_id": event_track_id})
except Exception as ex:
    logging.error(f'Ошибка: {ex}')
    sys.exit() 
    
logging.info('---------- С персональными и с добавлением истории')
get_recommendations(1374582)