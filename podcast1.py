from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pendulum
import requests
import xmltodict

@dag(
    dag_id = 'podcast_summary',
    schedule_interval=timedelta(days=1),
    start_date= pendulum.datetime(2023,3, 21),
    catchup = False
)

def podcast_summary():
    @task()
    def get_episodes():
        data = requests.get('https://www.marketplace.org/feed/podcast/marketplace/')
        feed = xmltodict.parse(data.text)
        episodes = feed['rss']['channel']['item']
        print("number of episodes: ", len(episodes))
        return episodes
    
    podcast_episodes = get_episodes()



summary = podcast_summary()