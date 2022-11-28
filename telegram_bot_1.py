import numpy as np
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Функция для запросов
def select(query):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20220920',
                  'user':'student', 
                  'password':'dpo_python_2020'}
    data_frame = ph.read_clickhouse(query, connection=connection)
    return data_frame



default_args = {
                'owner': 'a-stefanovich',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2022, 10, 16),
               }


schedule_interval = '0 11 * * *'

my_token = '*********:************************-**********'

bot = telegram.Bot(token=my_token)

chat_id =  -*********

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bot_astefanov():

    @task()
    def extract_1():
        query =  """SELECT toDate(time) AS event_date,
            countIf(action = 'like') AS likes,
            countIf(action = 'view') AS view,
            likes/view AS ctr,
            uniqExact(user_id) AS dau
            FROM simulator_20220920.feed_actions
            WHERE toDate(time) between today() - 7 and today() - 1
            GROUP BY event_date
            ORDER BY event_date"""
        
        df_cube_1 = select(query=query)
        return df_cube_1

    
    @task()
    def graph(df_cube_1):
        sns.set_style('white')
        fg = plt.figure(figsize=(15, 10))
        fg.suptitle('Ключевые метрики за 7 дней', fontsize=18)
        axis1 = fg.add_subplot(221)
        axis2 = fg.add_subplot(222)
        axis3 = fg.add_subplot(223)
        axis4 = fg.add_subplot(224)
        sns.lineplot(data=df_cube_1, x='event_date', y='view', ax=axis1, palette="rocket", color='y')
        sns.lineplot(data=df_cube_1, x='event_date', y='likes', ax=axis2, palette="rocket", color='y')
        sns.lineplot(data=df_cube_1, x='event_date', y='dau', ax=axis3, palette="rocket", color='y')
        sns.lineplot(data=df_cube_1, x='event_date', y='ctr', ax=axis4, palette="rocket", color='y')
        axis1.set_title('Просмотры', fontsize=14)
        axis2.set_title('Лайки', fontsize=14)
        axis3.set_title('DAU', fontsize=14)
        axis4.set_title('CTR', fontsize=14)
        axis1.set(xlabel = None)
        axis2.set(xlabel = None)
        axis3.set(xlabel = None)
        axis4.set(xlabel = None)
        axis1.grid()
        axis2.grid()
        axis3.grid()
        axis4.grid()
        
        plot_all = io.BytesIO()
        plt.tight_layout()
        plt.savefig(plot_all)
        plot_all.seek(0)
        plot_all.name = 'plot_all.png'
        plt.close() 
        return plot_all
           
      
    @task()   
    def message(df_cube_1):
        yesterday = pd.Timestamp('now') - pd.DateOffset(days = 1)
        date = yesterday.date()
        dau = df_cube_1[df_cube_1.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau'].iloc[0]
        likes = df_cube_1[df_cube_1.event_date == yesterday.date()-pd.DateOffset(days = 0)]['likes'].iloc[0]
        view = df_cube_1[df_cube_1.event_date == yesterday.date()-pd.DateOffset(days = 0)]['view'].iloc[0]
        ctr = df_cube_1[df_cube_1.event_date == yesterday.date()-pd.DateOffset(days = 0)]['ctr'].iloc[0]
           
        message = '''Отчет за {}:
        Просмотры: {}
        Лайки: {}
        DAU: {}
        CTR: {:.2%}'''.format(date,view,likes,dau,ctr)
        
        return message
    
    @task
    def total_message(message, plot_all):
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id= chat_id, photo = plot_all)




    df_cube_1 = extract_1()
    message = message(df_cube_1)
    plot_all = graph(df_cube_1)
    total_message(message, plot_all)

bot_astefanov = bot_astefanov()
