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
                'start_date': datetime(2022, 10, 17),
               }

schedule_interval = '0 11 * * *'

my_token = '*********:************************-**********'

bot = telegram.Bot(token=my_token)

chat_id =  -*********



@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_bot_astefanov():
    
    @task
    def extract_1():
        query =  """SELECT toDate(time) AS event_date,
                countIf(action = 'like') AS likes,
                countIf(action = 'view') AS view,
                likes/view AS ctr,
                uniqExact(user_id) AS dau_feed,
                uniqExact(post_id) AS post
                FROM simulator_20220920.feed_actions
                WHERE toDate(time) between today() - 8 and today() - 1
                GROUP BY event_date
                ORDER BY event_date"""
        df_feed = select(query=query)
        return df_feed
    

    @task()
    def extract_2():
        query1 =  """SELECT toDate(time) AS event_date,
                uniqExact(user_id) AS dau_mess,
                count(user_id) AS count_mess
                FROM simulator_20220920.message_actions
                WHERE toDate(time) between today() - 8 and today() - 1
                GROUP BY event_date
                ORDER BY event_date"""
            
        df_mess = select(query=query1)
        return df_mess
    
    @task()
    def extract_3():
        query2 = '''
                SELECT 
                event_date,
                uniqExact(user_id) as users,
                uniqExactIf(user_id, os = 'Android') as u_android,
                uniqExactIf(user_id, os = 'iOS') as u_ios
    
                FROM
                (
                SELECT DISTINCT
                toDate(time) AS event_date,
                user_id,
                os
                FROM simulator_20220920.feed_actions
                WHERE toDate(time) between today() - 8 and today() - 1
    
                UNION ALL
    
                SELECT DISTINCT
                toDate(time) AS event_date,
                user_id,
                os
                FROM simulator_20220920.message_actions
                WHERE toDate(time) between today() - 8 and today() - 1
                ) t1
                GROUP BY event_date
                ORDER BY event_date'''
    
        df_dau = select(query=query2)
        return df_dau
    
    @task()
    def extract_4():
        query3 = '''
                SELECT 
                event_date,
                uniqExact(user_id) as new_users,
                uniqExactIf(user_id, source = 'organic') as org_users,
                uniqExactIf(user_id, source = 'ads') as ads_users
    
                FROM (
    
                SELECT
                user_id,
                source,
                min(start_date) as event_date   
                
                FROM
                
                (
                SELECT 
                min(toDate(time)) AS start_date,
                user_id,
                source
                FROM simulator_20220920.feed_actions
                WHERE toDate(time) between today() - 30 and today() - 1
                GROUP BY user_id, source
    
                UNION ALL
    
                SELECT 
                min(toDate(time)) AS start_date,
                user_id,
                source
                FROM simulator_20220920.message_actions
                WHERE toDate(time) between today() - 30 and today() - 1
                GROUP BY user_id, source
                ) t1
                GROUP BY user_id, source
                ) t2
                GROUP BY event_date
                HAVING event_date between today() - 8 and today() - 1            
                '''
    
        df_new = select(query=query3)
        return df_new
    
    
    
    @task()
    def transform(df_feed,df_mess,df_dau,df_new):
        df_total = pd.merge(df_feed, df_mess, on = 'event_date')
        df_total = pd.merge(df_total, df_dau, on = 'event_date')
        df_total = pd.merge(df_total, df_new, on = 'event_date')
        
        return df_total
    
    
    
    @task()
    def message(df_total):
        yesterday = pd.Timestamp('now') - pd.DateOffset(days = 1)
        date = yesterday.date()
        dau = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau_feed'].iloc[0].astype('int')
        likes =df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['likes'].iloc[0].astype('int')
        view = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['view'].iloc[0].astype('int')
        ctr = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['ctr'].iloc[0]
        post = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['post'].iloc[0].astype('int')
        daum = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau_mess'].iloc[0].astype('int')
        mess = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['count_mess'].iloc[0].astype('int')
        dauall = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['users'].iloc[0].astype('int')
        dauand = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['u_android'].iloc[0].astype('int')
        dauios = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['u_ios'].iloc[0].astype('int')
        new = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['new_users'].iloc[0].astype('int')
        newads = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['ads_users'].iloc[0].astype('int')
        neworg = df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['org_users'].iloc[0].astype('int')
        
        dauret = (df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['users'].iloc[0].astype('int')\
        - df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 1)]['users'].iloc[0].astype('int'))\
        /df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 1)]['users'].iloc[0].astype('int')
        
        dauweek = (df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['users'].iloc[0].astype('int')\
        - df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 7)]['users'].iloc[0].astype('int'))\
        /df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 7)]['users'].iloc[0].astype('int')
        
        
        daufeed1 = (df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau_feed'].iloc[0].astype('int')\
        - df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 1)]['dau_feed'].iloc[0].astype('int'))\
        /df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 1)]['dau_feed'].iloc[0].astype('int')
        
        daufeed7 = (df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau_feed'].iloc[0].astype('int')\
        - df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 7)]['dau_feed'].iloc[0].astype('int'))\
        /df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 7)]['dau_feed'].iloc[0].astype('int')
        
        daum1 = (df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau_mess'].iloc[0].astype('int')\
           - df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 1)]['dau_mess'].iloc[0].astype('int'))\
           / df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 1)]['dau_mess'].iloc[0].astype('int')
        
        
        daum7 = (df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 0)]['dau_mess'].iloc[0].astype('int')\
           - df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 7)]['dau_mess'].iloc[0].astype('int'))\
           / df_total[df_total.event_date == yesterday.date()-pd.DateOffset(days = 7)]['dau_mess'].iloc[0].astype('int')
        
              
        message = f'''Отчет за {date} для всего приложения:
DAU: {dauall}, {dauret:+.2%} к предыдущему дню, {dauweek:+.2%} за неделю
В том числе:
Пользователи Android: {dauand}
Пользователи iOS: {dauios}
Количество новых пользователей: {new}
Из них:
{newads} пришли через рекламу
{neworg} органические пользователи

Данные для ленты новостей:
Просмотры: {view} 
Лайки: {likes}
DAU: {dau}, {daufeed1:+.2%} к предыдущему дню, {daufeed7:+.2%} за неделю
CTR: {ctr:.2%}
Посты: {post}

Данные для мессенджера:
DAU: {daum}, {daum1:+.2%} к предыдущему дню, {daum7:+.2%} за неделю
Количество сообщений: {mess}'''
                
        return message
    
    
    @task()
    def graph(df_total):
          
        fig, axes = plt.subplots(2, figsize = (12,12))
        fig.suptitle ('Статистика приложения за неделю', fontsize=18)
        all_dict = {0:{'y':['users','u_android','u_ios'], 'title' : 'DAU'}, 1:{'y':['new_users','org_users','ads_users'], 'title' : 'Новые пользователи'}}
        
        
        for i in range(2):
            for y in all_dict[i]['y']:
                sns.lineplot(ax = axes[i], data = df_total, x = 'event_date', y = y)
            axes[i].set_title(all_dict[i]['title'])
            axes[i].grid()
            axes[i].legend(all_dict[i]['y'], bbox_to_anchor=( 1.02 , 1 ), loc='upper left')
         
        
        plot = io.BytesIO()
        plt.tight_layout()
        plt.savefig(plot)
        plot.seek(0)
        plot.name = 'plot.png'
        plt.close()           
            
        return plot
    
    
    
    @task()
    def total_message(message, plot):
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id = chat_id, photo = plot)
        
        
    df_feed = extract_1()
    df_mess = extract_2()
    df_dau = extract_3()
    df_new = extract_4()
    df_total = transform(df_feed,df_mess,df_dau,df_new)
    
    message = message(df_total)
    plot = graph(df_total)
    total_message(message, plot)
        
a_bot_astefanov = a_bot_astefanov()
        

        
        
        
