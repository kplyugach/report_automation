import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse
import io
import os
import telegram
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


sns.set()


class Getch:
    def __init__(self, query, db='simulator_20221120'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)


def extract_data_app(ti):
    data_app = Getch('''       
                select date, uniqExact(user_id) as users  
                from (select distinct toDate(time) as date, user_id
                from {db}.feed_actions
                where toDate(time) between today() - 8 and today() - 1 
                union all
                select distinct toDate(time) as date, user_id
                from {db}.message_actions
                where toDate(time) between today() - 8 and today() - 1) as t
                group by date
                order by date
                ''').df
    
    data_app['date'] = pd.to_datetime(data_app['date']).dt.date
    data_app = data_app.astype({'users': int})
    
    ti.xcom_push(key='data_app', value=data_app)


def extract_data_new_users(ti):
    data_new_users = Getch('''
                select date, uniqExact(user_id) as new_users 
                from (select user_id, min(min_date) as date
                from (select user_id, min(toDate(time)) as min_date
                from {db}.feed_actions
                where toDate(time) between today() - 90 and today() - 1
                group by user_id
                union all
                select user_id, min(toDate(time)) as min_date
                from {db}.message_actions
                where toDate(time) between today() - 90 and today() - 1
                group by user_id) as t
                group by user_id) as tab
                where date between today() - 8 and today() - 1
                group by date 
                ''').df

    data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date
    data_new_users = data_new_users.astype({'new_users': int})

    ti.xcom_push(key='data_new_users', value=data_new_users)


def extract_data_feed(ti):
    data_feed = Getch('''
            select toDate(time) as date, 
            uniqExact(user_id) as users_feed, 
            countIf(user_id, action = 'view') as views, 
            countIf(user_id, action = 'like') as likes, 
            100 * likes / views as ctr, 
            views + likes as events, 
            uniqExact(post_id) as posts, 
            likes / users_feed as lpu   
            from {db}.feed_actions 
            where toDate(time) between today() - 8 and today() - 1 
            group by date
            order by date
            ''').df

    data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
    data_feed = data_feed.astype({'users_feed': int, 'views': int, 'likes': int, 'events': int, 'posts': int})

    ti.xcom_push(key='data_feed', value=data_feed)


def extract_data_message(ti):
    data_message = Getch('''
            select toDate(time) as date, 
            uniqExact(user_id) as users_message, 
            count(user_id) as messages, 
            messages / users_message as mpu
            from {db}.message_actions 
            where toDate(time) between today() - 8 and today() - 1 
            group by date
            order by date
            ''').df

    data_message['date'] = pd.to_datetime(data_message['date']).dt.date
    data_message = data_message.astype({'users_message': int, 'messages': int})

    ti.xcom_push(key='data_message', value=data_message)


def feed_report(ti):
    data_app = ti.xcom_pull(key='data_app', task_ids='extract_data_app')
    data_new_users = ti.xcom_pull(key='data_new_users', task_ids='extract_data_new_users')
    data_feed = ti.xcom_pull(key='data_feed', task_ids='extract_data_feed')
    data_message = ti.xcom_pull(key='data_message', task_ids='extract_data_message')

    message = '''
            App Report {date}

            App:
            Events: {events}
            DAU: {users} ({to_users_day_ago:+.2%}) by day ago, ({to_users_week_ago:+.2%}) by week ago
            New users: {new_users} ({to_new_users_day_ago:+.2%}) by day ago, ({to_new_users_week_ago:+.2%}) by week ago

            Feed:
            DAU: {users_feed} ({to_users_feed_day_ago:+.2%}) by day ago, ({to_users_feed_week_ago:+.2%}) by week ago
            Posts: {posts} ({to_posts_day_ago:+.2%}) by day ago, ({to_posts_week_ago:+.2%}) by week ago
            Views: {views} ({to_views_day_ago:+.2%}) by day ago, ({to_views_week_ago:+.2%}) by week ago
            Likes: {likes} ({to_likes_day_ago:+.2%}) by day ago, ({to_likes_week_ago:+.2%}) by week ago
            CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%}) by day ago, ({to_ctr_week_ago:+.2%}) by week ago
            Likes per user: {lpu:.2f}% ({to_lpu_day_ago:+.2%}) by day ago, ({to_lpu_week_ago:+.2%}) by week ago

            Message:
            DAU: {users_message} ({to_users_message_day_ago:+.2%}) by day ago, ({to_users_message_week_ago:+.2%}) by week ago
            Messages: {messages} ({to_messages_day_ago:+.2%}) by day ago, ({to_messages_week_ago:+.2%}) by week ago
            Messages per user: {mpu:.2f}% ({to_mpu_day_ago:+.2%}) by day ago, ({to_mpu_week_ago:+.2%}) by week ago
            '''

    today = pd.Timestamp('now') - pd.DateOffset(days=1)
    day_ago = today - pd.DateOffset(days=1)
    week_ago = today - pd.DateOffset(days=7)

    report = message.format(date=today.date(),
                            events=data_message[data_message['date'] == today.date()]['messages'].iloc[0]
                                   + data_feed[data_feed['date'] == today.date()]['events'].iloc[0],
                            users=data_app[data_app['date'] == today.date()]['users'].iloc[0],
                            to_users_day_ago=(data_app[data_app['date'] == today.date()]['users'].iloc[0]
                                              - data_app[data_app['date'] == day_ago.date()]['users'].iloc[0])
                                              / data_app[data_app['date'] == day_ago.date()]['users'].iloc[0],
                            to_users_week_ago=(data_app[data_app['date'] == today.date()]['users'].iloc[0]
                                               - data_app[data_app['date'] == week_ago.date()]['users'].iloc[0])
                                               / data_app[data_app['date'] == week_ago.date()]['users'].iloc[0],

                            new_users=data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0],
                            to_new_users_day_ago=(data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                                  - data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0])
                                                  / data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0],
                            to_new_users_week_ago=(data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                                   - data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0])
                                                   / data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0],

                            users_feed=data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0],
                            to_users_feed_day_ago=(data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0]
                                                   - data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0])
                                                   / data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0],
                            to_users_feed_week_ago=(data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0]
                                                    - data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0])
                                                    / data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0],

                            posts=data_feed[data_feed['date'] == today.date()]['posts'].iloc[0],
                            to_posts_day_ago=(data_feed[data_feed['date'] == today.date()]['posts'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0],
                            to_posts_week_ago=(data_feed[data_feed['date'] == today.date()]['posts'].iloc[0]
                                               - data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0])
                                               / data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0],

                            views=data_feed[data_feed['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago=(data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0],
                            to_views_week_ago=(data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                               - data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0])
                                               / data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0],

                            likes=data_feed[data_feed['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago=(data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0],
                            to_likes_week_ago=(data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                               - data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0])
                                               / data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0],

                            ctr=data_feed[data_feed['date'] == today.date()]['ctr'].iloc[0],
                            to_ctr_day_ago=(data_feed[data_feed['date'] == today.date()]['ctr'].iloc[0]
                                            - data_feed[data_feed['date'] == day_ago.date()]['ctr'].iloc[0])
                                            / data_feed[data_feed['date'] == day_ago.date()]['ctr'].iloc[0],
                            to_ctr_week_ago=(data_feed[data_feed['date'] == today.date()]['ctr'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['ctr'].iloc[0])
                                             / data_feed[data_feed['date'] == week_ago.date()]['ctr'].iloc[0],

                            lpu=data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0],
                            to_lpu_day_ago=(data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0]
                                            - data_feed[data_feed['date'] == day_ago.date()]['lpu'].iloc[0])
                                            / data_feed[data_feed['date'] == day_ago.date()]['lpu'].iloc[0],
                            to_lpu_week_ago=(data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['lpu'].iloc[0])
                                             / data_feed[data_feed['date'] == week_ago.date()]['lpu'].iloc[0],

                            users_message=data_message[data_message['date'] == today.date()]['users_message'].iloc[0],
                            to_users_message_day_ago=(data_message[data_message['date'] == today.date()]['users_message'].iloc[0]
                                                      - data_message[data_message['date'] == day_ago.date()]['users_message'].iloc[0])
                                                      / data_message[data_message['date'] == day_ago.date()]['users_message'].iloc[0],
                            to_users_message_week_ago=(data_message[data_message['date'] == today.date()]['users_message'].iloc[0]
                                                       - data_message[data_message['date'] == week_ago.date()]['users_message'].iloc[0])
                                                       / data_message[data_message['date'] == week_ago.date()]['users_message'].iloc[0],

                            messages=data_message[data_message['date'] == today.date()]['messages'].iloc[0],
                            to_messages_day_ago=(data_message[data_message['date'] == today.date()]['messages'].iloc[0]
                                                 - data_message[data_message['date'] == day_ago.date()]['messages'].iloc[0])
                                                 / data_message[data_message['date'] == day_ago.date()]['messages'].iloc[0],
                            to_messages_week_ago=(data_message[data_message['date'] == today.date()]['messages'].iloc[0]
                                                  - data_message[data_message['date'] == week_ago.date()]['messages'].iloc[0])
                                                  / data_message[data_message['date'] == week_ago.date()]['messages'].iloc[0],

                            mpu=data_message[data_message['date'] == today.date()]['mpu'].iloc[0],
                            to_mpu_day_ago=(data_message[data_message['date'] == today.date()]['mpu'].iloc[0]
                                            - data_message[data_message['date'] == day_ago.date()]['mpu'].iloc[0])
                                            / data_message[data_message['date'] == day_ago.date()]['mpu'].iloc[0],
                            to_mpu_week_ago=(data_message[data_message['date'] == today.date()]['mpu'].iloc[0]
                                             - data_message[data_message['date'] == week_ago.date()]['mpu'].iloc[0])
                                             / data_message[data_message['date'] == week_ago.date()]['mpu'].iloc[0])

    ti.xcom_push(key='report', value=report)


def get_plot(ti):
    data_app = ti.xcom_pull(key='data_app', task_ids='extract_data_app')
    data_new_users = ti.xcom_pull(key='data_new_users', task_ids='extract_data_new_users')
    data_feed = ti.xcom_pull(key='data_feed', task_ids='extract_data_feed')
    data_message = ti.xcom_pull(key='data_message', task_ids='extract_data_message')

    data = pd.merge(data_feed, data_message, on='date')
    data = pd.merge(data, data_new_users, on='date')
    data = pd.merge(data, data_app, on='date')

    data['events_app'] = data['events'] + data['messages']

    plot_objects = []

    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('App statistics by the 7 days')

    app_dict = {0: {'y': 'events_app', 'title': 'Events'},
                1: {'y': 'users', 'title': 'DAU'},
                2: {'y': 'new_users', 'title': 'New users'}
               }

    for i in range(3):
        for j in range(2):
            sns.lineplot(ax=axes[i], data=data, x='date', y=app_dict[i]['y'])
            axes[i].set_title(app_dict[i]['title'])
            axes[i].set(xlabel=None)
            axes[i].set(ylabel=None)
            for ind, label in enumerate(axes[i].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'app_stat.png'
    plot_object.seek(0)
    plt.close()

    plot_objects.append(plot_object)

    fig, axes = plt.subplots(2, 2, figsize=(14, 14))
    fig.suptitle('Feed statistics by the 7 days')

    plot_dict = {(0, 0): {'y': 'users_feed', 'title': 'DAU'},
                 (0, 1): {'y': 'views', 'title': 'Views'},
                 (1, 0): {'y': 'likes', 'title': 'Likes'},
                 (1, 1): {'y': 'ctr', 'title': 'CTR'} 
                }

    for i in range(2):
        for j in range(2):
            sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
            axes[i, j].set_title(plot_dict[(i, j)]['title'])
            axes[i, j].set(xlabel=None)
            axes[i, j].set(ylabel=None)
            for ind, label in enumerate(axes[i, j].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'feed_stat.png'
    plot_object.seek(0)
    plt.close()

    plot_objects.append(plot_object)

    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('Message statistics by the 7 days')

    message_dict = {0: {'y': 'users_message', 'title': 'DAU'},
                    1: {'y': 'messages', 'title': 'Messages'},
                    2: {'y': 'mpu', 'title': 'Messages per user'}
                   }

    for i in range(3):
        sns.lineplot(ax=axes[i], data=data, x='date', y=message_dict[i]['y'])
        axes[i].set_title(message_dict[i]['title'])
        axes[i].set(xlabel=None)
        axes[i].set(ylabel=None)
        for ind, label in enumerate(axes[i].get_xticklabels()):
            if ind % 3 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'message_stat.png'
    plot_object.seek(0)
    plt.close()

    plot_objects.append(plot_object)

    ti.xcom_push(key='plot_objects', value=plot_objects)

def send(ti):
    report = ti.xcom_pull(key='report', task_ids='feed_report')
    plot_objects = ti.xcom_pull(key='plot_objects', task_ids='get_plot')
    
    chat_id = -817095409
    my_token = os.environ.get('REPORT_BOT_TOKEN')
    bot = telegram.Bot(token=my_token)
    
    bot.sendMessage(chat_id=chat_id, text=report)
    for plot_object in plot_objects:
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)


default_args = {
    'owner': 'k-pljugach-13',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 14)
}


schedule_interval = '0 11 * * *'


dag = DAG('dag_app_report', default_args=default_args, schedule_interval=schedule_interval, catchup=False)


t1 = PythonOperator(task_id='extract_data_app',
                    python_callable=extract_data_app,
                    dag=dag)

t2 = PythonOperator(task_id='extract_data_new_users',
                    python_callable=extract_data_new_users,
                    dag=dag)

t3 = PythonOperator(task_id='extract_data_feed',
                    python_callable=extract_data_feed,
                    dag=dag)

t4 = PythonOperator(task_id='extract_data_message',
                    python_callable=extract_data_message,
                    dag=dag)

t5 = PythonOperator(task_id='feed_report',
                    python_callable=feed_report,
                    dag=dag)

t6 = PythonOperator(task_id='get_plot',
                    python_callable=get_plot,
                    dag=dag)

t7 = PythonOperator(task_id='send',
                    python_callable=send,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> [t5, t6] >> t7
