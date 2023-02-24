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


def extract_data(ti):
    data = Getch('''
                select toDate(time) as date, uniqExact(user_id) as users, 
                countIf(user_id, action = 'view') as views,
                countIf(user_id, action = 'like') as likes, 
                (likes / views) * 100 as ctr
                from {db}.feed_actions 
                where toDate(time) between today() - 8 and today() - 1 
                group by date
                order by date
                ''').df

    data['date'] = pd.to_datetime(data['date']).dt.date
    data = data.astype({'users': int, 'views': int, 'likes': int})
    
    ti.xcom_push(key='data', value=data)


def feed_report(ti):
    data = ti.xcom_pull(key='data', task_ids='extract_data')
    
    message = '''
            Feed Report {date}

            DAU: {users} ({to_users_day_ago:+.2%}) by day ago, ({to_users_week_ago:+.2%}) by week ago
            Views: {views} ({to_views_day_ago:+.2%}) by day ago, ({to_views_week_ago:+.2%}) by week ago
            Likes: {likes} ({to_likes_day_ago:+.2%}) by day ago, ({to_likes_week_ago:+.2%}) by week ago
            CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%}) by day ago, ({to_ctr_week_ago:+.2%}) by week ago
            '''

    today = pd.Timestamp('now') - pd.DateOffset(days=1)
    day_ago = today - pd.DateOffset(days=1)
    week_ago = today - pd.DateOffset(days=7)

    report = message.format(date=today.date(),
                            users=data[data['date'] == today.date()]['users'].iloc[0],
                            to_users_day_ago=(data[data['date'] == today.date()]['users'].iloc[0]
                                              - data[data['date'] == day_ago.date()]['users'].iloc[0])
                                              / data[data['date'] == day_ago.date()]['users'].iloc[0],
                            to_users_week_ago=(data[data['date'] == today.date()]['users'].iloc[0]
                                               - data[data['date'] == week_ago.date()]['users'].iloc[0])
                                               / data[data['date'] == week_ago.date()]['users'].iloc[0],
                            views=data[data['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago=(data[data['date'] == today.date()]['views'].iloc[0]
                                              - data[data['date'] == day_ago.date()]['views'].iloc[0])
                                              / data[data['date'] == day_ago.date()]['views'].iloc[0],
                            to_views_week_ago=(data[data['date'] == today.date()]['views'].iloc[0]
                                               - data[data['date'] == week_ago.date()]['views'].iloc[0])
                                               / data[data['date'] == week_ago.date()]['views'].iloc[0],
                            likes=data[data['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago=(data[data['date'] == today.date()]['likes'].iloc[0]
                                              - data[data['date'] == day_ago.date()]['likes'].iloc[0])
                                              / data[data['date'] == day_ago.date()]['likes'].iloc[0],
                            to_likes_week_ago=(data[data['date'] == today.date()]['likes'].iloc[0]
                                               - data[data['date'] == week_ago.date()]['likes'].iloc[0])
                                               / data[data['date'] == week_ago.date()]['likes'].iloc[0],
                            ctr=data[data['date'] == today.date()]['ctr'].iloc[0],
                            to_ctr_day_ago=(data[data['date'] == today.date()]['ctr'].iloc[0]
                                            - data[data['date'] == day_ago.date()]['ctr'].iloc[0])
                                            / data[data['date'] == day_ago.date()]['ctr'].iloc[0],
                            to_ctr_week_ago=(data[data['date'] == today.date()]['ctr'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['ctr'].iloc[0])
                                             / data[data['date'] == week_ago.date()]['ctr'].iloc[0])

    ti.xcom_push(key='report', value=report)


def get_plot(ti):
    data = ti.xcom_pull(key='data', task_ids='extract_data')
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Feed statistics by the 7 days')

    plot_dict = {(0, 0): {'y': 'users', 'title': 'DAU'},
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

    ti.xcom_push(key='plot_object', value=plot_object)


def send(ti):
    report = ti.xcom_pull(key='report', task_ids='feed_report')
    plot_object = ti.xcom_pull(key='plot_object', task_ids='get_plot')
    
    chat_id = -817095409
    my_token = os.environ.get('REPORT_BOT_TOKEN')
    bot = telegram.Bot(token=my_token)
    
    bot.sendMessage(chat_id=chat_id, text=report)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


default_args = {
    'owner': 'k-pljugach-13',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 14)
}


schedule_interval = '0 11 * * *'


dag = DAG('dag_feed_report', default_args=default_args, schedule_interval=schedule_interval, catchup=False)


t1 = PythonOperator(task_id='extract_data',
                    python_callable=extract_data,
                    dag=dag)

t2 = PythonOperator(task_id='feed_report',
                    python_callable=feed_report,
                    dag=dag)

t3 = PythonOperator(task_id='get_plot',
                    python_callable=get_plot,
                    dag=dag)

t4 = PythonOperator(task_id='send',
                    python_callable=send,
                    dag=dag)

t1 >> [t2, t3] >> t4
