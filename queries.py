import datetime
import os
from datetime import timedelta
from ra_mysql_package import *
from dotenv import load_dotenv
from models import Report

load_dotenv()

override_sql_settings({
    1: {'user_name': os.environ['SQL_USERNAME'], 'password': os.environ['SQL_PASSWORD']},
    2: {'user_name': os.environ['SQL_USERNAME'], 'password': os.environ['SQL_PASSWORD']},
})

API_BASE_URL = os.environ['BASE_URL']
HEADERS = {"API-key": os.environ['API_KEY']}


def get_radio_names(radio_ids):
    q = 'SELECT radio_id, radio_name FROM radioanalyzer.radios WHERE radio_id in %s'
    with SQLConnection(1) as conn:
        conn.execute(q, (radio_ids,))
        res = conn.fetchall(True)
    radio_names = {}
    for r in res:
        radio_names[r['radio_id']] = r['radio_name']
    return radio_names


def get_report_details(report_id: int):
    q = ('SELECT report_id, user_id, market_id, market_type, daypart_id, weeks_to_check,email_address,`limit`,`new`,last_date_used, start_time, end_time, '
         'min_spins, max_spins, days FROM radioanalyzer.music_reports WHERE '
         'report_id = %s')
    with SQLConnection(1) as conn:
        conn.execute(q, (report_id, ))
        res = conn.fetchone(True)
    return Report(**res)


def create_message_queue_entries(conn):
    q = ('SELECT report_id, last_date_used, frequency FROM radioanalyzer.music_reports WHERE being_processed != 1 AND (DATE_ADD(last_date_used, INTERVAL frequency WEEK) < NOW() '
         'OR last_date_used IS Null)')
    conn.execute(q)
    res = conn.fetchall(True)
    dt_now= datetime.now()
    jobs = []
    for r in res:
        if not r['last_date_used'] or r['last_date_used'] < dt_now - timedelta(days=7*r['frequency']):
            jobs.append({'report_id': r['report_id']})
    return jobs


def set_reports_being_processed(conn1, report_ids,  being_processed, last_data_used = None):
    last_data_query = ''
    args = ()
    if last_data_used:
        last_data_query = 'last_date_used = %s,'
        args += (last_data_used, )
    args += (being_processed, report_ids)
    q = f'UPDATE music_reports SET {last_data_query} being_processed = %s WHERE report_id IN %s'
    conn1.execute(q, args)


def get_greeting_name(user_id):
    q = 'SELECT greeting_name FROM radioanalyzer.users WHERE user_id = %s'
    with SQLConnection(1) as conn:
        conn.execute(q, (user_id, ))
        res = conn.fetchone(True)
    return res['greeting_name']


def get_competitor_market(market_id):
    q = 'SELECT radio_name FROM radioanalyzer.market_charts AS mc JOIN radioanalyzer.radios USING(radio_id) WHERE market_chart_id = %s'
    with SQLConnection(1) as conn:
        conn.execute(q, (market_id, ))
        res = conn.fetchone(True)
    return res['radio_name']


def get_tag_name(market_id):
    q = 'SELECT name FROM radioanalyzer.tags WHERE id = %s'
    with SQLConnection(1) as conn:
        conn.execute(q, (market_id,))
        res = conn.fetchone(True)
    return res['name']


def get_daypart_details(daypart_id):
    q = 'SELECT * FROM dayparts_new WHERE daypart_id = %s'
    with SQLConnection(1) as conn:
        conn.execute(q, (daypart_id, ))
        res = conn.fetchone(True)
    return res