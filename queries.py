import datetime
import os
from datetime import timedelta
from ra_mysql_package import *
from dotenv import load_dotenv

load_dotenv()

override_sql_settings({
    1: {'user_name': os.environ['SQL_USERNAME'], 'password': os.environ['SQL_PASSWORD']},
    2: {'user_name': os.environ['SQL_USERNAME'], 'password': os.environ['SQL_PASSWORD']},
})

API_BASE_URL = os.environ['BASE_URL']
HEADERS = {"API-key": os.environ['API_KEY']}


def get_radio_names(radio_ids):
    try:
        q = 'SELECT radio_id, radio_name FROM radioanalyzer.radios WHERE radio_id in %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (radio_ids,))
            res = conn.fetchall(True)
        radio_names = {}
        for r in res:
            radio_names[r['radio_id']] = r['radio_name']
        return radio_names
    except Exception as e:
        raise Exception(f'Failed to get radio_names: {repr(e)}')

def get_report_details(report_id: int):
    try:
        q = ('SELECT report_id, user_id, market_id, market_type, daypart_id, weeks_to_check,email_address,`limit`,`new`,last_date_used, start_time, end_time, '
             'min_spins, max_spins, days FROM radioanalyzer.music_reports WHERE '
             'report_id = %s')
        with SQLConnection(1) as conn:
            conn.execute(q, (report_id, ))
            res = conn.fetchone(True)
        return res
    except Exception as e:
        raise Exception(f'Failed to get report details: {repr(e)}')


def create_message_queue_entries(conn):
    try:
        q = ('SELECT report_id FROM radioanalyzer.music_reports AS mp JOIN users AS u USING(user_id) WHERE being_processed != 1 AND (DATE_ADD(last_date_used, INTERVAL frequency '
             'WEEK) < NOW() '
             'OR last_date_used IS Null) AND is_disabled = 0')
        conn.execute(q)
        res = conn.fetchall(True)
        return res
    except Exception as e:
        raise Exception(f'Failed to create messages: {repr(e)}')


def set_reports_being_processed(conn1, report_ids,  being_processed, last_data_used = None):
    try:
        last_data_query = ''
        args = ()
        if last_data_used:
            last_data_query = 'last_date_used = %s,'
            args += (last_data_used, )
        args += (being_processed, report_ids)
        q = f'UPDATE music_reports SET {last_data_query} being_processed = %s WHERE report_id IN %s'
        conn1.execute(q, args)
    except Exception as e:
        raise Exception(f'Failed to update reports: {repr(e)}')


def get_greeting_name(user_id):
    try:
        q = 'SELECT greeting_name FROM radioanalyzer.users WHERE user_id = %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (user_id, ))
            res = conn.fetchone(True)
        return res['greeting_name']
    except Exception as e:
        raise Exception(f'Failed to get greeting name: {repr(e)}')


def get_competitor_market(market_id):
    try:
        q = 'SELECT radio_name FROM radioanalyzer.market_charts AS mc JOIN radioanalyzer.radios USING(radio_id) WHERE market_chart_id = %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (market_id, ))
            res = conn.fetchone(True)
        return res['radio_name']
    except Exception as e:
        raise Exception(f'Failed to get competitor marekt: {repr(e)}')


def get_tag_name(market_id):
    try:
        q = 'SELECT name FROM radioanalyzer.tags WHERE id = %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (market_id,))
            res = conn.fetchone(True)
        return res['name']
    except Exception as e:
        raise Exception(f'Failed to get tag name: {repr(e)}')


def get_daypart_details(daypart_id):
    try:
        q = 'SELECT * FROM dayparts_new WHERE daypart_id = %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (daypart_id, ))
            res = conn.fetchone(True)
        return res
    except Exception as e:
        raise Exception(f'Failed to get daypart details: {repr(e)}')