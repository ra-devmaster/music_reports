import base64
import os
import pandas as pd
import requests
from dotenv import load_dotenv

from queries import get_greeting_name, get_radio_names, get_competitor_market, get_tag_name, get_daypart_details

load_dotenv()


API_BASE_URL = os.environ['BASE_URL']
HEADERS = {"API-key": os.environ['API_KEY']}

def generate_attachments(job, songs, start_dt, end_dt):
    attachments = []
    market_name = job.market_name.replace(' ', '_')
    loc = os.path.dirname(os.path.abspath(__file__))
    file_name = f'{loc}/attachments/{market_name}_({start_dt})_to_({end_dt})'
    song_df = pd.DataFrame.from_dict(songs)
    song_df.to_csv(file_name + '.csv', header=True, index=False)
    attachments.append(file_name + '.csv')
    attachments.append(make_excel_nice(song_df, file_name, 'Music report'))
    html_file = open(file_name + '.html', 'w', encoding='utf-8')
    song_df.to_html(buf=html_file, classes='table table-stripped', index=False, justify='left')
    html_file.close()
    attachments.append(file_name + '.html')
    return attachments


def make_excel_nice(song_table, file_name, market_name):
    attachment = file_name + '.xlsx'
    writer = pd.ExcelWriter(attachment, engine='xlsxwriter')
    song_table.to_excel(writer, sheet_name=market_name, index=False)
    worksheet = writer.sheets[market_name]
    (max_row, max_col) = song_table.shape
    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)
    # Set the autofilter.
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    return attachment


def create_email(job, song_dict, start_dt, end_dt, daypart_name):

    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    days_str = ''
    if len(job.days) == 7:
        days_str = 'all days'
    elif job.days == [0,1,2,3,4]:
        days_str = 'weekdays'
    elif job.days == [5,6]:
        days_str = 'weekends'
    else:
        for d in job.days:
            days_str+=f'{day_names[d]}, '
        days_str = days_str[:-2]

    greeting_name = get_greeting_name(job.user_id)

    email = {'subject': f'Market report for {job.market_name} from RadioAnalyzer', 'body': f'Hi {greeting_name},<br><br>'}

    if not job.new:
        if not job.limit:
            market_type_name = 'all songs'
        else:
            market_type_name = f'top {job.limit} songs'
    else:
        market_type_name = 'new songs'

    if job.min_spins > 0 and job.max_spins > 0:
        spins_text = f'between {job.min_spins} and {job.max_spins} time(s) '
    elif job.min_spins > 0 and job.max_spins == 0:
        spins_text = f'more then {job.min_spins} time(s) '
    elif job.min_spins == 0 and job.max_spins > 0:
        spins_text = f'under {job.max_spins} time(s) '
    else:
        spins_text = 'at least 1 time(s) '

    email['body'] += 'There were no results for ' if len(song_dict) < 1 else 'Here is your '
    email['body'] += f'{market_type_name} for {job.weeks_to_check} {"week" if job.weeks_to_check == 1 else "weeks"} of data on {job.market_name}.<br>'
    email['body'] += f'Containing songs played {spins_text}'
    email['body'] += f'between {start_dt} and {end_dt} for '
    if daypart_name:
        email['body'] += f'{daypart_name}. '
    else:
        email['body'] += f'{days_str} from hour {job.start_time} to hour {job.end_time}. '

    email['body'] += '<br><br>Have a question about this or any other report? Reach out to us at support@radioanalyzer.com'
    email['body'] += '<br>Your RadioAnalyzer Team<br>'

    return email


def get_song_list(job):
    start_time =  "{:0>8}".format(str(job.start_time))
    end_time = "{:0>8}".format(str(job.end_time))
    api_url = (f'{API_BASE_URL}/charts/{"radio" if job.market_type.value == 0 else "market"}/songs/{"new/" if job.new else ""}?id='
                         f'{job.market_id}'
               f'{"" if job.market_type.value == 0 else f"&market_type={job.market_type.name.lower()}"}&start_dt={job.start_dt}&end_dt={job.end_dt}&min_spins'
               f'={job.min_spins}&max_spins'
               f'={job.max_spins}&start_time={start_time}'
                         f'&end_time={end_time}{'&limit=' + job.limit if job.limit else ''}&user_id={job.user_id}')
    resp = requests.post(api_url, headers=HEADERS, json=job.days)
    return resp.json()


def send_api(email, subject, body, attachments):
    attachments_encoded = []
    # return True
    for a in attachments:
        file_name = a.split('/')[-1]
        with open(a, 'rb') as f:
            b64 = base64.b64encode(f.read()).decode()
            attachments_encoded.append({'content': b64, 'filename':file_name})

    data = {
        'recipients': email,
        'subject': subject,
        'body': body,
        'attachments': attachments_encoded
    }
    resp = requests.post(f'{API_BASE_URL}/emails/send_email/', headers=HEADERS, json=data)
    return resp.status_code == 204



def get_market_name(market_id, market_type):
    match market_type.value:
        case 0:
            radio_name = get_radio_names([market_id])[market_id]
            market_name = radio_name
        case 1:
            radio_name = get_competitor_market(market_id)
            market_name = f'Competitor market for {radio_name}'
        case 2:
            radio_name = get_radio_names([market_id])[market_id]
            market_name = f'Airplay market for {radio_name}'
        case 3:
            market_name = get_tag_name(market_id)
        case _:
            raise Exception('Invalid market type')
    return market_name


def fit_daypart_details(job):
    daypart = get_daypart_details(job.daypart_id)
    job.start_time = daypart['start_time']
    job.end_time = daypart['end_time']
    job.days = [int(x) for x in daypart['days'].split(',')]
    return job, daypart['name']