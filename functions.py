import base64
import os
import pandas as pd
import requests
from dotenv import load_dotenv

from queries import get_greeting_name, get_radio_names, get_competitor_market, get_tag_name, get_daypart_details

load_dotenv()


API_BASE_URL = os.environ['BASE_URL']
HEADERS = {"API-key": os.environ['API_KEY']}

def generate_attachments(report, songs, start_dt, end_dt):
    attachments = []
    market_name = get_market_name(report.market_id, report.market_type).replace(' ', '_')
    loc = os.path.dirname(os.path.abspath(__file__))
    file_name = f'{loc}/attachments/{market_name}_({start_dt})_to_({end_dt})'
    song_df = pd.DataFrame.from_dict(songs)
    song_df.to_csv(file_name + '.csv', header=True, index=False)
    attachments.append(file_name + '.csv')
    attachments.append(make_excel_nice(song_df, file_name, id))
    html_file = open(file_name + '.html', 'w', encoding='utf-8')
    song_df.to_html(buf=html_file, classes='table table-stripped', index=False, justify='left')
    html_file.close()
    attachments.append(file_name + '.html')
    return attachments


def make_excel_nice(song_table, file_name, id):
    id = str(id)
    attachment = file_name + '.xlsx'
    writer = pd.ExcelWriter(attachment, engine='xlsxwriter')
    song_table.to_excel(writer, sheet_name=id, index=False)
    worksheet = writer.sheets[id]
    (max_row, max_col) = song_table.shape
    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)
    # Set the autofilter.
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    return attachment


def create_email(report, song_dict, start_dt, end_dt, daypart_name):

    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    days_str = ''
    if len(report.days) == 7:
        days_str = 'all days'
    elif report.days == [0,1,2,3,4]:
        days_str = 'weekdays'
    elif report.days == [5,6]:
        days_str = 'weekends'
    else:
        for d in report.days:
            days_str+=f'{day_names[d]}, '
        days_str = days_str[:-2]

    greeting_name = get_greeting_name(report.user_id)
    market_name = get_market_name(report.market_id, report.market_type)

    email = {'subject': f'Market report for {market_name} from RadioAnalyzer', 'body': f'Hi {greeting_name},<br><br>'}

    if not report.new:
        if not report.limit:
            market_type_name = 'all songs'
        else:
            market_type_name = f'top {report.limit} songs'
    else:
        market_type_name = 'new songs'

    if report.min_spins > 0 and report.max_spins > 0:
        spins_text = f'between {report.min_spins} and {report.max_spins} time(s) '
    elif report.min_spins > 0 and report.max_spins == 0:
        spins_text = f'more then {report.min_spins} time(s) '
    elif report.min_spins == 0 and report.max_spins > 0:
        spins_text = f'under {report.max_spins} time(s) '
    else:
        spins_text = 'at least 1 time(s) '

    email['body'] += 'There were no results for ' if len(song_dict) < 1 else 'Here is your '
    email['body'] += f'{market_type_name} for {report.weeks_to_check} {"week" if report.weeks_to_check == 1 else "weeks"} of data on {market_name}.<br>'
    email['body'] += f'Containing songs played {spins_text}'
    email['body'] += f'between {start_dt} and {end_dt} for '
    if daypart_name:
        email['body'] += f'{daypart_name}. '
    else:
        email['body'] += f'{days_str} from hour {report.start_time} to hour {report.end_time}. '

    email['body'] += '<br><br>Have a question about this or any other report? Reach out to us at support@radioanalyzer.com'
    email['body'] += '<br>Your RadioAnalyzer Team<br>'

    return email


def get_song_list(report):
    start_time =  "{:0>8}".format(str(report.start_time))
    end_time = "{:0>8}".format(str(report.end_time))
    api_url = (f'{API_BASE_URL}/charts/{"radio" if report.market_type.value == 0 else "market"}/songs/{"new/" if report.new else ""}?id='
                         f'{report.market_id}'
               f'{"" if report.market_type.value == 0 else f"&market_type={report.market_type.name.lower()}"}&start_dt={report.start_dt}&end_dt={report.end_dt}&min_spins'
               f'={report.min_spins}&max_spins'
               f'={report.max_spins}&start_time={start_time}'
                         f'&end_time={end_time}{'&limit=' + report.limit if report.limit else ''}&user_id={report.user_id}')
    resp = requests.post(api_url, headers=HEADERS, json=report.days)
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


def fit_daypart_details(report):
    daypart = get_daypart_details(report.daypart_id)
    report.start_time = daypart['start_time']
    report.end_time = daypart['end_time']
    report.days = [int(x) for x in daypart['days'].split(',')]
    return report, daypart['name']