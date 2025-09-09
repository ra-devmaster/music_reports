import os

from ra_datetime_helper import start_of_the_day, end_of_the_day

from functions import *
from ra_service_helper import BackendService
from ra_service_helper.models import filter_dataclass_fields
from models import Job
from queries import *

load_dotenv()

hardcoded_job = {'report_id': 1}

# For production, uncomment line below
hardcoded_job = None


def init_job(instance: BackendService, job: dict):
    # Crate job object from job dict
    try:
        job = Job(**filter_dataclass_fields(Job, job))
    except Exception as ex:
        ex.add_note(f"Failed to create Job from dict, {ex}")
        raise
    # job_name = f"Syncing file {job.file_path} from workstation {job.workstation}"
    # instance.set_job_name(job_name)
    instance.update_job_action('Starting')
    instance.check_stop_processing()

    # Return object we want to get in process_job function
    return job


def process_job(instance: BackendService, job: Job):
    report = get_report_details(job.report_id)

    dt_now = datetime.now()
    report.start_dt = start_of_the_day(dt_now - timedelta(days=7*report.weeks_to_check)).strftime('%Y-%m-%d')
    job.end_dt = end_of_the_day(dt_now - timedelta(days=1))
    report.end_dt = job.end_dt.strftime('%Y-%m-%d')


    daypart_name = None

    if report.market_type.value == 0 and report.daypart_id:
        report, daypart_name = fit_daypart_details(report)

    songs = get_song_list(report)

    song_list = songs['songs']
    song_list = sorted(song_list, key=lambda d: d['spins'], reverse=True)

    if report.market_type.value != 0:
        radio_ids = set(d['first_on'] for d in song_list)
        radio_names = get_radio_names(radio_ids)
        song_list_formatted = []
        for s in song_list:
            song_list_formatted.append({
                'Artist': s['artist'],
                'Title': s['title'],
                'Release Year': s['release_year'],
                'Item ID': s['item_id'],
                'First On': radio_names[s['first_on']],
                'First PLay': datetime.strptime(s['first_play'], '%Y-%m-%dT%H:%M:%S'),
                'Spins': s['spins'],

            })
    else:
        song_list_formatted = []
        for s in song_list:
            song_list_formatted.append({
                'Artist': s['artist'],
                'Title': s['title'],
                'Release Year': s['release_year'],
                'Item ID': s['item_id'],
                'First PLay': datetime.strptime(s['first_play'], '%Y-%m-%dT%H:%M:%S'),
                'Spins': s['spins'],
                '18-6':  s['spins_18_6'],
                '6-10': s['spins_6_10'],
                '10-14': s['spins_10_14'],
                '14-18': s['spins_14_18'],
            })
        spins = []
        for d in song_list_formatted:
            spins += [d['18-6'], d['6-10'], d['10-14'], d['14-18']]
        if set(spins) == {0}:
            song_list_formatted = [{k: v for k, v in d.items() if k not in ['18-6', '6-10', '10-14', '14-18']} for d in song_list_formatted]

    attachments = generate_attachments(report, song_list_formatted, report.start_dt, report.end_dt)
    email = create_email(report, song_list_formatted, report.start_dt, report.end_dt, daypart_name)
    email_sent = send_api(report.email_address, email['subject'], email['body'], attachments)
    if email_sent:
        for a in attachments:
            os.remove(a)
    # return False
    return email_sent


def on_success(instance: BackendService, job: Job):
    with SQLConnection(1) as conn:
        set_reports_being_processed(conn, [job.report_id], 0, job.end_dt)


def on_fail(instance: BackendService, job: Job):
    with SQLConnection(1) as conn:
        set_reports_being_processed(conn, [job['report_id']], -1)


worker = BackendService(hardcoded_job, init_job, process_job, on_success_func=on_success, on_failure_func=on_fail)