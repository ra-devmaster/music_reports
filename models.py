from dataclasses import dataclass, asdict, fields
from datetime import datetime, date, time
from enum import Enum, IntEnum

from ra_datetime_helper import start_of_the_week, end_of_the_week
from ra_mysql_package import SQLConnection
from ra_radio_helper import get_radio_name

from functions import get_market_name
from queries import get_report_details


class MarketType(Enum):
    RADIO = 0
    COMPETITOR = 1
    AIRPLAY = 2
    TAG = 3


@dataclass
class Job:
    report_id: int
    end_dt_date: datetime = None
    user_id: int = None
    market_id: int = None
    market_type: MarketType = None
    weeks_to_check: int = None
    email_address: list[str] = None
    limit: int = None
    new: bool = None
    last_date_used: datetime = None
    start_time: time = None
    end_time: time = None
    min_spins: int = None
    max_spins: int = None
    days: list[int] = None
    daypart_id: int = None
    start_dt: str = None
    end_dt: str = None
    market_name: str = None

    def __post_init__(self):
        self.report_id= int(self.report_id)
        if not self.user_id:
            report = get_report_details(self.report_id)
            self.report_id = int(report['report_id'])
            self.user_id = int(report['user_id'])
            self.market_id = int(report['market_id'])
            self.market_type = MarketType(report['market_type'])
            self.weeks_to_check = int(report['weeks_to_check'])
            self.email_address = str(report['email_address']).split(',')
            if report['limit']:
                self.limit = int(report['limit'])
            else:
                self.limit = None
            self.new = bool(report['new'])
            if isinstance(report['last_date_used'], str):
                self.start_datetime = datetime.strptime(report['last_date_used'], '%Y-%m-%d %H:%M:%S')
            self.start_time = report['start_time'] if report['start_time'] else time(0, 0, 0)
            self.end_time = report['end_time'] if report['end_time'] else time(23, 59, 59)
            self.min_spins = int(report['min_spins']) if report['min_spins'] else 0
            self.max_spins = int(report['max_spins']) if report['max_spins'] else 0
            self.days = [int(x) for x in report['days'].split(',')] if report['days'] else [0, 1, 2, 3, 4, 5, 6]
            self.daypart_id = int(report['daypart_id']) if report['daypart_id'] else None
            self.market_name = get_market_name(self.market_id, self.market_type)

    def to_flow_message(self):
        # Convert the dataclass to a dictionary
        flow_message = asdict(self)
        # Return cleaned dictionary
        return flow_message

    to_dict = asdict

