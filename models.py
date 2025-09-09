from dataclasses import dataclass, asdict, fields
from datetime import datetime, date, time
from enum import Enum, IntEnum

from ra_datetime_helper import start_of_the_week, end_of_the_week
from ra_mysql_package import SQLConnection
from ra_radio_helper import get_radio_name


class MarketType(Enum):
    RADIO = 0
    COMPETITOR = 1
    AIRPLAY = 2
    TAG = 3


@dataclass
class Job:
    report_id: int
    end_dt: datetime = None

    def __post_init__(self):
        self.report_id= int(self.report_id)

    def to_flow_message(self):
        # Convert the dataclass to a dictionary
        flow_message = asdict(self)
        # Return cleaned dictionary
        return flow_message

    to_dict = asdict


@dataclass
class Report:
    report_id: int
    user_id: int
    market_id: int
    market_type: MarketType
    weeks_to_check: int
    email_address: list[str]
    limit: int
    new: bool
    last_date_used: datetime
    start_time: time
    end_time: time
    min_spins: int
    max_spins: int
    days: list[int]
    daypart_id: int
    start_dt: str = None
    end_dt: str = None


    def __post_init__(self):
        self.report_id= int(self.report_id)
        self.user_id= int(self.user_id)
        self.market_id= int(self.market_id)
        self.market_type= MarketType(self.market_type)
        self.weeks_to_check= int(self.weeks_to_check)
        self.email_address= str(self.email_address).split(',')
        if self.limit:
            self.limit = int(self.limit)
        else:
            self.limit = None
        self.new = bool(self.new)
        if isinstance(self.last_date_used, str):
            self.start_datetime = datetime.strptime(self.last_date_used, '%Y-%m-%d %H:%M:%S')
        self.start_time = self.start_time if self.start_time else time(0,0,0)
        self.end_time = self.end_time if self.end_time else time(23,59,59)
        self.min_spins = int(self.min_spins) if self.min_spins else 0
        self.max_spins = int(self.max_spins) if self.max_spins else 0
        self.days = [int(x) for x in self.days.split(',')] if self.days else [0,1,2,3,4,5,6]
        self.daypart_id = int(self.daypart_id) if self.daypart_id else None