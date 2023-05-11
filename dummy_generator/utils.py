import datetime
import random

def random_dates(start_date: datetime.date, end_date: datetime.date, min_day: int = 60, step: int = 30):
    num_days = (end_date - start_date).days
    random_days = random.randrange(min_day, num_days, step=step)
    random_date = start_date + datetime.timedelta(days=random_days)
    return random_date