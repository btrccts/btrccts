class Timeframe:

    def __init__(self, pd_start_date, pd_end_date, pd_timedelta):
        if pd_end_date < pd_start_date:
            raise ValueError('Timeframe: end date is smaller then start date')
        if pd_timedelta.value <= 0:
            raise ValueError('Timeframe: timedelta needs to be positive')
        self._pd_timedelta = pd_timedelta
        self._pd_start_date = pd_start_date
        self._pd_current_date = pd_start_date
        self._pd_end_date = pd_end_date

    def add_timedelta(self):
        self._pd_current_date += self._pd_timedelta

    def date(self):
        if self._pd_current_date > self._pd_end_date:
            return None
        return self._pd_current_date

    def add_timedelta_until(self, date):
        while self._pd_current_date + self._pd_timedelta < date:
            self.add_timedelta()

    def start_date(self):
        return self._pd_start_date

    def end_date(self):
        return self._pd_end_date
