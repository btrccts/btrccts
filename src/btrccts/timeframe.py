class Timeframe:

    def __init__(self, pd_start_date, pd_end_date, pd_interval):
        if pd_end_date < pd_start_date:
            raise ValueError('Timeframe: end date is smaller then start date')
        if pd_interval.value <= 0:
            raise ValueError('Timeframe: timedelta needs to be positive')
        self._pd_interval = pd_interval
        self._pd_start_date = pd_start_date
        self._pd_current_date = pd_start_date
        self._pd_end_date = pd_end_date

    def add_timedelta(self):
        self._pd_current_date += self._pd_interval

    def date(self):
        if self.finished():
            return self._pd_end_date
        return self._pd_current_date

    def add_timedelta_until(self, date):
        while self._pd_current_date + self._pd_interval < date:
            self.add_timedelta()

    def start_date(self):
        return self._pd_start_date

    def end_date(self):
        return self._pd_end_date

    def finished(self):
        return self._pd_current_date > self._pd_end_date
