import time
from datetime import timedelta, datetime as dt
from monthdelta import monthdelta
import holidays
import re

USHolidays = holidays.US()
USHolidays.append({'2019-04-19': 'Good Friday'})


class Job(object):

	RUNABLE_DAYS = {
		'day': lambda d : True,
		'weekday': lambda d : d.isoweekday() < 6,
		'weekend': lambda d : d.isoweekday() > 5,
		'businessday': lambda d : d not in USHolidays and d.isoweekday() < 6,
		'holiday': lambda d : d in USHolidays or d.isoweekday() > 5
	}

	def __init__(self, every, at, func, kwargs):
		self.interval = every
		self.time_string = at
		self.func = func
		self.kwargs = kwargs
	
	def init(self):
		self.schedule_next_run()
		return self

	def to_timestamp(self, d):
		return time.mktime(d.timetuple())+d.microsecond/1000000.0

	def schedule_next_run(self, just_ran=False):
		h, m = self.time_string.split(':')
		n = dt.now()
		n = dt(n.year, n.month, n.day, int(h), int(m), 0)
		ts = self.to_timestamp(n)
		if self.job_must_run_today() and time.time() < ts+300 and not just_ran: 
			self.timestamp = ts
		else:
			next_day = n + timedelta(days=1)
			while not self.job_must_run_today(next_day):
				next_day += timedelta(days=1)
			self.timestamp = self.to_timestamp(next_day)#next_day.timestamp()
		print(self)

	def job_must_run_today(self, date=None):
		return self.RUNABLE_DAYS[self.interval](date or dt.now())


	def is_due(self):
		# print(str(dt.fromtimestamp(time.time())), str(dt.fromtimestamp(self.timestamp)), time.time() >= self.timestamp)
		return time.time() >= self.timestamp

	def run(self):
		try:
			print("========== Scheduler Start =========")
			print("Executing {}".format(self))
			return self.func(**self.kwargs)
		except Exception as e:
			print(e)
		finally:
			
			self.schedule_next_run(just_ran=True)
			print("========== Scheduler End =========")


	def __repr__(self):
		return "Job {}. Next run = {}".format(self.func, str(dt.fromtimestamp(self.timestamp)))


class OneTimeJob(Job):

	def schedule_next_run(self, just_ran=False):
		H, M = self.time_string.split(':')
		Y, m, d = self.interval.split('-')
		n = dt(int(Y), int(m), int(d), int(H), int(M), 0)

		if just_ran or dt.now() > n + timedelta(minutes=3):
			self.timestamp = 0
			return None

		self.timestamp = self.to_timestamp(n)
		print(self)

	def is_due(self):
		if self.timestamp==0: raise JobExpired('remove me!')
		return time.time() >= self.timestamp

	def __repr__(self):
		return "OneTimeJob {}. Next run = {}".format(self.func, str(dt.fromtimestamp(self.timestamp)) if self.timestamp!=0 else 'Never')


class JobExpired(Exception):
	pass


class TaskScheduler(object):

	def __init__(self):
		self.jobs = []
		self.on = self.every

	def __current_timestring(self):
		return dt.now().strftime("%H:%M")

	def __valid_datestring(self, d):
		date_fmt = r'^([0-9]{4})-?(1[0-2]|0[1-9])-?(3[01]|0[1-9]|[12][0-9])$'
		return re.match(date_fmt, d) is not None

	def check(self):
		for j in self.jobs:
			try:
				if j.is_due(): j.run()
			except JobExpired:
				self.jobs.remove(j)

	def every(self, interval):
		self.interval = interval
		return self

	def at(self, time_string):
		if not self.interval: self.interval = 'day'
		self.temp_time = time_string
		return self

	def do(self, func, **kwargs):
		if not self.interval: raise Exception('Run .at()/.every().at() before .do()')
		if not self.temp_time: self.temp_time = self.__current_timestring()

		if self.__valid_datestring(self.interval):
			j = OneTimeJob(self.interval, self.temp_time, func, kwargs).init()
		else:
			j = Job(self.interval, self.temp_time, func, kwargs).init()
		self.jobs.append(j)
		self.temp_time = None
		self.interval = None
		return True






if __name__ == '__main__':
	def job(x, y): print(x, y)

	s = TaskScheduler()
	k = s.on('2019-04-16').at("15:57").do(job, x="hello", y="world")

	x = 1
	while x <10:
		s.check()
		x +=1
		time.sleep(10)
		print('due -', s.jobs)
	print(s.jobs)
