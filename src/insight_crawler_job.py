import arrow
import traceback
from insight.auth import InsightSession
from insight.client import InsightClient
from bird_eye_db_update import BirdEyeDBUpdate
from bird_eye_insight_crawler import BirdEyeInsightCrawler
from bird_eye_time_util import BirdEyeTimeUtil

class InsightCrawlerJob:
	@staticmethod
	def daily_work(project, stations_and_sites, date, db, config):
		n_try = 0
		while n_try < 3:
			try:
				session = InsightSession(**config).get_session()
				n_try = 1000
			except:
				n_try = n_try + 1
				print('n_try = ' + str(n_try))
				print(traceback.format_exc())
				time.sleep(n_try * 20)
		try:
			client = InsightClient(config['mop_url'], session)
			sites = set(stations_and_sites.get('sites', []))
			stations = set(stations_and_sites.get('stations', []))
			crawler = BirdEyeInsightCrawler(client)
			crawler.download(project, date, sites, stations, db)
		except:
			info = traceback.format_exc()
			print(info)
		session.close()
	@staticmethod
	def launch_job(project, stations_and_sites, db, config, begin_time):
		t1 = begin_time
		while True:
			date = t1.format()[:10]
			t1 = t1.shift(days=1)
			t2 = t1.shift(hours=10) # local time 2:00 am
			BirdEyeTimeUtil.wait_till(t2, 300)
			print('date = ' + date)
			print(t2)
			print(arrow.now())
			try:
				InsightCrawlerJob.daily_work(project, stations_and_sites, date, db, config)
			except:
				print(traceback.format_exc())
