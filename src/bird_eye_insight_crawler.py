import sys
import os
import time
import arrow
import json
import traceback
from insight.auth import InsightSession
from insight.client import InsightClient
from insight.downloader import *
from bird_eye_time_util import BirdEyeTimeUtil

class BirdEyeInsightCrawler:
	STATION_SERVICE_URL = 'https://manufacturing.apple.com'+'/integrationservices.api/v1/station-list'
	#
	def __init__(self, client, station_poll_interval=45, downloader_poll_interval=60, data_req_interval=90, data_download_interval=80):
		self.client = client
		self.downloader = InsightDownloader(client, downloader_poll_interval)
		self.station_poll_interval = station_poll_interval
		self.downloader_poll_interval = downloader_poll_interval
		self.data_req_interval = data_req_interval
		self.data_download_interval = data_download_interval
	@staticmethod
	def get_station_query(product, site, date):
		station_type_query = {  
			"fromDate":date+" 00:00:00" ,
			"toDate":date+" 23:59:59",
			"siteNames":[site],
  			"productCodes":[product],
		}
		return station_type_query
	def get_stations(self, product, site, date):
		ts = arrow.now().shift(seconds=self.station_poll_interval)
		q = BirdEyeInsightCrawler.get_station_query(product, site, date)
		r = self.client.session.post(BirdEyeInsightCrawler.STATION_SERVICE_URL, json.dumps(q))
		if r.raw.status == 200:
			ss = r.json()['stationType']
		else:
			ss = []
			s = 'status: ' + str(r.raw.status) + ' reason: ' + r.raw.reason
			print(s)			
		BirdEyeTimeUtil.wait_till(ts)
		return ss	
	def create_export_requests(self, product, date, tracked_sites, tracked_stations, db):
		t2 = arrow.now().shift(minutes=45)
		tasks = []
		site_stations = db.load_stations(product, date)
		for site in tracked_sites:
			print(site + ': in create_export_requests')
			ss = site_stations.get(site, None)
			if ss is None:
				ss = self.get_stations(product, site, date)
				site_stations[site] = ss
			req = self.create_export_requests_for_site(ss, product, site, date, tracked_stations, db)
			if len(req) > 0:
				for query in req:
					print(query)
				tasks.extend(req)
				time.sleep(self.data_req_interval)
		return t2, tasks, site_stations
	def create_export_requests_for_site(self, ss, product, site, date, tracked_stations, db):
		rqs = []
		print(ss)
		stations = set(ss) & tracked_stations
		print('no. of stations = ' + str(len(stations)))
		print(stations)
		not_done_stations = [station for station in stations if not db.already_done(product, station, site, date)]
		print('no. of not done sations = ' + str(len(not_done_stations)))
		print(not_done_stations)
		start_time = date + " 00:00:00"
		end_time = date + " 23:59:59"		
		for station in not_done_stations:
			ts = arrow.now().shift(seconds=self.data_req_interval)
			print(station)
			export = self.downloader.create_export_requests(site, product, station, start_time, end_time)
			n_try = 0;
			while n_try < 7:
				try:
					task_id = self.client.post_export_request(export)
					rqs.append((product, station, site, date, task_id))
					n_try = 1000
					time.sleep(self.data_req_interval)
				except:
					n_try = n_try + 1
					print('n_try = ' + str(n_try))
					print(traceback.format_exc())
					time.sleep(n_try*5*self.data_req_interval)
			BirdEyeTimeUtil.wait_till(ts)
		return rqs
	def download_data(self, t2, tasks, db):
		BirdEyeTimeUtil.wait_till(t2)
		for product, station, site, date, task_id in tasks:
			print(arrow.now().format())
			print((product, station, site, date, task_id))
			tn = arrow.now().shift(seconds=self.data_download_interval)
			n_try1 = 0
			while n_try1 < 7:
				try:
					job_status = self.downloader.wait_for_download(task_id)
					if job_status == InsightJobStatus.READY:
						n_try = 0
						csv = ''
						while n_try < 7:
							try:
								csv = self.client.download_export(task_id, station)
								print('len(csv) = ' + str(len(csv)))
								db.process_zipped_csv(product, station, site, date, csv)
								n_try = 1000 # break out while loop
							except:
								n_try = n_try + 1
								print('n_try = ' + str(n_try))
								print(traceback.format_exc())
								time.sleep((n_try+5)*self.data_download_interval)
						n_try1 = 1000
				except:
					n_try1 = n_try1 + 1
					print('n_try1 = ' + str(n_try1))
					print(traceback.format_exc())
					time.sleep((n_try1+5)*self.data_download_interval)
			BirdEyeTimeUtil.wait_till(tn)		
	def download(self, product, date, tracked_sites, tracked_stations, db):
		print(arrow.now())
		t2, tasks, site_stations = self.create_export_requests(product, date, tracked_sites, tracked_stations, db)
		db.write_stations(product, date, site_stations)
		if len(tasks) == 0:
			print('No data for ' + product + ' of ' + date)
			return
		print(t2)
		for t in tasks:
			print(t)
		self.download_data(t2, tasks, db)
		print('Done with ' + product + ' on ' + date)
