from enum import Enum
import pathlib
import os
import json

from bird_eye_util import BirdEyeUtil

class BirdEyeStorageType(Enum):
	RAW = ('.raw', '.csv.gzip')
	CSV = ('.csv', '.data.csv')
	INDEXJSON = ('.json', '.index.json')
	INDEXTXT = ('txt', '.index.txt')
	SYNC = ('.sync', '.lock')
	STATUS = ('status', '.status.txt')
	@staticmethod
	def is_binary(type):
		return type in [BirdEyeStorageType.RAW]

class BirdEyeLayout:
	def __init__(self, root):
		self._root = pathlib.Path(root)
		self._root.mkdir(parents=True, exist_ok=True)
	def file_path(self, project, station, site, type, date):
		return self._root.joinpath(*[project, station, site, type.value[0], date+type.value[1]])
	def station_file_path(self, project, date):
		return self._root.joinpath(*[project, '.stations', date+'.stations.json'])
	def transform_path(self, fp, t1, t2):
		return pathlib.Path(str(fp).replace('/'+t1.value[0], '/'+t2.value[0]).replace(t1.value[1], t2.value[1]))
	def write(self, project, station, site, date, type, payload):
		f = self.file_path(project, station, site, type, date)
		f.parent.mkdir(parents=True, exist_ok=True)
		fmode = 'wb' if BirdEyeStorageType.is_binary(type) else 'w'
		BirdEyeUtil.write(f, fmode, payload)
	def create_lock(self, project, station, site, date):
		self.write(project, station, site, date, BirdEyeStorageType.SYNC, '')	
	def write_stations(self, project, date, site_stations_json):
		f = self.station_file_path(project, date)
		f.parent.mkdir(parents=True, exist_ok=True)
		BirdEyeUtil.write(f, 'w', site_stations_json)
	def load_stations(self, project, date):
		r = dict()
		sf = self.station_file_path(project, date)
		if sf.exists() and sf.stat().st_size > 0:
			with sf.open('r') as f:
				r = json.load(f)
		return r		
	def select(self, selector, type, date=''):
		p = self._root.joinpath(*selector)
		r = sorted(list(p.rglob("*"+date+type.value[1]))) if p.exists() else []
		return r
	def unzip(self, raw):
		if raw.exists():
			csv = self.transform_path(raw, BirdEyeStorageType.RAW, BirdEyeStorageType.CSV)
			csv.parent.mkdir(parents=True, exist_ok=True)
			BirdEyeUtil.unzip(raw,csv)
