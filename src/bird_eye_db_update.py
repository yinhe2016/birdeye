import zlib
import json
import traceback
import arrow

from bird_eye_indexer import BirdEyeIndexer
from bird_eye_layout import BirdEyeStorageType, BirdEyeLayout

class BirdEyeDBUpdate:
	def __init__(self, root):
		self._db = BirdEyeLayout(root)
	def already_done(self, product, station, site, date):
		lock = self._db.file_path(product, station, site, BirdEyeStorageType.SYNC, date)
		raw = self._db.file_path(product, station, site, BirdEyeStorageType.RAW, date)
		return lock.exists() and raw.exists() and raw.stat().st_size > 0
	def write_stations(self, product, date, site_stations):
		self._db.write_stations(product, date, json.dumps(site_stations))
	def load_stations(self, product, date):
		return self._db.load_stations(product, date)
	def process_zipped_csv(self, product, station, site, date, csv):
		db = self._db
		try:
			db.create_lock(product, station, site, date)
			db.write(product, station, site, date, BirdEyeStorageType.RAW, csv)
			print(arrow.now())
			if len(csv) < 50:
				db.write(product, station, site, date, BirdEyeStorageType.STATUS, 'No Data')
				return			
			if len(csv) > 1<<30:  # 1Gbytes
				db.write(product, station, site, date, BirdEyeStorageType.STATUS, 'Too big file, indexer needs more memory')
				return			
			data = zlib.decompress(csv, 15 + 32).decode("utf-8")
			print("after decompress len(data) = " + str(len(data)))
			print(arrow.now())
			index = BirdEyeIndexer.index(data)
			print("after index")
			print(arrow.now())
			BirdEyeIndexer.switch_rep(index)
			db.write(product, station, site, date, BirdEyeStorageType.INDEXJSON, json.dumps(index))
			db.write(product, station, site, date, BirdEyeStorageType.INDEXTXT, BirdEyeIndexer.to_txt(index))
			db.write(product, station, site, date, BirdEyeStorageType.STATUS, 'OK')
		except:
			db.write(product, station, site, date, BirdEyeStorageType.STATUS, traceback.format_exc())


