import arrow
from bird_eye_util import BirdEyeUtil

class InsightNonParametricData:
	DEFAULT_START_TIME = arrow.get('2106-01-01 00:00:00')
	DEFAULT_END_TIME = arrow.get('2006-01-01 00:00:00')
	# DEFAULT_DATE_FORMAT = 'YYYY-MM-DD HH:MM:SS'
	@staticmethod
	def is_insight(dat):
		if not isinstance(dat, list):
			return False
		if len(dat) < 7:
			return False
		for line in dat:
			if not BirdEyeUtil.is_str_collection(line):
				return False
		line_lens = set([len(line) for line in dat])
		if len(line_lens) > 2:
			return False
		x = sorted(list(line_lens))
		if x[0] != len(dat[0]) or x[0] != len(dat[2]):
			return False
		return True
	@staticmethod
	def extract(dat):
		if not InsightNonParametricData.is_insight(dat):
			return None
		n = len(dat[0])-1
		col = [[] for i in range(n)]
		for line in dat[7:]:
			for i in range(n):
				col[i].append(line[i])
		return {dat[1][i] : col[i] for i in range(n)}
	@staticmethod
	def get_start_time(meta):
		a = meta.get('StartTime', [])
		r = InsightNonParametricData.DEFAULT_START_TIME
		for time_str in a:
			try:
				t = arrow.get(time_str)
				if t < r:
					r = t
			except:
				pass
		return r
	@staticmethod
	def get_end_time(meta):
		a = meta.get('EndTime', [])
		r = InsightNonParametricData.DEFAULT_END_TIME
		for time_str in a:
			try:
				t = arrow.get(time_str)
				if t > r:
					r = t
			except:
				pass
		return r
	@staticmethod
	def get_sites(meta):
		return set(meta.get('Site', []))
	@staticmethod
	def extract_build(a):
		r = ''
		a = a.strip()
		if len(a) > 0:
			a = a.replace('_', '-')
			w = a.split('-')
			r = w[1] if len(w) > 1 else a
		return r
	@staticmethod
	def build(b, d):
		r = InsightNonParametricData.extract_build(b)
		if len(r) == 0:
			r = InsightNonParametricData.extract_build(d)
		return r
	@staticmethod
	def get_builds(meta):
		a = meta['Special Build Name']
		b = meta.get('Special Build Description', meta.get('Special Build Descripton', ''))
		c = [InsightNonParametricData.build(a[i], b[i]) for i in range(len(a))]
		return set(c)
	@staticmethod
	def get_meta_data_index(meta):
		r = dict()
		r['product'] = set(meta.get('Product', set()))
		r['build'] = InsightNonParametricData.get_builds(meta)
		r['site'] = InsightNonParametricData.get_sites(meta)
		r['starttime'] = InsightNonParametricData.get_start_time(meta)
		r['endtime'] =  InsightNonParametricData.get_end_time(meta)
		return r
	@staticmethod
	def index(data):
		if not InsightNonParametricData.is_insight(data):
			return dict()
		meta = InsightNonParametricData.extract(data)
		r = InsightNonParametricData.get_meta_data_index(meta)
		r['station'] = {data[0][0].strip()}
		w = data[0][1].split(';')
		w1 = [x.strip() for x in w]
		r['version'] = set(w1)
		return r