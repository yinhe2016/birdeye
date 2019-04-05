import arrow
from insight_data_parametric_header import InsightDataParametricHeader
from insight_non_parametric_data import InsightNonParametricData
from bird_eye_util import BirdEyeUtil

class BirdEyeIndexer:
	_DATETIME_OUTPUT_FORMAT = 'YYYY-MM-DD HH:MM:SS'
	_PRODUCT_META_INFO_KEYS = ['product', 'build', 'station', 'site', 'version']
	_PRODUCT_META_INFO_TIME_KEYS = ['starttime', 'endtime']
	@staticmethod
	def is_time_key(s):
		return s in BirdEyeIndexer._PRODUCT_META_INFO_TIME_KEYS
	@staticmethod
	def is_valid_meta_key(s):
		return s in BirdEyeIndexer._PRODUCT_META_INFO_KEYS or s in BirdEyeIndexer._PRODUCT_META_INFO_TIME_KEYS
	@staticmethod
	def read_atmost_n(fn, n=16000000):
		with open(fn, 'r') as f:
			d = f.readlines(n) if n > 0 else f.readlines()
			return d
	@staticmethod
	def read_f(file):
		return ''.join(BirdEyeIndexer.read_atmost_n(file, -1))
	@staticmethod
	def get_data(fn, n=16000000):
		x = BirdEyeIndexer.read_atmost_n(fn, n)
		lines = [a.rstrip() for a in x]
		return [x.split(',') for x in lines]
	@staticmethod
	def partition_with_key(alist, k):
		kk = [k(x) for x in alist]
		x = sorted(list(set(kk)))
		r = {}
		for a in x:
			r[a] = []
		for i in range(len(alist)):
			r[kk[i]].append(i)
		return r
	@staticmethod
	def partition_data_with_tc(d):
		n = len(d[0]) - 1
		try:
			m = BirdEyeIndexer.partition_with_key(d[1][n:], InsightDataParametricHeader.test_case)
		except:
			return dict()
		r = {k : [] for k in m.keys()}
		for line in d:
			line = line[n:]
			nn = len(line)
			for k, s in m.items():
				sub = [line[i] for i in s if i < nn]
				r[k].append(sub)
		return r
	@staticmethod
	def show_t(fn, n=16000000):
		d = BirdEyeIndexer.get_data(fn, n)
		u = BirdEyeIndexer.partition_data_with_tc(d)
		kk = sorted(u.keys())
		return d, u, kk
	@staticmethod
	def drop_zero_len(d):
		if len(d) < 1:
			return [[]]
		n = len(d[1])
		return [x for x in d if len(x) == n]	
	@staticmethod
	def transpose_double_array(x):
		n = len(x)
		m = len(x[0])
		return [[x[i][j] for i in range(n)] for j in range(m)]
	@staticmethod
	def sfmt(d):
		return BirdEyeIndexer.transpose_double_array(BirdEyeIndexer.drop_zero_len(d))
	@staticmethod
	def pfmt(d, n=15):
		for x in d:
			na = np.min([n, len(x)])
			print(x[:na])
	@staticmethod
	def short_dict(di):
		d = {k : v for k, v in di.items()}
		s = []
		k = sorted(list(d.keys()))
		for a in k:
			w = list(d[a])
			n = len(w)
			if n == 1:
				s.append(a+'='+w[0])
				del d[a]
		if len(s) > 0:
			d['FIXED_SETTING'] = ' '.join(s)
		return d
	@staticmethod
	def format_dict(d, leading_white_space=''):
		r = []
		k = sorted(list(d.keys()))
		for a in k:
			if isinstance(d[a], str):
				v = leading_white_space + a  +': ' + d[a]
			else:
				s = '('+str(len(d[a])) + ')'
				v = leading_white_space + a + s +': ' + ' '.join(BirdEyeUtil.sorted(d[a]))
			r.append(v)
		r.append('\n')
		return r
	@staticmethod
	def format_testcase_info(x, t = '  '):
		r =['TEST CASES:',]
		keys = sorted(list(x.keys()))
		for k in keys:
			v = x[k]
			r.append(t+k)
			t1 = t+t
			t2 = t1+t
			for test_case, test_cond in v.items():
				r.append(t1+test_case)
				short_test_cond = BirdEyeIndexer.short_dict(test_cond)
				r.extend(BirdEyeIndexer.format_dict(short_test_cond, t2))
			r.append('\n')
		return '\n'.join(r)
	@staticmethod
	def print_index(x, t = '  '):
		print(BirdEyeIndexer.to_txt(x, t))
	@staticmethod
	def to_txt(y, t = '  '):
		return BirdEyeIndexer.format_meta_info(y[1]) + BirdEyeIndexer.format_testcase_info(y[0])
	@staticmethod
	def format_meta_info(x, t = '  '):
		r = ['PRODUCT META INFO:',]
		for k in BirdEyeIndexer._PRODUCT_META_INFO_KEYS:
			r.append(t+k+': ' + ';'.join(BirdEyeUtil.sorted(x[k])))
		for k in BirdEyeIndexer._PRODUCT_META_INFO_TIME_KEYS:
			r.append(t+k+': ' + x[k].format())
		r.append('\n')
		return '\n'.join(r)
	@staticmethod
	def add_cond(s, d):
		for k, v in d.items():
			if k not in s:
				s[k] = set()
			s[k].add(v)
	@staticmethod
	def test_case_index(d, cutoff=16):
		u = BirdEyeIndexer.partition_data_with_tc(d)
		kk = sorted(u.keys())
		rf = dict()
		for s in kk:	
			v = u[s]
			a =  BirdEyeIndexer.sfmt(v)
			if len(a) >= cutoff:
				z = [InsightDataParametricHeader.parse(x[0]) for x in a]
				ds = dict()
				for d in z:
					test = sorted(list(d.keys()))
					test_class = ' '.join(test)
					if test_class not in ds:
						ds[test_class] = dict()
					BirdEyeIndexer.add_cond(ds[test_class], d)
				rf[s] = ds
		return rf
	@staticmethod
	def tokenized(data):
		raw_lines = data.splitlines()
		strip_lines = [x.rstrip() for x in raw_lines]
		lines = [x for x in strip_lines if len(x) > 0]
		dat = [x.split(',') for x in lines]	
		return dat
	@staticmethod
	def index(data, cutoff=16):
		if not isinstance(data, str):
			return dict()
		dat = BirdEyeIndexer.tokenized(data)
		meta_info = InsightNonParametricData.index(dat)
		if len(meta_info) == 0:
			return [], meta_info
		rf = BirdEyeIndexer.test_case_index(dat[:7], cutoff)
		return rf, meta_info
	@staticmethod
	def index_file(fn, cutoff=16):
		data = BirdEyeIndexer.read_f(fn)
		return BirdEyeIndexer.index(data, cutoff)
	@staticmethod
	def switch_test_case_rep(a):
		for test_case, test_setting in a.items():
			for asetting, test_class in test_setting.items():
				for k, v in test_class.items():
					test_class[k] = list(v) if isinstance(v, set) else set(v)
	@staticmethod
	def match_test_cond(a, q):
		q_keys = set(q.keys())
		for test_case, test_setting in a.items():
			for asetting, test_class in test_setting.items():
				if BirdEyeIndexer.match_a_test_class(test_class, q_keys, q):
					return True
		return False
	def match_test_conds(a, q):
		for query in q:
			if BirdEyeIndexer.match_test_cond(a, query):
				return True
		return False
	@staticmethod
	def match_a_test_class(test_class, q_keys, q):
		if not q_keys.issubset(set(test_class.keys())):
			return False
		for k, v in q.items():
			if len(q[k] & test_class[k]) == 0:
				return False
		return True
	@staticmethod
	def switch_product_meta_data_rep(m):
		for k, v in m.items():
			if BirdEyeIndexer.is_time_key(k):
				m[k] = arrow.get(v) if isinstance(v, str) else v.format()
			else:
				m[k] = list(v) if isinstance(v, set) else set(v)
	@staticmethod
	def switch_rep(y):
		if isinstance(y, tuple) and len(y) == 2:
			BirdEyeIndexer.switch_test_case_rep(y[0])
			BirdEyeIndexer.switch_product_meta_data_rep(y[1])
	@staticmethod
	def time_overlap(m, query):
		if 'starttime' not in query and 'endtime' not in query:
			return True
		if 'starttime' in query and query['starttime'] <= m['endtime']:
			return True
		if 'endtime' in query and query['endtime'] >= m['starttime']:
			return True
		return False
	@staticmethod
	def match_meta_data(m, query):
		if len(query) == 0:
			return True
		if not BirdEyeIndexer.time_overlap(m, query):
			return False
		for k in BirdEyeIndexer._PRODUCT_META_INFO_KEYS:
			if k in query:
				if len(m[k] & query[k]) == 0:
					return False
		return True
	@staticmethod
	def match(index, test_cond_query, meta_data_query):
		return BirdEyeIndexer.match_meta_data(index[1], meta_data_query) and BirdEyeIndexer.match_test_conds(index[0], test_cond_query)
