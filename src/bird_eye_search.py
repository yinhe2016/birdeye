from bird_eye_util import BirdEyeUtil
from bird_eye_indexer import BirdEyeIndexer
import arrow

class BirdEyeSearch:
	@staticmethod
	def normalize_query(q):
		r = []
		if isinstance(q, dict):
			x = BirdEyeSearch.normalize_query_dict(q)
			if len(x) > 0:
				r.append(x)
		elif isinstance(q, collections.Iterable):
			for u in q:
				v = BirdEyeSearch.normalize_query_dict(u)
				if len(v) > 0:
					r.append(v)
		return r
	@staticmethod
	def normalize_meta_query(q):
		r = {k : v for k, v in q.items() if BirdEyeIndexer.is_valid_meta_key(k)}
		for k, v in r.items():
			if BirdEyeIndexer.is_time_key(k):
				r[k] = arrow.get(v)
			else:
				if isinstance(v, str):
					r[k] = {v}
				elif isinstance(v, collections.Iterable):
					r[k] = set(v)
		return r
	@staticmethod
	def normalize_query_dict(q):
		r = dict()
		if isinstance(q, dict):
			for k, v in q.items():
				if isinstance(v, str):
					r[k] = {v}
				elif BirdEyeUtil.is_str_collection(v):
					r[k] = set(v)
		return r
	@staticmethod
	def find(all_index, tc_query, meta_query={}):
		tc_query_n = BirdEyeSearch.normalize_query(tc_query)
		meta_query_n = BirdEyeSearch.normalize_meta_query(meta_query)		
		return [k for k, v in all_index.items() if len(v) > 0 and BirdEyeIndex.match(v, tc_query_n, meta_query_n)]
