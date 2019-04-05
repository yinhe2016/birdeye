import re
from collections import OrderedDict

class InsightDataParametricHeader:
	KEY_EQUAL_VALUE = re.compile(r'(\w+)=([-\w.]+)')
	COLON_SEPARATED = re.compile(r'([a-z]+)([^a-z]\w*)')
	CASE_SEPARATED = re.compile(r'([a-z]+)([^a-z\s]+)')
	@staticmethod
	def _parse(h, fmt):
		return OrderedDict(fmt.findall(h))
	@staticmethod
	def _match(h, filter):
		if filter.search(h):
			return OrderedDict(m.groupdict())
		return None
	@staticmethod
	def parse(x):
		x = x.strip()
		if '=' in x:
			r = InsightDataParametricHeader._parse(x, InsightDataParametricHeader.KEY_EQUAL_VALUE)
		elif ':' in x:
			if x.startswith('CoF_tc'):
				r = InsightDataParametricHeader._parse(x[4:], InsightDataParametricHeader.COLON_SEPARATED)
				r['CoF_tc'] = r['tc']
				del r['tc']
			else:
				r = InsightDataParametricHeader._parse(x, InsightDataParametricHeader.COLON_SEPARATED)
		else:
			r = InsightDataParametricHeader._parse(x, InsightDataParametricHeader.CASE_SEPARATED)
		return r
	@staticmethod
	def test_case(x):
		d = InsightDataParametricHeader.parse(x)
		if 'CoF_tc' in d:
			return 'CoF_'+ d.get('CoF_tc')
		else:
			return d.get('tc', x)
	@staticmethod
	def test_class(x):
		return sorted(list(InsightDataParametricHeader.parse(x).keys()))
	def test_class_and_cond(x):
		d = InsightDataParametricHeader.parse_header(x)
		k = sorted(list(d))
		v = [d[u] for u in k]
		return k, v


