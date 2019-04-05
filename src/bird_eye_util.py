import collections
import re
import pathlib
import os
import gzip

class BirdEyeUtil:
	INT_RE = re.compile(r"^[-]?\d+$")
	@staticmethod
	def is_str_collection(x):
		if isinstance(x, collections.Iterable):
			for a in x:
				if not isinstance(a, str):
					return False
		return True
	@staticmethod
	def is_all_int(x):
		for s in x:
			if BirdEyeUtil.INT_RE.match(s) is None:
				return False
		return True
	@staticmethod
	def sorted(x):
		if len(x) == 1:
			return x	
		if BirdEyeUtil.is_all_int(x):
			v = [int(a) for a in x]
			vs = sorted(v)
			return [str(a) for a in vs]
		try:
			v = [float(a) for a in x]
			vs = sorted(v)
			return [str(a) for a in vs]
		except:
			return sorted(x)
	@staticmethod
	def write(file, fmode, data):
		CHUNK_SIZE = 1<<30
		if file.exists():
			file.unlink()
		n_chunk = len(data)//CHUNK_SIZE
		with file.open(mode=fmode+'+') as fh:
			for i in range(n_chunk):
				fh.write(data[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE])
			fh.write(data[n_chunk*CHUNK_SIZE:])
			fh.flush()
			os.fsync(fh.fileno())
			file.chmod(0o444)
	@staticmethod
	def unzip(zipped_file_path, unzipped_file_path):
		if not str(zipped_file_path).endswith('gzip'):
			return
		if unzipped_file_path.exists():
			unzipped_file_path.unlink()
		BLOCK_SIZE = 1 << 26
		with zipped_file_path.open('rb') as f:
			gzip_f = gzip.GzipFile(fileobj=f)
			buf = gzip_f.read(BLOCK_SIZE)
			with unzipped_file_path.open('wb+') as fout:
				while buf:
					fout.write(buf)
					buf = gzip_f.read(BLOCK_SIZE)
				fout.flush()
				os.fsync(fout.fileno())
				unzipped_file_path.chmod(0o444)