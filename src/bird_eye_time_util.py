import arrow
import time

class BirdEyeTimeUtil:
	@staticmethod
	def wait_till(end, poll_interval=1):
		while True:
			if arrow.now() > end:
				return
			time.sleep(poll_interval)
