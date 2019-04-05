import sys
proj_root = '/Users/yinhecao/Documents/proj/birdeye'
sys.path.append(proj_root+'/src')
import arrow
import yaml
import json
from bird_eye_db_update import BirdEyeDBUpdate
from insight_crawler_job import InsightCrawlerJob

project = 'D32'

station_site_file = proj_root+'/D32_stations_sites.json'
with open(station_site_file) as f:
        stations_and_sites = json.load(f)

dbroot = '/Volumes/data/birdeye/db'
db = BirdEyeDBUpdate(dbroot)

insight_login_config = proj_root+'/insight_config_test.yml'
with open(insight_login_config) as f:
        config = yaml.load(f)

# next two begins
begin_time = arrow.get('2019-03-28 00:00:00')
InsightCrawlerJob.launch_job(project, stations_and_sites, db, config, begin_time)
