
class Config:
    mysql_uri = 'mysql+pymysql://root:root@mysql/jobless'
    lock_hosts = [{"host": "redis", "port": 6379, "db": 4}]
    broker_url = 'redis://redis:6379/0'
    results_url = 'redis://redis:6379/1'
    max_fetch_size = 3
