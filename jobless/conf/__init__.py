
class Config:
    mysql_uri = 'mysql+pymysql://root:root@mysql/jobless'
    lock_hosts = [{"host": "redis", "port": 6379, "db": 4}]
