from jobless.conf import Config

# CELERY_ENABLE_UTC = True
# CELERYD_POOL_RESTARTS = True

CELERY_ACCEPT_CONTENT = ['pickle']
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
BROKER_TRANSPORT = 'redis'
BROKER_URL = Config.broker_url
CELERY_RESULT_BACKEND = Config.results_url
