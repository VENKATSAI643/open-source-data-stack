FEATURE_FLAGS = {"ALERT_REPORTS": True}

class CeleryConfig:
    broker_url = 'sqla+sqlite:////app/superset_home/superset.db'
    imports = ('superset.sql_lab', 'superset.tasks', 'superset.tasks.thumbnails', 'superset.tasks.scheduler')
    result_backend = 'db+sqlite:////app/superset_home/superset.db'
    task_annotations = {'sql_lab.get_sql_results': {'rate_limit': '100/s'}}

CELERY_CONFIG = CeleryConfig
WEBDRIVER_BASEURL = "http://superset:8088/"
