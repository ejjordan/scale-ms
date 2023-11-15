import os
import sys
from airflow.utils import timezone
from airflow.models import DagBag

from loud_logger import loud_logger

from airflow.configuration import conf
from airflow import settings

conf.set('database', 'sql_alchemy_conn', "mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db")
#sys.path.append('/home/joe/dev/scale-ms/src/scalems/simple/airflow/executors/')
sys.path.append('/home/joe/dev/scale-ms/src/scalems/simple/airflow/')
# Both of these seem to be needed to flush the config changes before changing the executor
settings.configure_vars()
settings.configure_orm()
#conf.set('core', 'executor', "executors.radical_executor.RadicalExecutor")

'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db"'
'export PYTHONPATH=/home/joe/dev/scale-ms/src/scalems/simple/airflow/executors/'
'export AIRFLOW__CORE__EXECUTOR=radical_executor.RadicalExecutor'

loud_logger()

dag_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dags/')

utc_now = DEFAULT_DATE = timezone.utcnow()

dagbag = DagBag(dag_folder=dag_dir, include_examples=False, read_dags_from_db=False)
dag = dagbag.dags.get("run_gmxapi")
dag.run()
#import ipdb;ipdb.set_trace()


