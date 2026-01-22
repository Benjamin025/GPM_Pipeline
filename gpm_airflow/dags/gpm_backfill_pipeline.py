"""
GPM Backfill Pipeline - Process multiple months/years in parallel
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from dags.gpm_pipeline.operators import (
    GPMDownloadOperator,
    GPMProcessOperator,
    GPMCheckOperator
)

default_args = {
    'owner': 'gpm_pipeline',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


def generate_date_range(start_year=1998, start_month=1, end_year=None, end_month=None):
    """Generate list of year-month tuples to process"""
    if end_year is None:
        end_year = datetime.now().year
    if end_month is None:
        end_month = datetime.now().month - 1  # Previous month
    
    dates = []
    for year in range(start_year, end_year + 1):
        start_m = start_month if year == start_year else 1
        end_m = end_month if year == end_year else 12
        
        for month in range(start_m, end_m + 1):
            dates.append((year, month))
    
    return dates


with DAG(
    dag_id='gpm_backfill_processor',
    default_args=default_args,
    description='Backfill historical GPM data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['gpm', 'backfill', 'historical'],
) as dag:
    
    start = DummyOperator(task_id='start_backfill')
    end = DummyOperator(task_id='end_backfill')
    
    # Get date range from Airflow Variables or default
    def get_date_range(**context):
        start_year = int(Variable.get('gpm_backfill_start_year', default_var=1998))
        start_month = int(Variable.get('gpm_backfill_start_month', default_var=1))
        end_year = int(Variable.get('gpm_backfill_end_year', 
                                  default_var=datetime.now().year))
        end_month = int(Variable.get('gpm_backfill_end_month',
                                   default_var=datetime.now().month - 1))
        
        dates = generate_date_range(start_year, start_month, end_year, end_month)
        
        context['ti'].xcom_push(key='date_range', value=dates)
        return dates
    
    get_dates = PythonOperator(
        task_id='get_date_range',
        python_callable=get_date_range,
        provide_context=True,
    )
    
    # Create parallel processing groups (batches of 3 months)
    with TaskGroup('parallel_processing', tooltip='Parallel month processing') as parallel_group:
        
        # This will be populated dynamically
        pass
    
    # Dynamic task generation (will be done by Airflow at runtime)
    def create_parallel_tasks(**context):
        """Dynamically create parallel tasks for each month"""
        ti = context['ti']
        dates = ti.xcom_pull(task_ids='get_date_range', key='date_range')
        
        if not dates:
            return
        
        # Create task groups for batches
        batch_size = 3  # Process 3 months in parallel
        for i in range(0, len(dates), batch_size):
            batch = dates[i:i + batch_size]
            
            with TaskGroup(f'batch_{i//batch_size}', dag=dag) as batch_group:
                for year, month in batch:
                    # Create check task
                    check = GPMCheckOperator(
                        task_id=f'check_{year}_{month:02d}',
                        year=year,
                        month=month,
                        base_dir='/opt/airflow/data/gpm',
                        min_file_size_mb=1.0,
                    )
                    
                    # Create download task
                    download = GPMDownloadOperator(
                        task_id=f'download_{year}_{month:02d}',
                        year=year,
                        month=month,
                        base_dir='/opt/airflow/data/gpm',
                        max_workers=3,
                    )
                    
                    # Create process task
                    process = GPMProcessOperator(
                        task_id=f'process_{year}_{month:02d}',
                        year=year,
                        month=month,
                        base_dir='/opt/airflow/data/gpm',
                        min_daily_files=10,
                    )
                    
                    # Set dependencies within batch
                    check >> download >> process
    
    # Add the dynamic task creation
    from airflow.decorators import task
    
    @task
    def dynamic_task_creator(**context):
        return create_parallel_tasks(**context)
    
    create_tasks = dynamic_task_creator()
    
    # Define workflow
    start >> get_dates >> create_tasks >> end