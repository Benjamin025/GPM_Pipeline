"""
NASA GPM Monthly Processing Pipeline
Processes daily GPM data to monthly aggregates on a schedule
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from dags.gpm_pipeline.operators import (
    GPMDownloadOperator,
    GPMProcessOperator,
    GPMCheckOperator
)

# Default arguments
default_args = {
    'owner': 'gpm_pipeline',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}


def get_processing_date(**context):
    """Get the date to process (typically previous month)"""
    execution_date = context['execution_date']
    # Process data from previous month (GPM data has 1-2 month latency)
    prev_month = execution_date - timedelta(days=30)
    
    result = {
        'year': prev_month.year,
        'month': prev_month.month,
        'execution_date': execution_date.isoformat()
    }
    
    context['ti'].xcom_push(key='processing_date', value=result)
    return result


def should_process_branch(**context):
    """Branch based on check result"""
    ti = context['ti']
    check_result = ti.xcom_pull(task_ids='check_processing_status')
    
    if check_result and check_result.get('should_process', False):
        return 'download_gpm_data'
    else:
        return 'skip_processing'


# Create main monthly DAG
with DAG(
    dag_id='gpm_monthly_processor',
    default_args=default_args,
    description='Process GPM daily data to monthly aggregates',
    schedule_interval='0 2 10 * *',  # Run on 10th of each month at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gpm', 'nasa', 'precipitation', 'monthly'],
) as dag:
    
    # Start
    start = DummyOperator(task_id='start')
    
    # Get processing date
    get_date = PythonOperator(
        task_id='get_processing_date',
        python_callable=get_processing_date,
        provide_context=True,
    )
    
    # Check if processing is needed
    check = GPMCheckOperator(
        task_id='check_processing_status',
        year="{{ ti.xcom_pull(task_ids='get_processing_date')['year'] }}",
        month="{{ ti.xcom_pull(task_ids='get_processing_date')['month'] }}",
        base_dir='/opt/airflow/data/gpm',
        min_file_size_mb=1.0,
        force_reprocess=False,
    )
    
    # Branch based on check result
    branch = BranchPythonOperator(
        task_id='decide_processing',
        python_callable=should_process_branch,
        provide_context=True,
    )
    
    # Skip processing path
    skip = DummyOperator(task_id='skip_processing')
    
    # Download data
    download = GPMDownloadOperator(
        task_id='download_gpm_data',
        year="{{ ti.xcom_pull(task_ids='get_processing_date')['year'] }}",
        month="{{ ti.xcom_pull(task_ids='get_processing_date')['month'] }}",
        base_dir='/opt/airflow/data/gpm',
        max_workers=5,
        max_retries_per_file=3,
        pool='gpm_download_pool',  # Use a pool to limit concurrent downloads
        task_concurrency=2,
    )
    
    # Process to monthly GeoTIFF
    process = GPMProcessOperator(
        task_id='process_to_monthly',
        year="{{ ti.xcom_pull(task_ids='get_processing_date')['year'] }}",
        month="{{ ti.xcom_pull(task_ids='get_processing_date')['month'] }}",
        base_dir='/opt/airflow/data/gpm',
        min_daily_files=15,
        pool='gpm_process_pool',
    )
    
    # Send notification
    def send_success_notification(**context):
        """Send success notification"""
        ti = context['ti']
        process_result = ti.xcom_pull(task_ids='process_to_monthly')
        
        message = f"""
        ✅ GPM Processing Complete
        
        Processed: {process_result['year']}-{process_result['month']:02d}
        Output: {process_result['output_path']}
        Size: {process_result['file_size_mb']:.1f} MB
        
        Processing completed at: {datetime.now().isoformat()}
        """
        
        print(message)
        # Here you could add email, Slack, etc. notifications
    
    notify = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule='all_done',
    )
    
    # End
    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )
    
    # Define workflow
    start >> get_date >> check >> branch
    branch >> [skip, download]
    download >> process >> notify
    skip >> notify
    notify >> end