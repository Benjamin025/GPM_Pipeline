"""
Test GPM Pipeline - For development and testing
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from dags.gpm_pipeline.operators import GPMDownloadOperator, GPMProcessOperator

default_args = {
    'owner': 'gpm_test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def test_environment():
    """Test environment setup"""
    import sys
    import os
    
    print("=== GPM Pipeline Environment Test ===")
    print(f"Python: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    
    # Test imports
    packages = ['rasterio', 'xarray', 'numpy', 'h5py', 'netCDF4']
    for pkg in packages:
        try:
            __import__(pkg)
            print(f"✅ {pkg} installed")
        except ImportError:
            print(f"❌ {pkg} NOT installed")
    
    # Test NASA credentials
    netrc_path = '/opt/airflow/.netrc'
    if os.path.exists(netrc_path):
        print(f"✅ .netrc file exists")
    else:
        print(f"❌ .netrc file missing")
    
    return "Test complete"


with DAG(
    dag_id='test_gpm_environment',
    default_args=default_args,
    description='Test GPM pipeline environment',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'gpm', 'development'],
) as dag:
    
    test_env = PythonOperator(
        task_id='test_environment',
        python_callable=test_environment,
    )
    
    test_download = GPMDownloadOperator(
        task_id='test_download_single_month',
        year=2023,
        month=1,
        base_dir='/opt/airflow/data/gpm/test',
        max_workers=2,
    )
    
    test_process = GPMProcessOperator(
        task_id='test_process_single_month',
        year=2023,
        month=1,
        base_dir='/opt/airflow/data/gpm/test',
        min_daily_files=5,
    )
    
    cleanup = BashOperator(
        task_id='cleanup_test_data',
        bash_command='rm -rf /opt/airflow/data/gpm/test/* 2>/dev/null || true',
    )
    
    test_env >> test_download >> test_process >> cleanup