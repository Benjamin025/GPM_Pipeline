"""
GPM Download Operator for Airflow
"""
from typing import Dict, List, Optional
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dags.gpm_pipeline.utils.gpm_workflow import GPMDailyToMonthlyWorkflow


class GPMDownloadOperator(BaseOperator):
    """
    Operator to download GPM daily files for a specific month
    """
    
    template_fields = ('year', 'month')  # Enable templating for these fields
    
    @apply_defaults
    def __init__(
        self,
        year: int,
        month: int,
        base_dir: Optional[str] = None,
        max_workers: int = 5,
        max_retries_per_file: int = 3,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.year = year
        self.month = month
        self.base_dir = base_dir
        self.max_workers = max_workers
        self.max_retries_per_file = max_retries_per_file
    
    def execute(self, context) -> Dict:
        """Execute the download task"""
        self.log.info(f"📥 Starting GPM download for {self.year}-{self.month:02d}")
        
        # Initialize workflow with Airflow logger
        workflow = GPMDailyToMonthlyWorkflow(base_dir=self.base_dir, log=self.log)
        
        # Setup authentication
        if not workflow.setup_authentication():
            raise Exception("NASA Earthdata authentication failed. Check .netrc file.")
        
        # Download files
        downloaded_files = workflow.download_month_daily_files(
            year=self.year,
            month=self.month,
            max_workers=self.max_workers
        )
        
        # Prepare result
        result = {
            'year': self.year,
            'month': self.month,
            'downloaded_files': [str(f) for f in downloaded_files],
            'file_count': len(downloaded_files),
            'status': 'success' if len(downloaded_files) > 0 else 'partial'
        }
        
        self.log.info(f"✅ Download complete: {len(downloaded_files)} files downloaded")
        
        # Push to XCom for downstream tasks
        context['ti'].xcom_push(key='download_result', value=result)
        
        return result