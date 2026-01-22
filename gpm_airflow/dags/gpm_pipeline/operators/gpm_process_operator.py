"""
GPM Process Operator for Airflow
"""
from typing import Dict, Optional
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dags.gpm_pipeline.utils.gpm_workflow import GPMDailyToMonthlyWorkflow


class GPMProcessOperator(BaseOperator):
    """
    Operator to process daily GPM files to monthly GeoTIFF
    """
    
    template_fields = ('year', 'month')  # Enable templating
    
    @apply_defaults
    def __init__(
        self,
        year: int,
        month: int,
        base_dir: Optional[str] = None,
        min_daily_files: int = 15,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.year = year
        self.month = month
        self.base_dir = base_dir
        self.min_daily_files = min_daily_files
    
    def execute(self, context) -> Dict:
        """Execute the processing task"""
        self.log.info(f"🔄 Starting GPM processing for {self.year}-{self.month:02d}")
        
        # Initialize workflow with Airflow logger
        workflow = GPMDailyToMonthlyWorkflow(base_dir=self.base_dir, log=self.log)
        
        # Try to get downloaded files from upstream task
        try:
            download_result = context['ti'].xcom_pull(
                task_ids=f'download_{self.year}_{self.month:02d}',
                key='download_result'
            )
            if download_result and 'downloaded_files' in download_result:
                downloaded_files = [Path(f) for f in download_result['downloaded_files']]
                self.log.info(f"Using {len(downloaded_files)} files from upstream download")
            else:
                downloaded_files = None
        except:
            downloaded_files = None
        
        # Process month
        result_path = workflow.process_month_from_daily(
            year=self.year,
            month=self.month,
            daily_files=downloaded_files
        )
        
        if result_path and result_path.exists():
            file_size = result_path.stat().st_size / (1024 * 1024)
            
            result = {
                'year': self.year,
                'month': self.month,
                'output_path': str(result_path),
                'file_size_mb': file_size,
                'status': 'success'
            }
            
            self.log.info(f"✅ Processing complete: {result_path.name} ({file_size:.1f} MB)")
            
            # Push to XCom
            context['ti'].xcom_push(key='process_result', value=result)
            
            return result
        else:
            error_msg = f"Failed to process GPM data for {self.year}-{self.month:02d}"
            self.log.error(error_msg)
            raise Exception(error_msg)