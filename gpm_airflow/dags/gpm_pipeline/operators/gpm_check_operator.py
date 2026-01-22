"""
GPM Check Operator for Airflow
"""
from typing import Dict, Optional
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dags.gpm_pipeline.utils.gpm_workflow import GPMDailyToMonthlyWorkflow


class GPMCheckOperator(BaseOperator):
    """
    Operator to check if processing is needed for a month
    """
    
    template_fields = ('year', 'month')
    
    @apply_defaults
    def __init__(
        self,
        year: int,
        month: int,
        base_dir: Optional[str] = None,
        min_file_size_mb: float = 1.0,
        force_reprocess: bool = False,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.year = year
        self.month = month
        self.base_dir = base_dir
        self.min_file_size_mb = min_file_size_mb
        self.force_reprocess = force_reprocess
    
    def execute(self, context) -> Dict:
        """Check if processing is needed"""
        self.log.info(f"🔍 Checking processing status for {self.year}-{self.month:02d}")
        
        # Initialize workflow
        workflow = GPMDailyToMonthlyWorkflow(base_dir=self.base_dir, log=self.log)
        
        # Check if GeoTIFF already exists
        geotiff_path = workflow.dirs['geotiffs'] / str(self.year) / f"GPM_{self.year}_{self.month:02d}_monthly_from_daily.tif"
        
        result = {
            'year': self.year,
            'month': self.month,
            'geotiff_exists': geotiff_path.exists(),
            'geotiff_path': str(geotiff_path) if geotiff_path.exists() else None,
            'should_process': True,  # Default
            'reason': ''
        }
        
        if self.force_reprocess:
            result['should_process'] = True
            result['reason'] = 'forced_reprocess'
            self.log.info(f"Force reprocess requested for {self.year}-{self.month:02d}")
        elif geotiff_path.exists():
            file_size = geotiff_path.stat().st_size / (1024 * 1024)
            if file_size >= self.min_file_size_mb:
                result['should_process'] = False
                result['reason'] = 'file_exists_valid'
                result['file_size_mb'] = file_size
                self.log.info(f"Valid GeoTIFF exists ({file_size:.1f} MB), skipping")
            else:
                result['should_process'] = True
                result['reason'] = 'file_exists_invalid'
                result['file_size_mb'] = file_size
                self.log.warning(f"Invalid GeoTIFF exists ({file_size:.1f} MB), reprocessing")
                # Delete corrupt file
                geotiff_path.unlink(missing_ok=True)
        else:
            result['should_process'] = True
            result['reason'] = 'file_not_found'
            self.log.info(f"No GeoTIFF found, processing needed")
        
        # Push to XCom
        context['ti'].xcom_push(key='check_result', value=result)
        
        return result