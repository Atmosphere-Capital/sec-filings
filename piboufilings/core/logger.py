"""
Logging functionality for SEC EDGAR filings operations.
"""

import os
import csv
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

class FilingLogger:
    """A class to handle logging of filing operations to CSV."""
    
    def __init__(self, log_dir: str = "./logs"):
        """
        Initialize the FilingLogger.
        
        Args:
            log_dir: Directory to store log files
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.log_file = self.log_dir / f"filing_operations_{datetime.now().strftime('%Y%m%d')}.csv"
        
        # Create log file with headers if it doesn't exist
        if not self.log_file.exists():
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", 
                    "operation_type",
                    "cik", 
                    "form_type_processed",
                    "accession_number", 
                    "download_success", 
                    "download_error_message", 
                    "parse_success",
                    "error_code",
                    "custom_identifier"
                ])
    
    def log_operation(
        self, 
        operation_type: Optional[str] = "",
        cik: Optional[str] = None, 
        form_type_processed: Optional[str] = None,
        accession_number: Optional[str] = None, 
        download_success: bool = False, 
        download_error_message: Optional[str] = None, 
        parse_success: Optional[bool] = None,
        error_code: Optional[Any] = None,
        custom_identifier: Optional[str] = None
    ) -> None:
        """
        Log a filing operation to the CSV file.
        
        Args:
            cik: Company CIK number (optional, for system-wide events)
            accession_number: Filing accession number
            download_success: Whether the download was successful
            download_error_message: Error message if download failed
            parse_success: Whether parsing was successful (if applicable)
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp,
                operation_type,
                cik or "SYSTEM",
                form_type_processed or "",
                accession_number or "",
                "True" if download_success else "False",
                download_error_message or "",
                "True" if parse_success else "False" if parse_success is not None else "",
                str(error_code) if error_code is not None else "",
                custom_identifier or ""
            ])
    
    def get_logs(self) -> pd.DataFrame:
        """
        Get all logs as a DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame containing all logs
        """
        if not self.log_file.exists():
            return pd.DataFrame(columns=[
                "timestamp", 
                "operation_type",
                "cik", 
                "form_type_processed",
                "accession_number", 
                "download_success", 
                "download_error_message", 
                "parse_success",
                "error_code",
                "custom_identifier"
            ])
        
        return pd.read_csv(self.log_file)
    
    def get_logs_by_cik(self, cik: str) -> pd.DataFrame:
        """
        Get logs for a specific CIK.
        
        Args:
            cik: Company CIK number
            
        Returns:
            pd.DataFrame: DataFrame containing logs for the specified CIK
        """
        logs = self.get_logs()
        return logs[logs["cik"] == cik] 