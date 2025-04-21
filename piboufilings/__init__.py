"""
piboufilings - A Python library for downloading and parsing SEC EDGAR filings.
"""

from typing import Optional, List, Dict, Any, Union
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

from .core.downloader import SECDownloader
from .core.parser import SECFilingParser
from .core.logger import FilingLogger
from .config.settings import DATA_DIR

def get_filings(
    cik: Union[str, List[str], None] = None,
    form_type: str = "13F-HR",
    start_year: int = None,
    end_year: Optional[int] = None,
    user_agent: Optional[str] = None,
    base_dir: str = "./data_parse",
    log_dir: str = "./logs"
) -> None:
    """
    Download and parse SEC filings for one or more companies.
    
    Args:
        cik: Company CIK number(s) - can be a single CIK string, a list of CIKs, or None to get all CIKs
        form_type: Type of form to download (defaults to '13F-HR')
        start_year: Starting year (defaults to current year)
        end_year: Ending year (defaults to current year)
        user_agent: Email address for SEC's fair access rules
        base_dir: Base directory for parsed data (defaults to './data_parse')
        log_dir: Directory to store log files (defaults to './logs')
    """
    if start_year is None:
        start_year = datetime.today().year
        
    if end_year is None:
        end_year = start_year
        
    # Convert base_dir to absolute path
    base_dir = Path(base_dir).resolve()
    
    # Initialize downloader, parser, and logger
    downloader = SECDownloader(user_agent=user_agent)
    parser = SECFilingParser(base_dir=str(base_dir))
    logger = FilingLogger(log_dir=log_dir)
    
    # Get all CIKs if None is provided
    if cik is None:
        # Get index data to extract all available CIKs for the specified form type
        index_data = downloader.get_sec_index_data(start_year, end_year)
        if not index_data.empty:
            ciks = index_data[index_data["Form Type"].str.contains(form_type, na=False)]["CIK"].unique().tolist()
            logger.log_operation(
                download_success=True,
                download_error_message=f"Found {len(ciks)} CIKs for form type {form_type} from years {start_year}-{end_year}"
            )
        else:
            logger.log_operation(
                download_success=False,
                download_error_message=f"No CIKs found for form type {form_type} from years {start_year}-{end_year}"
            )
            return
    # Convert single CIK to list for uniform processing
    elif isinstance(cik, str):
        ciks = [cik]
    else:
        ciks = cik
    
    # Initialize result containers
    all_raw_files = {}
    all_parsed_files = {}
    all_metadata = {}
    
    # Process each CIK
    for current_cik in ciks:
        # Download filings
        try:
            downloaded = downloader.download_filings(
                cik=current_cik,
                form_type=form_type,
                start_year=start_year,
                end_year=end_year
            )
            
            if downloaded.empty:
                logger.log_operation(
                    cik=current_cik,
                    download_success=False,
                    download_error_message="No filings found"
                )
                all_raw_files[current_cik] = []
                all_parsed_files[current_cik] = {}
                all_metadata[current_cik] = pd.DataFrame()
                continue
            
            # For 13F filings, automatically parse them
            parsed_files = {}
            if form_type.startswith("13F"):
                for _, filing in downloaded.iterrows():
                    # Parse the filing
                    try:
                        raw_path = filing["raw_path"]
                        if os.path.exists(raw_path):
                            with open(raw_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            # Process the filing using the parser's process_filing method
                            parser.process_filing(content)
                            
                            # Log successful parse
                            logger.log_operation(
                                cik=current_cik,
                                accession_number=filing["accession_number"],
                                download_success=True,
                                parse_success=True
                            )
                        else:
                            logger.log_operation(
                                cik=current_cik,
                                accession_number=filing["accession_number"],
                                download_success=True,
                                parse_success=False,
                                download_error_message=f"Raw file not found at {raw_path}"
                            )
                    except Exception as e:
                        # Log parse error
                        logger.log_operation(
                            cik=current_cik,
                            accession_number=filing["accession_number"],
                            download_success=True,
                            parse_success=False,
                            download_error_message=f"Parse error: {str(e)}"
                        )
            
            # Store results for this CIK
            all_raw_files[current_cik] = downloaded["raw_path"].tolist()
            all_parsed_files[current_cik] = parsed_files
            all_metadata[current_cik] = downloaded
            
        except Exception as e:
            # Log download error
            logger.log_operation(
                cik=current_cik,
                download_success=False,
                download_error_message=f"Download error: {str(e)}"
            )
            all_raw_files[current_cik] = []
            all_parsed_files[current_cik] = {}
            all_metadata[current_cik] = pd.DataFrame()
    
    # Get all logs
    logs = logger.get_logs()
    
    # No return statement needed

__version__ = "0.1.0"
__all__ = ["get_filings", "SECDownloader", "SECFilingParser", "FilingLogger"]
