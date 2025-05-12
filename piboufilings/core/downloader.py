"""
Core functionality for downloading SEC filings.
"""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import time

from ..config.settings import (
    SEC_MAX_REQ_PER_SEC,
    SAFETY_FACTOR,
    SAFE_REQ_PER_SEC,
    REQUEST_DELAY,
    DEFAULT_HEADERS,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    RETRY_STATUS_CODES,
    DATA_DIR
)

from .logger import FilingLogger

class SECDownloader:
    """A class to handle downloading SEC EDGAR filings."""
    
    def __init__(self, user_agent: Optional[str] = None, log_dir: str = "./logs"):
        """
        Initialize the SEC downloader.
        
        Args:
            user_agent: Optional custom user agent string. If not provided,
                       uses the default from settings.
            log_dir: Directory to store log files (defaults to './logs')
        """
        self.session = self._setup_session()
        self.headers = DEFAULT_HEADERS.copy()
        if user_agent:
            self.headers["User-Agent"] = user_agent
        self.logger = FilingLogger(log_dir=log_dir)
        self.last_request_time = time.time() - REQUEST_DELAY  # Initialize to allow immediate first request
            
    def download_filings(
        self,
        cik: str,
        form_type: str,
        start_year: int,
        end_year: Optional[int] = None,
        save_raw: bool = True,
        show_progress: bool = True
    ) -> pd.DataFrame:
        """
        Download all filings of a specific type for a company within a date range.
        
        Args:
            cik: Company CIK number (will be zero-padded to 10 digits)
            form_type: Type of form to download (e.g., '13F-HR')
            start_year: Starting year for the search
            end_year: Ending year (defaults to current year)
            save_raw: Whether to save raw filing data (defaults to True)
            show_progress: Whether to show progress bars (defaults to True)
            
        Returns:
            pd.DataFrame: DataFrame containing information about downloaded filings
        """
        try:
            # Normalize CIK
            cik = str(cik).zfill(10)
            
            # Get index data
            index_data = self.get_sec_index_data(start_year, end_year)
            
            # Filter for the specific company and form type
            if not index_data.empty:
                company_filings = index_data[
                    (index_data["CIK"] == cik) & 
                    (index_data["Form Type"].str.contains(form_type, na=False))
                ]
                
                if company_filings.empty:
                    self.logger.log_operation(
                        cik=cik,
                        download_success=False,
                        download_error_message=f"No {form_type} filings found for {cik} between {start_year}-{end_year}"
                    )
                    return pd.DataFrame()
                
                # Download each filing with progress bar
                downloaded_filings = []
                
                # Add tqdm progress bar
                from tqdm import tqdm
                filing_iterator = tqdm(
                    company_filings.iterrows(), 
                    desc=f"Downloading filings for CIK {cik}", 
                    total=len(company_filings),
                    disable=not show_progress
                ) if show_progress else company_filings.iterrows()
                
                for _, filing in filing_iterator:
                    # Extract accession number from Filename
                    accession_match = re.search(r'edgar/data/\d+/([0-9\-]+)\.txt', filing["Filename"])
                    if not accession_match:
                        self.logger.log_operation(
                            cik=cik,
                            download_success=False,
                            download_error_message=f"Invalid filename format: {filing['Filename']}"
                        )
                        continue
                        
                    accession_number = accession_match.group(1)
                    
                    # Rate limiting
                    self._respect_rate_limit()
                    
                    filing_info = self._download_single_filing(
                        cik=cik,
                        accession_number=accession_number,
                        form_type=form_type,
                        save_raw=save_raw
                    )
                    if filing_info:
                        downloaded_filings.append(filing_info)
                
                self.logger.log_operation(
                    cik=cik,
                    download_success=True,
                    download_error_message=f"Successfully downloaded {len(downloaded_filings)} filings"
                )
                return pd.DataFrame(downloaded_filings)
            else:
                self.logger.log_operation(
                    cik=cik,
                    download_success=False,
                    download_error_message=f"Failed to get index data for years {start_year}-{end_year}"
                )
                return pd.DataFrame()
        except Exception as e:
            self.logger.log_operation(
                cik=cik,
                download_success=False,
                download_error_message=f"Error downloading filings: {str(e)}"
            )
            return pd.DataFrame()
    
    def _respect_rate_limit(self):
        """Ensure requests comply with SEC rate limits."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < REQUEST_DELAY:
            sleep_time = REQUEST_DELAY - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _download_single_filing(
        self,
        cik: str,
        accession_number: str,
        form_type: str,
        save_raw: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Download a single filing and save it if requested.
        
        Args:
            cik: Company CIK number
            accession_number: Filing accession number
            form_type: Type of form
            save_raw: Whether to save the raw filing
            
        Returns:
            Optional[Dict[str, Any]]: Information about the downloaded filing
        """
        try:
            # Construct the URL
            # The accession number might contain hyphens, which need to be removed for the URL
            clean_accession = accession_number.replace('-', '')
            url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_accession}/{accession_number}.txt"
            # Download the filing
            response = self.session.get(url, headers=self.headers)
            
            if response.status_code != 200:
                self.logger.log_operation(
                    cik=cik,
                    accession_number=accession_number,
                    download_success=False,
                    download_error_message=f"HTTP error {response.status_code}: {response.reason}"
                )
                return None
            
            # Save raw filing if requested
            raw_path = None
            if save_raw:
                try:
                    raw_path = self._save_raw_filing(
                        cik=cik,
                        form_type=form_type,
                        accession_number=accession_number,
                        content=response.text
                    )
                except IOError as e:
                    self.logger.log_operation(
                        cik=cik,
                        accession_number=accession_number,
                        download_success=True,
                        parse_success=False,
                        download_error_message=f"Failed to save raw filing: {str(e)}"
                    )
            
            self.logger.log_operation(
                cik=cik,
                accession_number=accession_number,
                download_success=True
            )
            
            return {
                "cik": cik,
                "accession_number": accession_number,
                "form_type": form_type,
                "download_date": datetime.now().strftime("%Y-%m-%d"),
                "raw_path": raw_path,
                "url": url
            }
        except requests.RequestException as e:
            self.logger.log_operation(
                cik=cik,
                accession_number=accession_number,
                download_success=False,
                download_error_message=f"Request error: {str(e)}"
            )
            return None
        except Exception as e:
            self.logger.log_operation(
                cik=cik,
                accession_number=accession_number,
                download_success=False,
                download_error_message=f"Unexpected error: {str(e)}"
            )
            return None
    
    def _save_raw_filing(
        self,
        cik: str,
        form_type: str,
        accession_number: str,
        content: str
    ) -> str:
        """
        Save a raw filing to disk.
        
        Args:
            cik: Company CIK number
            form_type: Type of form
            accession_number: Filing accession number
            content: Raw filing content
            
        Returns:
            str: Path to the saved file
            
        Raises:
            IOError: If there is an error creating directories or writing the file
        """
        # Ensure DATA_DIR exists
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(os.path.join(DATA_DIR, "raw"), exist_ok=True)
        
        # Create directory structure
        cik_dir = os.path.join(DATA_DIR, "raw", cik)
        
        # Check if this is an amendment filing
        is_amendment = form_type.endswith("/A") or "/A" in form_type
        
        # Handle the form type path correctly
        base_form_type = form_type.split("/A")[0] if is_amendment else form_type
        form_dir = os.path.join(cik_dir, base_form_type)
        os.makedirs(form_dir, exist_ok=True)
        
        # If it's an amendment, create an A subfolder
        if is_amendment:
            a_dir = os.path.join(form_dir, "A")
            os.makedirs(a_dir, exist_ok=True)
            output_dir = a_dir
        else:
            output_dir = form_dir
        
        # Save the filing
        output_path = os.path.join(output_dir, f"{cik}_{form_type}_{accession_number}.txt")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return output_path
    
    def get_sec_index_data(
        self,
        start_year: int = 1999,
        end_year: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get SEC EDGAR index data for the specified year range.
        
        Args:
            start_year: Starting year for the index data
            end_year: Ending year for the index data (defaults to current year)
            
        Returns:
            pd.DataFrame: DataFrame containing the index data
        """
        try:
            if end_year is None:
                end_year = datetime.today().year
                
            all_reports = []
            for year in range(start_year, end_year + 1):
                for quarter in range(1, 5):
                    # Skip future quarters
                    current_year = datetime.today().year
                    current_quarter = (datetime.today().month - 1) // 3 + 1
                    if year > current_year or (year == current_year and quarter > current_quarter):
                        continue
                        
                    # Rate limiting
                    self._respect_rate_limit()
                    
                    df = self._parse_form_idx(year, quarter)
                    if not df.empty:
                        all_reports.append(df)
                        
            if not all_reports:
                self.logger.log_operation(
                    download_success=False,
                    download_error_message=f"No index data found for years {start_year}-{end_year}"
                )
                return pd.DataFrame(columns=[
                    "CIK", "Name", "Date Filed", "Form Type",
                    "accession_number", "Filename"
                ])
                
            df_all = pd.concat(all_reports).reset_index(drop=True)
            
            # Extract CIK and accession number from Filename
            df_all[['CIK_extracted', 'accession_number']] = df_all['Filename'].str.extract(
                r'edgar/data/(\d+)/([0-9\-]+)\.txt'
            )
            
            # Clean up accession number (remove .txt if present)
            df_all['accession_number'] = df_all['accession_number'].str.replace('.txt', '')
            
            # Zero-pad CIK to 10 digits
            df_all['CIK'] = df_all['CIK_extracted'].str.zfill(10)
            
            self.logger.log_operation(
                download_success=True,
                download_error_message=f"Retrieved index data with {len(df_all)} entries"
            )
            
            return df_all[['CIK', 'Name', 'Date Filed', 'Form Type', 'accession_number', 'Filename']]
        except Exception as e:
            self.logger.log_operation(
                download_success=False,
                download_error_message=f"Error retrieving index data: {str(e)}"
            )
            return pd.DataFrame(columns=[
                "CIK", "Name", "Date Filed", "Form Type",
                "accession_number", "Filename"
            ])
    
    def _parse_form_idx(self, year: int, quarter: int) -> pd.DataFrame:
        """
        Parse a specific quarter's form index file.
        
        Args:
            year: Year to retrieve index for
            quarter: Quarter (1-4) to retrieve index for
            
        Returns:
            pd.DataFrame: DataFrame containing the parsed index data
        """
        try:
            url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.idx"
            response = self.session.get(url, headers=self.headers)
            
            if response.status_code != 200:
                self.logger.log_operation(
                    download_success=False,
                    download_error_message=f"Failed to retrieve index for {year} Q{quarter}: HTTP {response.status_code}"
                )
                return pd.DataFrame()
                
            lines = response.text.splitlines()
            try:
                start_idx = next(i for i, line in enumerate(lines) if set(line.strip()) == {'-'})
            except StopIteration:
                self.logger.log_operation(
                    download_success=False,
                    download_error_message=f"Unexpected format in index file for {year} Q{quarter}"
                )
                return pd.DataFrame()
                
            entries = []
            for line in lines[start_idx + 1:]:
                try:
                    if len(line) < 98:  # Ensure minimum line length
                        continue
                        
                    entry = {
                        "Form Type": line[0:12].strip(),
                        "Name": line[12:74].strip(),
                        "CIK": line[74:86].strip(),
                        "Date Filed": line[86:98].strip(),
                        "Filename": line[98:].strip()
                    }
                    entries.append(entry)
                except Exception as e:
                    # Skip individual entries that can't be parsed
                    continue
                    
            if not entries:
                self.logger.log_operation(
                    download_success=False,
                    download_error_message=f"No valid entries found in index for {year} Q{quarter}"
                )
                return pd.DataFrame()
                
            self.logger.log_operation(
                download_success=True,
                download_error_message=f"Successfully parsed {len(entries)} entries from {year} Q{quarter}"
            )
            return pd.DataFrame(entries)
        except Exception as e:
            self.logger.log_operation(
                download_success=False,
                download_error_message=f"Error parsing index for {year} Q{quarter}: {str(e)}"
            )
            return pd.DataFrame()
    
    def _setup_session(self) -> requests.Session:
        """Set up a requests session with retry logic."""
        session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session 