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

class SECDownloader:
    """A class to handle downloading SEC EDGAR filings."""
    
    def __init__(self, user_agent: Optional[str] = None):
        """
        Initialize the SEC downloader.
        
        Args:
            user_agent: Optional custom user agent string. If not provided,
                       uses the default from settings.
        """
        self.session = self._setup_session()
        self.headers = DEFAULT_HEADERS.copy()
        if user_agent:
            self.headers["User-Agent"] = user_agent
            
    def download_filings(
        self,
        cik: str,
        form_type: str,
        start_year: int,
        end_year: Optional[int] = None,
        save_raw: bool = True
    ) -> pd.DataFrame:
        """
        Download all filings of a specific type for a company within a date range.
        
        Args:
            cik: Company CIK number (will be zero-padded to 10 digits)
            form_type: Type of form to download (e.g., '13F-HR')
            start_year: Starting year for the search
            end_year: Ending year (defaults to current year)
            save_raw: Whether to save raw filing data (defaults to True)
            
        Returns:
            pd.DataFrame: DataFrame containing information about downloaded filings
        """
        # Normalize CIK
        cik = str(cik).zfill(10)
        
        # Get index data
        index_data = self.get_sec_index_data(start_year, end_year)
        
        # Filter for the specific company and form type
        company_filings = index_data[
            (index_data["CIK"] == cik) & 
            (index_data["Form Type"].str.contains(form_type, na=False))
        ]
        
        if company_filings.empty:
            return pd.DataFrame()
        
        # Download each filing
        downloaded_filings = []
        for _, filing in company_filings.iterrows():
            # Extract accession number from Filename
            accession_match = re.search(r'edgar/data/\d+/([0-9\-]+)\.txt', filing["Filename"])
            if not accession_match:
                continue
                
            accession_number = accession_match.group(1)
            
            filing_info = self._download_single_filing(
                cik=cik,
                accession_number=accession_number,
                form_type=form_type,
                save_raw=save_raw
            )
            if filing_info:
                downloaded_filings.append(filing_info)
        
        return pd.DataFrame(downloaded_filings)
    
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
        # Construct the URL
        # The accession number might contain hyphens, which need to be removed for the URL
        clean_accession = accession_number.replace('-', '')
        url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_accession}/{accession_number}.txt"
        # Download the filing
        response = self.session.get(url, headers=self.headers)
        
        if response.status_code != 200:
            return None
        
        # Save raw filing if requested
        if save_raw:
            raw_path = self._save_raw_filing(
                cik=cik,
                form_type=form_type,
                accession_number=accession_number,
                content=response.text
            )
        
        return {
            "cik": cik,
            "accession_number": accession_number,
            "form_type": form_type,
            "download_date": datetime.now().strftime("%Y-%m-%d"),
            "raw_path": raw_path if save_raw else None,
            "url": url
        }
    
    def _save_raw_filing(
        self,
        cik: str,
        form_type: str,
        accession_number: str,
        content: str
    ) -> str:
        """Save a raw filing to disk."""
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
        if end_year is None:
            end_year = datetime.today().year
            
        all_reports = []
        for year in range(start_year, end_year + 1):
            for quarter in range(1, 5):
                df = self._parse_form_idx(year, quarter)
                if not df.empty:
                    all_reports.append(df)
                    
        if not all_reports:
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
        
        return df_all[['CIK', 'Name', 'Date Filed', 'Form Type', 'accession_number', 'Filename']]
    
    def _parse_form_idx(self, year: int, quarter: int) -> pd.DataFrame:
        """Parse a specific quarter's form index file."""
        url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.idx"
        response = self.session.get(url, headers=self.headers)
        
        if response.status_code != 200:
            return pd.DataFrame()
            
        lines = response.text.splitlines()
        try:
            start_idx = next(i for i, line in enumerate(lines) if set(line.strip()) == {'-'})
        except StopIteration:
            return pd.DataFrame()
            
        entries = []
        for line in lines[start_idx + 1:]:
            try:
                entry = {
                    "Form Type": line[0:12].strip(),
                    "Name": line[12:74].strip(),
                    "CIK": line[74:86].strip(),
                    "Date Filed": line[86:98].strip(),
                    "Filename": line[98:].strip()
                }
                entries.append(entry)
            except Exception as e:
                continue
                
        return pd.DataFrame(entries)
    
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