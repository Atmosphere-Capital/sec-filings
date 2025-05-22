"""
Base parser class for SEC EDGAR filings.
"""

from typing import Optional, Dict, Any
import pandas as pd
import re
from pathlib import Path
from ..core.data_organizer import DataOrganizer

class BaseFilingParser:
    """Base class for all SEC EDGAR filing parsers."""
    
    def __init__(self, base_dir: str = "./data_parse"):
        """
        Initialize the base filing parser.
        
        Args:
            base_dir: Base directory for parsed data
        """
        self.base_dir = Path(base_dir).resolve()
        self.data_organizer = DataOrganizer(str(self.base_dir))
    
    def parse_company_info(self, content: str) -> pd.DataFrame:
        """
        Parse company information from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            pd.DataFrame: DataFrame containing company information
        """
        # Define regex patterns and default values for safety
        patterns = {
            "CIK": (r"CENTRAL INDEX KEY:\s+(\d+)", pd.NA),
            "IRS_NUMBER": (r"IRS NUMBER:\s+(\d+)", pd.NA),
            "COMPANY_CONFORMED_NAME": (r"COMPANY CONFORMED NAME:\s+(.+)", pd.NA),
            "DATE": (r"DATE AS OF CHANGE:\s+(\d+)", pd.NA),
            "STATE_INC": (r"STATE OF INCORPORATION:\s+([A-Z]+)", pd.NA),
            "SIC": (r"STANDARD INDUSTRIAL CLASSIFICATION:\s+([^[]+)", pd.NA),
            "ORGANIZATION_NAME": (r"ORGANIZATION NAME:\s+(.+)", pd.NA),
            "FISCAL_YEAR_END": (r"FISCAL YEAR END:\s+(\d+)", pd.NA),
            "BUSINESS_ADRESS_STREET_1": (r"STREET 1:\s+(.+)", pd.NA),
            "BUSINESS_ADRESS_STREET_2": (r"STREET 2:\s+(.+)", pd.NA),
            "BUSINESS_ADRESS_CITY": (r"CITY:\s+([A-Za-z]+)", pd.NA),
            "BUSINESS_ADRESS_STATE": (r"STATE:\s+([A-Z]+)", pd.NA),
            "BUSINESS_ADRESS_ZIP": (r"ZIP:\s+(\d+)", pd.NA),
            "BUSINESS_PHONE": (r"BUSINESS PHONE:\s+(\d+)", pd.NA),
            "MAIL_ADRESS_STREET_1": (r"STREET 1:\s+(.+)", pd.NA),
            "MAIL_ADRESS_STREET_2": (r"STREET 2:\s+(.+)", pd.NA),
            "MAIL_ADRESS_CITY": (r"CITY:\s+([A-Za-z]+)", pd.NA),
            "MAIL_ADRESS_STATE": (r"STATE:\s+([A-Z]+)", pd.NA),
            "MAIL_ADRESS_ZIP": (r"ZIP:\s+(\d+)", pd.NA),
            "FORMER_COMPANY_NAME": (r"FORMER CONFORMED NAME:\s+(.+)", pd.NA),
            "DATE_OF_NAME_CHANGE": (r"DATE OF NAME CHANGE:\s+(\d+)", pd.NA)
        }
            
        # Extract data using regex patterns with safety defaults
        info = {}
        for field, (pattern, default) in patterns.items():
            try:
                match = re.search(pattern, content)
                info[field] = match.group(1).strip() if match else default
            except (AttributeError, IndexError):
                info[field] = default
            
        # Convert to DataFrame and format the DATE columns
        try:
            cik_info_df = pd.DataFrame([info])
            cik_info_df['DATE'] = pd.to_datetime(cik_info_df['DATE'], format='%Y%m%d', errors='coerce')
            cik_info_df['DATE_OF_NAME_CHANGE'] = pd.to_datetime(cik_info_df['DATE_OF_NAME_CHANGE'], format='%Y%m%d', errors='coerce')
            return cik_info_df
        except Exception as e:
            # Return an empty DataFrame with proper columns if formatting fails
            empty_df = pd.DataFrame(columns=list(patterns.keys()))
            return empty_df
    
    def parse_accession_info(self, content: str) -> pd.DataFrame:
        """
        Parse accession information from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            pd.DataFrame: DataFrame containing accession information
        """
        # Define regex patterns and default values for safety
        patterns = {
            "CIK": (r'CENTRAL INDEX KEY:\s+(\d{10})', pd.NA),
            "ACCESSION_NUMBER": (r"ACCESSION NUMBER:\s+(\d+-\d+-\d+)", pd.NA),
            "DOC_TYPE": (r"CONFORMED SUBMISSION TYPE:\s+([\w-]+)", pd.NA),
            "FORM_TYPE": (r"<type>([\w-]+)</type>", pd.NA),
            "CONFORMED_DATE": (r"CONFORMED PERIOD OF REPORT:\s+(\d+)", pd.NA),
            "FILED_DATE": (r"FILED AS OF DATE:\s+(\d+)", pd.NA),
            "EFFECTIVENESS_DATE": (r"EFFECTIVENESS DATE:\s+(\d+)", pd.NA),
            "PUBLIC_DOCUMENT_COUNT": (r"PUBLIC DOCUMENT COUNT:\s+(\d+)", pd.NA),
            "SEC_ACT": (r"SEC ACT:\s+(.+)", pd.NA),
            "SEC_FILE_NUMBER": (r"SEC FILE NUMBER:\s+(.+)", pd.NA),
            "FILM_NUMBER": (r"FILM NUMBER:\s+(\d+)", pd.NA)
        }

        # Extract data using regex patterns with safety defaults
        info = {}
        for field, (pattern, default) in patterns.items():
            try:
                match = re.search(pattern, content)
                info[field] = match.group(1).strip() if match else default
            except (AttributeError, IndexError):
                info[field] = default

        try:
            # Convert to DataFrame and format the DATE columns
            accession_info_df = pd.DataFrame([info])
            
            # Safely convert date columns
            date_columns = ['CONFORMED_DATE', 'FILED_DATE', 'EFFECTIVENESS_DATE']
            for col in date_columns:
                if col in accession_info_df.columns:
                    accession_info_df[col] = pd.to_datetime(
                        accession_info_df[col], format='%Y%m%d', errors='coerce')
            
            # Safely convert numeric columns
            if 'ACCESSION_NUMBER' in accession_info_df.columns:
                accession_info_df['ACCESSION_NUMBER'] = accession_info_df['ACCESSION_NUMBER'].str.replace(
                    '-', '', regex=False).astype(float, errors='ignore')
            
            if 'CIK' in accession_info_df.columns:
                accession_info_df['CIK'] = accession_info_df['CIK'].astype(float, errors='ignore')

            return accession_info_df
        except Exception as e:
            # Return an empty DataFrame with proper columns if formatting fails
            empty_df = pd.DataFrame(columns=list(patterns.keys()))
            return empty_df
            
    def process_filing(self, content: str) -> None:
        """
        Process a filing and save the parsed data. This method should be overridden by subclasses.
        
        Args:
            content: Raw filing content
        """
        raise NotImplementedError("This method must be implemented by subclasses")
        
    def get_form_type(self, content: str) -> str:
        """
        Extract form type from filing content.
        
        Args:
            content: Raw filing content
            
        Returns:
            str: Form type
        """
        form_type_match = re.search(r"CONFORMED SUBMISSION TYPE:\s+([\w-]+)", content)
        return form_type_match.group(1).strip() if form_type_match else "" 