"""
Data organization functionality for SEC EDGAR filings.
"""

import os
import pandas as pd
from typing import Optional, Dict, Any
from pathlib import Path

class DataOrganizer:
    """A class to organize parsed SEC EDGAR filing data."""
    
    def __init__(self, base_dir: str = "./data_parse"):
        """
        Initialize the DataOrganizer.
        
        Args:
            base_dir: Base directory for parsed data
        """
        self.base_dir = Path(base_dir).resolve()
        self.accession_info_file = self.base_dir / "accession_info.csv"
        self.company_info_file = self.base_dir / "company_info.csv"
        self.holdings_dir = self.base_dir / "holdings"
        
        # Create directories if they don't exist
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.holdings_dir.mkdir(parents=True, exist_ok=True)
    
    def save_accession_info(self, accession_info_df: pd.DataFrame) -> None:
        """
        Save accession information to a CSV file.
        
        Args:
            accession_info_df: DataFrame containing accession information
        """
        # Check if file exists to determine if we need to write headers
        file_exists = self.accession_info_file.exists()
        
        # Append to the CSV file
        accession_info_df.to_csv(
            self.accession_info_file,
            mode='a',
            header=not file_exists,
            index=False
        )
    
    def save_company_info(self, company_info_df: pd.DataFrame) -> None:
        """
        Save company information to a CSV file.
        
        Args:
            company_info_df: DataFrame containing company information
        """
        # Read existing company info if file exists
        if self.company_info_file.exists():
            existing_df = pd.read_csv(self.company_info_file)
            
            # Get the CIK from the new data
            new_cik = company_info_df['CIK'].iloc[0]
            
            # Check if this CIK already exists
            if new_cik in existing_df['CIK'].values:
                # Update the existing entry with new data
                existing_df.loc[existing_df['CIK'] == new_cik] = company_info_df.iloc[0]
            else:
                # Append new data
                existing_df = pd.concat([existing_df, company_info_df], ignore_index=True)
            
            # Save the updated DataFrame
            existing_df.to_csv(self.company_info_file, index=False)
        else:
            # If file doesn't exist, create it with the new data
            company_info_df.to_csv(self.company_info_file, index=False)
    
    def save_holdings(self, holdings_df: pd.DataFrame, cik: str, accession_number: str) -> None:
        """
        Save holdings information to a CSV file.
        
        Args:
            holdings_df: DataFrame containing holdings information
            cik: CIK number
            accession_number: Accession number
        """
        # Create CIK directory if it doesn't exist
        cik_dir = self.holdings_dir / cik
        cik_dir.mkdir(parents=True, exist_ok=True)
        
        # Save holdings to CSV file
        file_path = cik_dir / f"{accession_number}.csv"
        holdings_df.to_csv(file_path, index=False)
    
    def process_filing_data(self, 
                           accession_info_df: pd.DataFrame, 
                           company_info_df: pd.DataFrame, 
                           holdings_df: pd.DataFrame) -> None:
        """
        Process and save all filing data.
        
        Args:
            accession_info_df: DataFrame containing accession information
            company_info_df: DataFrame containing company information
            holdings_df: DataFrame containing holdings information
        """
        # Extract CIK and accession number
        cik = str(int(accession_info_df['CIK'].iloc[0]))
        accession_number = str(int(accession_info_df['ACCESSION_NUMBER'].iloc[0]))
        
        # Save all data
        self.save_accession_info(accession_info_df)
        self.save_company_info(company_info_df)
        self.save_holdings(holdings_df, cik, accession_number) 