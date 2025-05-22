"""
Parser compatibility layer for SEC EDGAR filings.
This module provides backward compatibility with the original parser.
"""

from typing import Optional, Tuple, Dict, Any
import pandas as pd
import re
import xml.etree.ElementTree as ET
from lxml import etree
from ..parsers import Form13FParser

class SECFilingParser:
    """
    A compatibility wrapper for the new parser structure.
    This class maintains backward compatibility with code that used the original parser.
    """
    
    def __init__(self, base_dir: str = "./data_parse"):
        """
        Initialize the SEC filing parser.
        
        Args:
            base_dir: Base directory for parsed data
        """
        # Use the 13F parser since that was the original implementation
        self._parser = Form13FParser(base_dir=base_dir)
    
    def parse_company_info(self, content: str) -> pd.DataFrame:
        """
        Parse company information from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            pd.DataFrame: DataFrame containing company information
        """
        return self._parser.parse_company_info(content)
    
    def parse_accession_info(self, content: str) -> pd.DataFrame:
        """
        Parse accession information from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            pd.DataFrame: DataFrame containing accession information
        """
        return self._parser.parse_accession_info(content)
    
    def extract_xml(self, content: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract XML data from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            tuple: (XML data, accession number, conformed date)
        """
        return self._parser.extract_xml(content)
    
    def parse_holdings(self, xml_data: str, accession_number: str, conformed_date: str) -> pd.DataFrame:
        """
        Parse holdings information from XML data.
        
        Args:
            xml_data: XML data as string
            accession_number: Accession number
            conformed_date: Conformed date
            
        Returns:
            pd.DataFrame: DataFrame containing holdings information
        """
        return self._parser.parse_holdings(xml_data, accession_number, conformed_date)

    def process_filing(self, content: str) -> None:
        """
        Process a filing and save the parsed data.
        
        Args:
            content: Raw filing content
        """
        return self._parser.process_filing(content) 