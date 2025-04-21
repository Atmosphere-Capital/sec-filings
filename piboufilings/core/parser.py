"""
Parser functionality for SEC EDGAR filings.
"""

from typing import Optional, Tuple, Dict, Any
import pandas as pd
import re
import xml.etree.ElementTree as ET
from lxml import etree
from .data_organizer import DataOrganizer

class SECFilingParser:
    """A class to parse SEC EDGAR filings."""
    
    def __init__(self, base_dir: str = "./data_parse"):
        """
        Initialize the SECFilingParser.
        
        Args:
            base_dir: Base directory for parsed data
        """
        self.data_organizer = DataOrganizer(base_dir)
    
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
            "FILM_NUMBER": (r"FILM NUMBER:\s+(\d+)", pd.NA),
            "NUMBER_TRADES": (r"tableEntryTotal>(\d+)</", pd.NA),
            "TOTAL_VALUE": (r"tableValueTotal>(\d+)</", pd.NA),
            "OTHER_INCLUDED_MANAGERS_COUNT": (r"otherIncludedManagersCount>(\d+)</", pd.NA),
            "IS_CONFIDENTIAL_OMITTED": (r"isConfidentialOmitted>(true|false)</", pd.NA),
            "REPORT_TYPE": (r"reportType>(.+)</", pd.NA),
            "FORM_13F_FILE_NUMBER": (r"form13FFileNumber>(.+)</", pd.NA),
            "PROVIDE_INFO_FOR_INSTRUCTION5": (r"provideInfoForInstruction5>(Y|N)</", pd.NA),
            "SIGNATURE_NAME": (r"<signatureBlock>\s*<name>(.+?)</name>", pd.NA),
            "SIGNATURE_TITLE": (r"<title>(.+?)</title>", pd.NA),
            "SIGNATURE_PHONE": (r"<phone>([\d\-\(\)\s]+)</phone>", pd.NA)
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
            
            # Convert boolean column
            if 'IS_CONFIDENTIAL_OMITTED' in accession_info_df.columns:
                accession_info_df['IS_CONFIDENTIAL_OMITTED'] = accession_info_df['IS_CONFIDENTIAL_OMITTED'].map(
                    {'true': True, 'false': False})

            return accession_info_df
        except Exception as e:
            # Return an empty DataFrame with proper columns if formatting fails
            empty_df = pd.DataFrame(columns=list(patterns.keys()))
            return empty_df
    
    def extract_xml(self, content: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract XML data from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            tuple: (XML data, accession number, conformed date)
        """
        try:
            # Extract accession number
            accession_match = re.search(r"ACCESSION NUMBER:\s+(\d+-\d+-\d+)", content)
            accession_number = accession_match.group(1) if accession_match else None
            
            # Extract conformed date
            date_match = re.search(r"CONFORMED PERIOD OF REPORT:\s+(\d+)", content)
            conformed_date = date_match.group(1) if date_match else None
            
            # Method 1: Find XML between <XML> tags
            xml_start_tags = [match.start() for match in re.finditer(r'<XML>', content)]
            xml_end_tags = [match.start() for match in re.finditer(r'</XML>', content)]
            
            # Combine the results to show start and end indices
            xml_indices = list(zip(xml_start_tags, xml_end_tags))
            
            if xml_indices:
                # Use the second XML section (index 1) as it typically contains the holdings data
                start_index, end_index = xml_indices[1] if len(xml_indices) > 1 else xml_indices[0]
                xml_content = content[start_index:end_index + len('</XML>')]
                # Clean up XML declaration
                xml_content = re.sub(r'\n<\?xml.*?\?>', '', xml_content)
                return xml_content, accession_number, conformed_date
            
            # Method 2: Find XML after an XML declaration
            xml_decl_match = re.search(r'<\?xml[^>]+\?>', content)
            if xml_decl_match:
                start_index = xml_decl_match.start()
                # Find the first opening tag after the XML declaration
                opening_tag_match = re.search(r'<[^?][^>]*>', content[start_index:])
                if opening_tag_match:
                    tag_name = opening_tag_match.group(0).strip('<>').split()[0]
                    # Find the corresponding closing tag
                    closing_tag = f'</{tag_name}>'
                    closing_tag_index = content.rfind(closing_tag, start_index)
                    if closing_tag_index > start_index:
                        xml_content = content[start_index:closing_tag_index + len(closing_tag)]
                        return xml_content, accession_number, conformed_date
            
            # Method 3: Look for common 13F XML elements
            info_table_match = re.search(r'<informationTable[^>]*>.*?</informationTable>', content, re.DOTALL | re.IGNORECASE)
            if info_table_match:
                xml_content = f'<XML>{info_table_match.group(0)}</XML>'
                return xml_content, accession_number, conformed_date
                
            return None, accession_number, conformed_date
            
        except Exception as e:
            # Return None for all values on error
            return None, None, None
    
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
        try:
            # Define namespaces
            namespaces = {
                'ns1': 'http://www.sec.gov/edgar/document/thirteenf/informationtable',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }
            
            # Parse the XML
            root = ET.fromstring(xml_data)
            
            # Initialize a list to hold parsed data
            parsed_data = []
            
            # Loop through each 'infoTable' element
            for info_table in root.findall('.//ns1:infoTable', namespaces):
                data = {
                    'ACCESSION_NUMBER': accession_number,
                    'CONFORMED_DATE': conformed_date,
                    'NAME_OF_ISSUER': info_table.find('ns1:nameOfIssuer', namespaces).text if info_table.find('ns1:nameOfIssuer', namespaces) is not None else pd.NA,
                    'TITLE_OF_CLASS': info_table.find('ns1:titleOfClass', namespaces).text if info_table.find('ns1:titleOfClass', namespaces) is not None else pd.NA,
                    'CUSIP': info_table.find('ns1:cusip', namespaces).text if info_table.find('ns1:cusip', namespaces) is not None else pd.NA,
                    'SHARE_VALUE': info_table.find('ns1:value', namespaces).text if info_table.find('ns1:value', namespaces) is not None else pd.NA,
                    'SHARE_AMOUNT': info_table.find('ns1:shrsOrPrnAmt/ns1:sshPrnamt', namespaces).text if info_table.find('ns1:shrsOrPrnAmt/ns1:sshPrnamt', namespaces) is not None else pd.NA,
                    'SH_PRN': info_table.find('ns1:shrsOrPrnAmt/ns1:sshPrnamtType', namespaces).text if info_table.find('ns1:shrsOrPrnAmt/ns1:sshPrnamtType', namespaces) is not None else pd.NA,
                    'DISCRETION': info_table.find('ns1:investmentDiscretion', namespaces).text if info_table.find('ns1:investmentDiscretion', namespaces) is not None else pd.NA,
                    'SOLE_VOTING_AUTHORITY': info_table.find('ns1:votingAuthority/ns1:Sole', namespaces).text if info_table.find('ns1:votingAuthority/ns1:Sole', namespaces) is not None else pd.NA,
                    'SHARED_VOTING_AUTHORITY': info_table.find('ns1:votingAuthority/ns1:Shared', namespaces).text if info_table.find('ns1:votingAuthority/ns1:Shared', namespaces) is not None else pd.NA,
                    'NONE_VOTING_AUTHORITY': info_table.find('ns1:votingAuthority/ns1:None', namespaces).text if info_table.find('ns1:votingAuthority/ns1:None', namespaces) is not None else pd.NA,
                }
                parsed_data.append(data)
            
            # Convert to DataFrame
            df = pd.DataFrame(parsed_data)
            
            # Convert numeric columns
            columns_to_convert = [
                'SHARE_VALUE',
                'SHARE_AMOUNT',
                'SOLE_VOTING_AUTHORITY',
                'SHARED_VOTING_AUTHORITY',
                'NONE_VOTING_AUTHORITY'
            ]
            
            for column in columns_to_convert:
                if column in df.columns:
                    df[column] = df[column].astype(str).str.replace(
                        r'\s+', '', regex=True).replace('', '0').astype(float, errors='ignore').astype(pd.Int64Dtype())
            
            return df
            
        except Exception as e:
            # Return empty DataFrame on error
            return pd.DataFrame() 

    def process_filing(self, content: str) -> None:
        """
        Process a filing and save the parsed data.
        
        Args:
            content: Raw filing content
        """
        try:
            # Parse company information
            company_info_df = self.parse_company_info(content)
            
            # Parse accession information
            accession_info_df = self.parse_accession_info(content)
            
            # Extract and parse XML data
            xml_data, accession_number, conformed_date = self.extract_xml(content)
            
            # Initialize an empty holdings DataFrame as default
            holdings_df = pd.DataFrame()
            
            if xml_data:
                # Parse holdings information
                holdings_df = self.parse_holdings(xml_data, accession_number, conformed_date)
            
            # Validate data before saving
            if not accession_info_df.empty and 'ACCESSION_NUMBER' in accession_info_df.columns:
                # Save all parsed data
                self.data_organizer.process_filing_data(
                    accession_info_df,
                    company_info_df,
                    holdings_df
                )
        except Exception as e:
            # Silently handle the exception - errors should already be logged by the caller
            pass 