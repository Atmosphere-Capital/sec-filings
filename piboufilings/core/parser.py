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
        info = {
            "CIK": re.search(r"CENTRAL INDEX KEY:\s+(\d+)", content).group(1) if re.search(r"CENTRAL INDEX KEY:\s+(\d+)", content) else pd.NA,
            "IRS_NUMBER": re.search(r"IRS NUMBER:\s+(\d+)", content).group(1) if re.search(r"IRS NUMBER:\s+(\d+)", content) else pd.NA,
            "COMPANY_CONFORMED_NAME": re.search(r"COMPANY CONFORMED NAME:\s+(.+)", content).group(1).strip() if re.search(r"COMPANY CONFORMED NAME:\s+(.+)", content) else pd.NA,
            "DATE": re.search(r"DATE AS OF CHANGE:\s+(\d+)", content).group(1) if re.search(r"DATE AS OF CHANGE:\s+(\d+)", content) else pd.NA,
            "STATE_INC": re.search(r"STATE OF INCORPORATION:\s+([A-Z]+)", content).group(1) if re.search(r"STATE OF INCORPORATION:\s+([A-Z]+)", content) else pd.NA,
            "SIC": re.search(r"STANDARD INDUSTRIAL CLASSIFICATION:\s+([^[]+)", content).group(1).strip() if re.search(r"STANDARD INDUSTRIAL CLASSIFICATION:\s+([^[]+)", content) else pd.NA,
            "ORGANIZATION_NAME": re.search(r"ORGANIZATION NAME:\s+(.+)", content).group(1).strip() if re.search(r"ORGANIZATION NAME:\s+(.+)", content) else pd.NA,
            "FISCAL_YEAR_END": re.search(r"FISCAL YEAR END:\s+(\d+)", content).group(1) if re.search(r"FISCAL YEAR END:\s+(\d+)", content) else pd.NA,
            "BUSINESS_ADRESS_STREET_1": re.search(r"STREET 1:\s+(.+)", content).group(1).strip() if re.search(r"STREET 1:\s+(.+)", content) else pd.NA,
            "BUSINESS_ADRESS_STREET_2": re.search(r"STREET 2:\s+(.+)", content).group(1).strip() if re.search(r"STREET 2:\s+(.+)", content) else pd.NA,
            "BUSINESS_ADRESS_CITY": re.search(r"CITY:\s+([A-Za-z]+)", content).group(1) if re.search(r"CITY:\s+([A-Za-z]+)", content) else pd.NA,
            "BUSINESS_ADRESS_STATE": re.search(r"STATE:\s+([A-Z]+)", content).group(1) if re.search(r"STATE:\s+([A-Z]+)", content) else pd.NA,
            "BUSINESS_ADRESS_ZIP": re.search(r"ZIP:\s+(\d+)", content).group(1) if re.search(r"ZIP:\s+(\d+)", content) else pd.NA,
            "BUSINESS_PHONE": re.search(r"BUSINESS PHONE:\s+(\d+)", content).group(1) if re.search(r"BUSINESS PHONE:\s+(\d+)", content) else pd.NA,
            "MAIL_ADRESS_STREET_1": re.search(r"STREET 1:\s+(.+)", content).group(1).strip() if re.search(r"STREET 1:\s+(.+)", content) else pd.NA,
            "MAIL_ADRESS_STREET_2": re.search(r"STREET 2:\s+(.+)", content).group(1).strip() if re.search(r"STREET 2:\s+(.+)", content) else pd.NA,
            "MAIL_ADRESS_CITY": re.search(r"CITY:\s+([A-Za-z]+)", content).group(1) if re.search(r"CITY:\s+([A-Za-z]+)", content) else pd.NA,
            "MAIL_ADRESS_STATE": re.search(r"STATE:\s+([A-Z]+)", content).group(1) if re.search(r"STATE:\s+([A-Z]+)", content) else pd.NA,
            "MAIL_ADRESS_ZIP": re.search(r"ZIP:\s+(\d+)", content).group(1) if re.search(r"ZIP:\s+(\d+)", content) else pd.NA,
            "FORMER_COMPANY_NAME": re.search(r"FORMER CONFORMED NAME:\s+(.+)", content).group(1).strip() if re.search(r"FORMER CONFORMED NAME:\s+(.+)", content) else pd.NA,
            "DATE_OF_NAME_CHANGE": re.search(r"DATE OF NAME CHANGE:\s+(\d+)", content).group(1) if re.search(r"DATE OF NAME CHANGE:\s+(\d+)", content) else pd.NA
        }
            
        # Convert to DataFrame and format the DATE columns
        cik_info_df = pd.DataFrame([info])
        cik_info_df['DATE'] = pd.to_datetime(cik_info_df['DATE'], format='%Y%m%d', errors='coerce')
        cik_info_df['DATE_OF_NAME_CHANGE'] = pd.to_datetime(cik_info_df['DATE_OF_NAME_CHANGE'], format='%Y%m%d', errors='coerce')
        return cik_info_df
    
    def parse_accession_info(self, content: str) -> pd.DataFrame:
        """
        Parse accession information from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            pd.DataFrame: DataFrame containing accession information
        """
        info = {
            "CIK": re.search(r'CENTRAL INDEX KEY:\s+(\d{10})', content).group(1) if re.search(r'CENTRAL INDEX KEY:\s+(\d{10})', content) else pd.NA,
            "ACCESSION_NUMBER": re.search(r"ACCESSION NUMBER:\s+(\d+-\d+-\d+)", content).group(1) if re.search(r"ACCESSION NUMBER:\s+(\d+-\d+-\d+)", content) else pd.NA,
            "DOC_TYPE": re.search(r"CONFORMED SUBMISSION TYPE:\s+([\w-]+)", content).group(1) if re.search(r"CONFORMED SUBMISSION TYPE:\s+([\w-]+)", content) else pd.NA,
            "FORM_TYPE": re.search(r"<type>([\w-]+)</type>", content).group(1) if re.search(r"<type>([\w-]+)</type>", content) else pd.NA,
            "CONFORMED_DATE": re.search(r"CONFORMED PERIOD OF REPORT:\s+(\d+)", content).group(1) if re.search(r"CONFORMED PERIOD OF REPORT:\s+(\d+)", content) else pd.NA,
            "FILED_DATE": re.search(r"FILED AS OF DATE:\s+(\d+)", content).group(1) if re.search(r"FILED AS OF DATE:\s+(\d+)", content) else pd.NA,
            "EFFECTIVENESS_DATE": re.search(r"EFFECTIVENESS DATE:\s+(\d+)", content).group(1) if re.search(r"EFFECTIVENESS DATE:\s+(\d+)", content) else pd.NA,
            "PUBLIC_DOCUMENT_COUNT": re.search(r"PUBLIC DOCUMENT COUNT:\s+(\d+)", content).group(1) if re.search(r"PUBLIC DOCUMENT COUNT:\s+(\d+)", content) else pd.NA,
            "SEC_ACT": re.search(r"SEC ACT:\s+(.+)", content).group(1).strip() if re.search(r"SEC ACT:\s+(.+)", content) else pd.NA,
            "SEC_FILE_NUMBER": re.search(r"SEC FILE NUMBER:\s+(.+)", content).group(1).strip() if re.search(r"SEC FILE NUMBER:\s+(.+)", content) else pd.NA,
            "FILM_NUMBER": re.search(r"FILM NUMBER:\s+(\d+)", content).group(1) if re.search(r"FILM NUMBER:\s+(\d+)", content) else pd.NA,
            "NUMBER_TRADES": re.search(r"tableEntryTotal>(\d+)</", content).group(1) if re.search(r"tableEntryTotal>(\d+)</", content) else pd.NA,
            "TOTAL_VALUE": re.search(r"tableValueTotal>(\d+)</", content).group(1) if re.search(r"tableValueTotal>(\d+)</", content) else pd.NA,
            "OTHER_INCLUDED_MANAGERS_COUNT": re.search(r"otherIncludedManagersCount>(\d+)</", content).group(1) if re.search(r"otherIncludedManagersCount>(\d+)</", content) else pd.NA,
            "IS_CONFIDENTIAL_OMITTED": re.search(r"isConfidentialOmitted>(true|false)</", content).group(1) if re.search(r"isConfidentialOmitted>(true|false)</", content) else pd.NA,
            "REPORT_TYPE": re.search(r"reportType>(.+)</", content).group(1) if re.search(r"reportType>(.+)</", content) else pd.NA,
            "FORM_13F_FILE_NUMBER": re.search(r"form13FFileNumber>(.+)</", content).group(1) if re.search(r"form13FFileNumber>(.+)</", content) else pd.NA,
            "PROVIDE_INFO_FOR_INSTRUCTION5": re.search(r"provideInfoForInstruction5>(Y|N)</", content).group(1) if re.search(r"provideInfoForInstruction5>(Y|N)</", content) else pd.NA,
            "SIGNATURE_NAME": re.search(r"<signatureBlock>\s*<name>(.+?)</name>", content).group(1).strip() if re.search(r"<signatureBlock>\s*<name>(.+?)</name>", content) else pd.NA,
            "SIGNATURE_TITLE": re.search(r"<title>(.+?)</title>", content).group(1).strip() if re.search(r"<title>(.+?)</title>", content) else pd.NA,
            "SIGNATURE_PHONE": re.search(r"<phone>([\d\-\(\)\s]+)</phone>", content).group(1).strip() if re.search(r"<phone>([\d\-\(\)\s]+)</phone>", content) else pd.NA
        }

        # Convert to DataFrame and format the DATE columns
        accession_info_df = pd.DataFrame([info])
        accession_info_df['CONFORMED_DATE'] = pd.to_datetime(
            accession_info_df['CONFORMED_DATE'], format='%Y%m%d', errors='coerce')
        accession_info_df['FILED_DATE'] = pd.to_datetime(
            accession_info_df['FILED_DATE'], format='%Y%m%d', errors='coerce')
        accession_info_df['EFFECTIVENESS_DATE'] = pd.to_datetime(
            accession_info_df['EFFECTIVENESS_DATE'], format='%Y%m%d', errors='coerce')
        accession_info_df['ACCESSION_NUMBER'] = accession_info_df['ACCESSION_NUMBER'].str.replace(
            '-', '').astype(float, errors='ignore')
        accession_info_df['CIK'] = accession_info_df['CIK'].astype(float, errors='ignore')
        accession_info_df['IS_CONFIDENTIAL_OMITTED'] = accession_info_df['IS_CONFIDENTIAL_OMITTED'].map({'true': True, 'false': False})

        return accession_info_df
    
    def extract_xml(self, content: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract XML data from a filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            tuple: (XML data, accession number, conformed date)
        """
        # Extract accession number
        accession_match = re.search(r"ACCESSION NUMBER:\s+(\d+-\d+-\d+)", content)
        accession_number = accession_match.group(1) if accession_match else None
        
        # Extract conformed date
        date_match = re.search(r"CONFORMED PERIOD OF REPORT:\s+(\d+)", content)
        conformed_date = date_match.group(1) if date_match else None
        
        # Find the start and end indices of the XML sections
        xml_start_tags = [match.start() for match in re.finditer(r'<XML>', content)]
        xml_end_tags = [match.start() for match in re.finditer(r'</XML>', content)]
        
        # Combine the results to show start and end indices
        xml_indices = list(zip(xml_start_tags, xml_end_tags))
        
        if xml_indices:
            # Use the second XML section (index 1) as it typically contains the holdings data
            start_index, end_index = xml_indices[1] if len(xml_indices) > 1 else xml_indices[0]
            xml_content = content[start_index:end_index + len('</XML>')]
            xml_content = re.sub(r'\n<\?xml.*?\?>', '', xml_content)
            return xml_content, accession_number, conformed_date
        
        return None, accession_number, conformed_date
    
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
        # Parse company information
        company_info_df = self.parse_company_info(content)
        
        # Parse accession information
        accession_info_df = self.parse_accession_info(content)
        
        # Extract and parse XML data
        xml_data, accession_number, conformed_date = self.extract_xml(content)
        
        if xml_data:
            # Parse holdings information
            holdings_df = self.parse_holdings(xml_data, accession_number, conformed_date)
            
            # Save all parsed data
            self.data_organizer.process_filing_data(
                accession_info_df,
                company_info_df,
                holdings_df
            ) 