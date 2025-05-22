"""
Parser for 13F filings.
"""

from typing import Optional, Tuple, Dict, Any
import pandas as pd
import re
import xml.etree.ElementTree as ET
from lxml import etree
from .base_parser import BaseFilingParser

class Form13FParser(BaseFilingParser):
    """A class to parse SEC EDGAR 13F filings."""
    
    def __init__(self, base_dir: str = "./data_parse"):
        """
        Initialize the 13F filing parser.
        
        Args:
            base_dir: Base directory for parsed data
        """
        super().__init__(base_dir)
        
    def parse_accession_info(self, content: str) -> pd.DataFrame:
        """
        Parse accession information from a 13F filing.
        
        Args:
            content: Raw filing content
            
        Returns:
            pd.DataFrame: DataFrame containing accession information
        """
        # Get the base accession info
        accession_info_df = super().parse_accession_info(content)
        
        # Add 13F-specific fields
        patterns = {
            "NUMBER_TRADES": (r"tableEntryTotal>(\d+)</", pd.NA),
            "TOTAL_VALUE": (r"tableValueTotal>(\d+)</", pd.NA),
            "OTHER_INCLUDED_MANAGERS_COUNT": (r"otherIncludedManagersCount>(\d+)</", pd.NA),
            "IS_CONFIDENTIAL_OMITTED": (r"isConfidentialOmitted>(true|false)</", pd.NA),
            "REPORT_TYPE": (r"reportType>(.+)</", pd.NA),
            "FORM_13F_FILE_NUMBER": (r"form13FFileNumber>(.+)</", pd.NA),
            "PROVIDE_INFO_FOR_INSTRUCTION5": (r"provideInfoForInstruction5>(Y|N)</", pd.NA),
            "SIGNATURE_NAME": (r"<signatureBlock>\s*<n>(.+?)</n>", pd.NA),
            "SIGNATURE_TITLE": (r"<title>(.+?)</title>", pd.NA),
            "SIGNATURE_PHONE": (r"<phone>([\d\-\(\)\s]+)</phone>", pd.NA)
        }
        
        # Extract 13F-specific data
        info = {}
        for field, (pattern, default) in patterns.items():
            try:
                match = re.search(pattern, content)
                info[field] = match.group(1).strip() if match else default
            except (AttributeError, IndexError):
                info[field] = default
        
        # Convert to DataFrame
        try:
            form_13f_df = pd.DataFrame([info])
            
            # Convert boolean column
            if 'IS_CONFIDENTIAL_OMITTED' in form_13f_df.columns:
                form_13f_df['IS_CONFIDENTIAL_OMITTED'] = form_13f_df['IS_CONFIDENTIAL_OMITTED'].map(
                    {'true': True, 'false': False})
            
            # Combine with base accession info
            for col in form_13f_df.columns:
                accession_info_df[col] = form_13f_df[col].values[0]
                
            return accession_info_df
        except Exception as e:
            # If there's an error, just return the base accession info
            return accession_info_df
    
    def extract_xml(self, content: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract XML data from a 13F filing.
        
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
        Parse holdings information from XML data in a 13F filing.
        
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
                    'PUT_CALL': info_table.find('ns1:putCall', namespaces).text if info_table.find('ns1:putCall', namespaces) is not None else pd.NA,
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
        Process a 13F filing and save the parsed data.
        
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