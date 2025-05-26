"""
Enhanced 13F filing parser - completely self-contained with comprehensive field extraction.
Updated to merge company and filing info into single 13f_info.csv file.
"""

import pandas as pd
import re
import xml.etree.ElementTree as ET
from typing import Optional, Tuple, Dict, Any
from pathlib import Path
import os

class Form13FParser:
    """Enhanced self-contained parser for 13F filings with comprehensive field extraction."""
    
    def __init__(self, output_dir: str = "./parsed_13f"):
        """Initialize parser with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def parse_filing(self, content: str) -> Dict[str, pd.DataFrame]:
        """
        Parse a complete 13F filing.
        
        Args:
            content: Raw filing content as string
        
        Returns:
            Dict containing 'filing_info' and 'holdings' DataFrames
        """
        result = {
            'filing_info': self._parse_filing_info(content),
            'holdings': pd.DataFrame()  # Default empty
        }
        
        # Extract and parse holdings
        xml_data, accession, date = self._extract_xml(content)
        if xml_data and accession and date:
            result['holdings'] = self._parse_holdings(xml_data, accession, date)
        
        return result
    
    def save_parsed_data(self, parsed_data: Dict[str, pd.DataFrame], accession_number: str, cik: str):
        """Save parsed data to CSV files using CIK-based structure."""
        base_filename = accession_number.replace('-', '_')
        
        for data_type, df in parsed_data.items():
            
            if data_type == "holdings":
                master_name = f"13f_holdings.csv"
                master_file_path = self.output_dir / f"{master_name}"
                
                if not df.empty:
                    # Ensure CIK is in the DataFrame - add it as the first column
                    df_with_cik = df.copy()
                    df_with_cik.insert(0, "CIK", cik)  # Insert CIK as first column
                    
                    # Check if master file exists
                    if not os.path.exists(master_file_path):
                        df_with_cik.to_csv(master_file_path, index=False)
                    else:
                        df_with_cik.to_csv(master_file_path, mode='a', header=False, index=False)
            
            elif data_type == "filing_info":
                # Save the merged company and filing info to 13f_info.csv
                if not df.empty:
                    filepath = self.output_dir / f"13f_info.csv"
                    if filepath.exists():
                        df.to_csv(filepath, mode='a', header=False, index=False)
                    else:
                        df.to_csv(filepath, index=False)
    
    def _parse_filing_info(self, content: str) -> pd.DataFrame:
        """Extract comprehensive filing and company information from 13F filing in one unified method."""
        # Combined patterns from both company and filing info parsers
        patterns = {
            # Define the desired column order based on your schema
            "ACCESSION_NUMBER": (r"ACCESSION NUMBER:\s+([\d\-]+)", pd.NA),
            "REPORT_TYPE": (r"reportType>([^<]+)</", pd.NA),
            "FORM_13F_FILE_NUMBER": (r"form13FFileNumber>([^<]+)</", pd.NA),
            "CIK": (r'CENTRAL INDEX KEY:\s+(\d+)', pd.NA),
            "DOC_TYPE": (r"CONFORMED SUBMISSION TYPE:\s+([\w-]+)", pd.NA),
            "CONFORMED_DATE": (r"CONFORMED PERIOD OF REPORT:\s+(\d+)", pd.NA),
            "FILED_DATE": (r"FILED AS OF DATE:\s+(\d+)", pd.NA),
            "ACCEPTANCE_DATETIME": (r"ACCEPTANCE-DATETIME>\s*(\d+)", pd.NA),
            "PUBLIC_DOCUMENT_COUNT": (r"PUBLIC DOCUMENT COUNT:\s+(\d+)", pd.NA),
            "SEC_ACT": (r"SEC ACT:\s+([^\r\n]+)", pd.NA),
            "SEC_FILE_NUMBER": (r"SEC FILE NUMBER:\s+([^\r\n]+)", pd.NA),
            "FILM_NUMBER": (r"FILM NUMBER:\s+(\d+)", pd.NA),
            "NUMBER_TRADES": (r"tableEntryTotal>(\d+)</", pd.NA),
            "TOTAL_VALUE": (r"tableValueTotal>(\d+)</", pd.NA),
            "OTHER_INCLUDED_MANAGERS_COUNT": (r"otherIncludedManagersCount>(\d+)</", pd.NA),
            "IS_CONFIDENTIAL_OMITTED": (r"isConfidentialOmitted>(true|false)</", pd.NA),
            "SIGNATURE_NAME": (r"<signatureBlock>\s*<name>([^<]+)</name>", pd.NA),
            "SIGNATURE_TITLE": (r"<signatureBlock>.*?<title>([^<]+)</title>", pd.NA),
            "SIGNATURE_CITY": (r"<signatureBlock>.*?<city>([^<]+)</city>", pd.NA),
            "SIGNATURE_STATE": (r"<signatureBlock>.*?<stateOrCountry>([^<]+)</stateOrCountry>", pd.NA),
            "AMENDMENT_FLAG": (r"amendmentFlag>(Y|N)</", pd.NA),
            
            # Company information fields
            "MAIL_STREET_2": (r"MAIL ADDRESS:.*?STREET 2:\s+([^\r\n]+)", pd.NA),
            "BUSINESS_STREET_1": (r"BUSINESS ADDRESS:.*?STREET 1:\s+([^\r\n]+)", pd.NA),
            "BUSINESS_STATE": (r"BUSINESS ADDRESS:.*?STATE:\s+([A-Z]{2})", pd.NA),
            "COMPANY_CONFORMED_NAME": (r"COMPANY CONFORMED NAME:\s+([^\r\n]+)", pd.NA),
            "BUSINESS_PHONE": (r"BUSINESS PHONE:\s+([\d\-\(\)\s]+)", pd.NA),
            "IRS_NUMBER": (r"(?:IRS NUMBER|EIN):\s+([\d-]+)", pd.NA),
            "MAIL_CITY": (r"MAIL ADDRESS:.*?CITY:\s+([^\r\n]+)", pd.NA),
            "MAIL_STREET_1": (r"MAIL ADDRESS:.*?STREET 1:\s+([^\r\n]+)", pd.NA),
            "STATE_INC": (r"STATE OF INCORPORATION:\s+([A-Z]{1,4})", pd.NA),
            "FORMER_COMPANY_NAME": (r"FORMER CONFORMED NAME:\s+([^\r\n]+)", pd.NA),
            "MAIL_ZIP": (r"MAIL ADDRESS:.*?ZIP:\s+(\d{5}(?:-\d{4})?)", pd.NA),
            "BUSINESS_CITY": (r"BUSINESS ADDRESS:.*?CITY:\s+([^\r\n]+)", pd.NA),
            "MAIL_STATE": (r"MAIL ADDRESS:.*?STATE:\s+([A-Z]{2})", pd.NA),
            "BUSINESS_STREET_2": (r"BUSINESS ADDRESS:.*?STREET 2:\s+([^\r\n]+)", pd.NA),
            "BUSINESS_ZIP": (r"BUSINESS ADDRESS:.*?ZIP:\s+(\d{5}(?:-\d{4})?)", pd.NA),
            "FISCAL_YEAR_END": (r"FISCAL YEAR END:\s+(\d{4})", pd.NA)
        }

        # Extract data using regex patterns with safety defaults
        info = {}
        for field, (pattern, default) in patterns.items():
            try:
                match = re.search(pattern, content, re.DOTALL)
                info[field] = match.group(1).strip() if match else default
            except (AttributeError, IndexError):
                info[field] = default
        
        # Add timestamp fields
        current_time = pd.Timestamp.now()
        info["CREATED_AT"] = current_time
        info["UPDATED_AT"] = current_time

        try:
            # Convert to DataFrame with desired column order
            desired_columns = [
                "ACCESSION_NUMBER", "REPORT_TYPE", "FORM_13F_FILE_NUMBER", "CIK", "DOC_TYPE",
                "CONFORMED_DATE", "FILED_DATE", "ACCEPTANCE_DATETIME", "PUBLIC_DOCUMENT_COUNT",
                "SEC_ACT", "SEC_FILE_NUMBER", "FILM_NUMBER", "NUMBER_TRADES", "TOTAL_VALUE",
                "OTHER_INCLUDED_MANAGERS_COUNT", "IS_CONFIDENTIAL_OMITTED", "SIGNATURE_NAME",
                "SIGNATURE_TITLE", "SIGNATURE_CITY", "SIGNATURE_STATE", "AMENDMENT_FLAG",
                "MAIL_STREET_2", "BUSINESS_STREET_1", "BUSINESS_STATE", "COMPANY_CONFORMED_NAME",
                "BUSINESS_PHONE", "IRS_NUMBER", "MAIL_CITY", "MAIL_STREET_1", "STATE_INC",
                "FORMER_COMPANY_NAME", "MAIL_ZIP", "BUSINESS_CITY", "MAIL_STATE",
                "BUSINESS_STREET_2", "BUSINESS_ZIP", "FISCAL_YEAR_END",
                "CREATED_AT", "UPDATED_AT"  # Added timestamp columns
            ]
            
            filing_info_df = pd.DataFrame([info])
            filing_info_df = filing_info_df.reindex(columns=desired_columns)
            
            # Convert date columns
            date_columns = ['CONFORMED_DATE', 'FILED_DATE']
            for col in date_columns:
                if col in filing_info_df.columns:
                    filing_info_df[col] = pd.to_datetime(
                        filing_info_df[col], format='%Y%m%d', errors='coerce')
            
            # Convert datetime columns
            if 'ACCEPTANCE_DATETIME' in filing_info_df.columns:
                filing_info_df['ACCEPTANCE_DATETIME'] = pd.to_datetime(
                    filing_info_df['ACCEPTANCE_DATETIME'], format='%Y%m%d%H%M%S', errors='coerce')
            
            # Convert numeric columns
            numeric_cols = [
                'NUMBER_TRADES', 'TOTAL_VALUE', 'OTHER_INCLUDED_MANAGERS_COUNT', 
                'PUBLIC_DOCUMENT_COUNT', 'FILM_NUMBER', 'CIK', 'FISCAL_YEAR_END'
            ]
            for col in numeric_cols:
                if col in filing_info_df.columns:
                    filing_info_df[col] = pd.to_numeric(filing_info_df[col], errors='coerce')
            
            # Convert boolean columns
            boolean_cols = ['IS_CONFIDENTIAL_OMITTED', 'AMENDMENT_FLAG']
            for col in boolean_cols:
                if col in filing_info_df.columns:
                    filing_info_df[col] = filing_info_df[col].map({
                        'true': True, 'false': False, 'Y': True, 'N': False
                    })

            return filing_info_df
        except Exception as e:
            # Return an empty DataFrame with proper columns if formatting fails
            desired_columns = [
                "ACCESSION_NUMBER", "REPORT_TYPE", "FORM_13F_FILE_NUMBER", "CIK", "DOC_TYPE",
                "CONFORMED_DATE", "FILED_DATE", "ACCEPTANCE_DATETIME", "PUBLIC_DOCUMENT_COUNT",
                "SEC_ACT", "SEC_FILE_NUMBER", "FILM_NUMBER", "NUMBER_TRADES", "TOTAL_VALUE",
                "OTHER_INCLUDED_MANAGERS_COUNT", "IS_CONFIDENTIAL_OMITTED", "SIGNATURE_NAME",
                "SIGNATURE_TITLE", "SIGNATURE_CITY", "SIGNATURE_STATE", "AMENDMENT_FLAG",
                "MAIL_STREET_2", "BUSINESS_STREET_1", "BUSINESS_STATE", "COMPANY_CONFORMED_NAME",
                "BUSINESS_PHONE", "IRS_NUMBER", "MAIL_CITY", "MAIL_STREET_1", "STATE_INC",
                "FORMER_COMPANY_NAME", "MAIL_ZIP", "BUSINESS_CITY", "MAIL_STATE",
                "BUSINESS_STREET_2", "BUSINESS_ZIP", "FISCAL_YEAR_END",
                "CREATED_AT", "UPDATED_AT"  # Added timestamp columns
            ]
            empty_df = pd.DataFrame(columns=desired_columns)
            return empty_df
    
    def _extract_xml(self, content: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract XML data from 13F filing with enhanced methods."""
        try:
            # Get accession and date
            acc_match = re.search(r"ACCESSION NUMBER:\s+([\d\-]+)", content)
            date_match = re.search(r"CONFORMED PERIOD OF REPORT:\s+(\d+)", content)
            
            accession = acc_match.group(1) if acc_match else None
            date = date_match.group(1) if date_match else None
            
            # Method 1: Find XML blocks between <XML> tags
            xml_start_tags = [match.start() for match in re.finditer(r'<XML>', content)]
            xml_end_tags = [match.start() for match in re.finditer(r'</XML>', content)]
            
            xml_indices = list(zip(xml_start_tags, xml_end_tags))
            
            if xml_indices:
                # Use the second XML section (index 1) as it typically contains the holdings data
                start_index, end_index = xml_indices[1] if len(xml_indices) > 1 else xml_indices[0]
                xml_content = content[start_index + 5:end_index]  # +5 to skip <XML>
                # Clean XML declaration
                xml_content = re.sub(r'<\?xml[^>]+\?>', '', xml_content).strip()
                
                if xml_content:
                    return xml_content, accession, date
            
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
                        # Clean XML declaration
                        xml_content = re.sub(r'<\?xml[^>]+\?>', '', xml_content).strip()
                        return xml_content, accession, date
            
            # Method 3: Look for informationTable directly
            info_table_match = re.search(
                r'<informationTable[^>]*>.*?</informationTable>', 
                content, 
                re.DOTALL | re.IGNORECASE
            )
            if info_table_match:
                xml_content = info_table_match.group(0)
                return xml_content, accession, date
            
            return None, accession, date
            
        except Exception:
            return None, None, None
    
    def _parse_holdings(self, xml_data: str, accession: str, date: str) -> pd.DataFrame:
        """Parse comprehensive holdings from 13F XML data with ALL SEC parser fields."""
        try:
            # Try to parse XML
            root = ET.fromstring(xml_data)
            
            # Common 13F namespaces
            namespaces = {
                'ns1': 'http://www.sec.gov/edgar/document/thirteenf/informationtable',
                'ns': 'http://www.sec.gov/edgar/document/thirteenf/informationtable',
                '': 'http://www.sec.gov/edgar/document/thirteenf/informationtable'
            }
            
            holdings = []
            
            # Try different namespace patterns systematically
            for ns_prefix in ['ns1:', 'ns:', '']:
                info_tables = root.findall(f'.//{ns_prefix}infoTable', namespaces)
                if not info_tables:
                    # Try without namespace
                    info_tables = root.findall('.//infoTable')
                
                if info_tables:
                    current_time = pd.Timestamp.now() # Get current time for all holdings in this batch
                    for entry in info_tables:
                        holding = {
                            # Filing identification
                            'ACCESSION_NUMBER': accession.replace('-', '_'),
                            'CONFORMED_DATE': date,  # Match SEC parser naming
                            
                            # ALL SEC Parser Holdings Fields - Core security information
                            'NAME_OF_ISSUER': self._get_xml_text(entry, f'{ns_prefix}nameOfIssuer', namespaces),
                            'TITLE_OF_CLASS': self._get_xml_text(entry, f'{ns_prefix}titleOfClass', namespaces),
                            'CUSIP': self._get_xml_text(entry, f'{ns_prefix}cusip', namespaces),
                            'SHARE_VALUE': self._get_xml_text(entry, f'{ns_prefix}value', namespaces),
                            
                            # Shares/Principal information
                            'SHARE_AMOUNT': self._get_xml_text(entry, f'{ns_prefix}shrsOrPrnAmt/{ns_prefix}sshPrnamt', namespaces),
                            'SH_PRN': self._get_xml_text(entry, f'{ns_prefix}shrsOrPrnAmt/{ns_prefix}sshPrnamtType', namespaces),
                            
                            # Options information
                            'PUT_CALL': self._get_xml_text(entry, f'{ns_prefix}putCall', namespaces),
                            
                            # Investment management
                            'DISCRETION': self._get_xml_text(entry, f'{ns_prefix}investmentDiscretion', namespaces),
                            
                            # Voting authority breakdown - ALL SEC parser names
                            'SOLE_VOTING_AUTHORITY': self._get_xml_text(entry, f'{ns_prefix}votingAuthority/{ns_prefix}Sole', namespaces),
                            'SHARED_VOTING_AUTHORITY': self._get_xml_text(entry, f'{ns_prefix}votingAuthority/{ns_prefix}Shared', namespaces),
                            'NONE_VOTING_AUTHORITY': self._get_xml_text(entry, f'{ns_prefix}votingAuthority/{ns_prefix}None', namespaces),
                            
                            # Timestamps
                            'CREATED_AT': current_time,
                            'UPDATED_AT': current_time
                        }
                        holdings.append(holding)
                    break  # Found data, no need to try other namespace patterns
            
            # If no namespaced elements found, try without any namespace
            if not holdings:
                current_time = pd.Timestamp.now() # Get current time for all holdings in this batch
                for entry in root.findall('.//infoTable'):
                    holding = {
                        'ACCESSION_NUMBER': accession.replace('-', '_'),
                        'CONFORMED_DATE': date,
                        'NAME_OF_ISSUER': self._get_xml_text_simple(entry, 'nameOfIssuer'),
                        'TITLE_OF_CLASS': self._get_xml_text_simple(entry, 'titleOfClass'),
                        'CUSIP': self._get_xml_text_simple(entry, 'cusip'),
                        'SHARE_VALUE': self._get_xml_text_simple(entry, 'value'),
                        'SHARE_AMOUNT': self._get_xml_text_simple(entry, 'shrsOrPrnAmt/sshPrnamt'),
                        'SH_PRN': self._get_xml_text_simple(entry, 'shrsOrPrnAmt/sshPrnamtType'),
                        'PUT_CALL': self._get_xml_text_simple(entry, 'putCall'),
                        'DISCRETION': self._get_xml_text_simple(entry, 'investmentDiscretion'),
                        'SOLE_VOTING_AUTHORITY': self._get_xml_text_simple(entry, 'votingAuthority/Sole'),
                        'SHARED_VOTING_AUTHORITY': self._get_xml_text_simple(entry, 'votingAuthority/Shared'),
                        'NONE_VOTING_AUTHORITY': self._get_xml_text_simple(entry, 'votingAuthority/None'),
                        
                        # Timestamps
                        'CREATED_AT': current_time,
                        'UPDATED_AT': current_time
                    }
                    holdings.append(holding)
            
            if not holdings:
                return pd.DataFrame()
            
            df = pd.DataFrame(holdings)
            
            # Enhanced data type conversion - ALL SEC parser numeric columns
            numeric_cols = [
                'SHARE_VALUE',
                'SHARE_AMOUNT', 
                'SOLE_VOTING_AUTHORITY',
                'SHARED_VOTING_AUTHORITY', 
                'NONE_VOTING_AUTHORITY'
            ]
            
            for col in numeric_cols:
                if col in df.columns:
                    # Clean numeric data exactly like SEC parser
                    df[col] = df[col].astype(str).str.replace(
                        r'\s+', '', regex=True).replace('', '0').astype(float, errors='ignore').astype(pd.Int64Dtype())
            
            # Convert date column
            if 'CONFORMED_DATE' in df.columns:
                df['CONFORMED_DATE'] = pd.to_datetime(df['CONFORMED_DATE'], format='%Y%m%d', errors='coerce')
            
            return df
            
        except Exception as e:
            return pd.DataFrame()
    
    def _get_xml_text(self, element, path: str, namespaces: dict) -> Optional[str]:
        """Safely extract text from XML element with namespace support."""
        try:
            found = element.find(path, namespaces)
            if found is not None and found.text:
                return found.text.strip()
            # Try without namespace if namespaced search failed
            path_no_ns = path.replace('ns1:', '').replace('ns:', '')
            found = element.find(path_no_ns)
            return found.text.strip() if found is not None and found.text else None
        except Exception:
            return None
    
    def _get_xml_text_simple(self, element, path: str) -> Optional[str]:
        """Safely extract text from XML element without namespace."""
        try:
            found = element.find(path)
            return found.text.strip() if found is not None and found.text else None
        except Exception:
            return None

    def get_cik_from_content(self, content: str) -> Optional[str]:
        """Extract CIK from filing content for use when calling save_parsed_data."""
        try:
            match = re.search(r"CENTRAL INDEX KEY:\s+(\d+)", content)
            return match.group(1) if match else None
        except Exception:
            return None

# Usage example
def process_13f_filing(file_path: str, parser: Form13FParser):
    """Process a single 13F filing file."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Parse the filing
        parsed_data = parser.parse_filing(content)
        
        # Extract CIK and accession number for saving
        cik = parser.get_cik_from_content(content)
        accession_match = re.search(r"ACCESSION NUMBER:\s+([\d\-]+)", content)
        accession = accession_match.group(1) if accession_match else "unknown"
        
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")