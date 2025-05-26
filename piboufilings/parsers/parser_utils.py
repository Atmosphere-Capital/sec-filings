"""
Utility functions for parser selection and filing processing.
"""

import re
from typing import Optional, Union, Dict, Any
from pathlib import Path
from .form_13f_parser import Form13FParser
from .form_nport_parser import FormNPORTParser


def get_parser(content: str, output_dir: str = "./parsed_data") -> Optional[Union[Form13FParser, FormNPORTParser]]:
    """
    Get the appropriate parser for the filing content.
    
    Args:
        content: Raw filing content
        output_dir: Base output directory
    
    Returns:
        Appropriate parser instance or None if unsupported
    """
    # Extract form type
    form_match = re.search(r"CONFORMED SUBMISSION TYPE:\s+([\w\-]+)", content)
    
    if not form_match:
        return None
    
    form_type = form_match.group(1).upper()
    
    if "13F" in form_type:
        return Form13FParser(output_dir=f"{output_dir}/13f")
    elif "NPORT" in form_type:
        return FormNPORTParser(output_dir=f"{output_dir}/nport")
    
    return None


def get_parser_for_form_type(form_type: str, base_dir: str) -> Optional[Union[Form13FParser, FormNPORTParser]]:
    """
    Get the appropriate parser for a specific form type.
    
    Args:
        form_type: SEC form type (e.g., '13F-HR', 'NPORT-P')
        base_dir: Base directory for output
    
    Returns:
        Appropriate parser instance or None if unsupported
    """
    form_type = form_type.upper()
    
    if "13F" in form_type:
        return Form13FParser(output_dir=f"{base_dir}/13f_parsed")
    elif "NPORT" in form_type:
        return FormNPORTParser(output_dir=f"{base_dir}/nport_parsed")
    
    return None


def process_filing(filing_content: str, output_dir: str = "./parsed_data") -> Optional[Dict[str, Any]]:
    """
    Process a filing with the appropriate parser.
    
    Args:
        filing_content: Raw filing content
        output_dir: Output directory for parsed data
    
    Returns:
        Dict with parsing results or None if failed
    """
    parser = get_parser(filing_content, output_dir)
    
    if parser is None:
        return None
    
    try:
        # Parse the filing
        parsed_data = parser.parse_filing(filing_content)
        
        # Get accession number for filename
        accession = "unknown"
        if not parsed_data['filing_info'].empty and 'ACCESSION_NUMBER' in parsed_data['filing_info'].columns:
            accession = parsed_data['filing_info']['ACCESSION_NUMBER'].iloc[0]
        
        # Save the data
        parser.save_parsed_data(parsed_data, accession)
        
        # Return summary
        return {
            'parser_type': type(parser).__name__,
            'accession_number': accession,
            'company_data_found': not parsed_data['company'].empty,
            'filing_info_found': not parsed_data['filing_info'].empty,
            'holdings_count': len(parsed_data['holdings']),
            'parsed_data': parsed_data
        }
        
    except Exception as e:
        print(f"Error processing filing: {e}")
        return None


def get_supported_form_types() -> Dict[str, str]:
    """
    Get a dictionary of supported form types and their descriptions.
    
    Returns:
        Dict mapping form types to descriptions
    """
    return {
        "13F-HR": "Quarterly holdings report for institutional investment managers",
        "13F-HR/A": "Amendment to 13F-HR",
        "NPORT-P": "Monthly portfolio holdings report for mutual funds",
        "NPORT-P/A": "Amendment to NPORT-P",
        "NPORT-EX": "Exhibit filing for NPORT"
    }


def validate_filing_content(content: str) -> Dict[str, Any]:
    """
    Validate and analyze filing content.
    
    Args:
        content: Raw filing content
    
    Returns:
        Dict with validation results
    """
    validation_result = {
        'is_valid_sec_filing': False,
        'form_type': None,
        'accession_number': None,
        'cik': None,
        'company_name': None,
        'filing_date': None,
        'has_xml_data': False,
        'has_html_data': False,
        'file_size': len(content),
        'supported': False
    }
    
    try:
        # Check if it's a valid SEC filing
        if 'SEC-HEADER' in content or 'ACCESSION NUMBER' in content:
            validation_result['is_valid_sec_filing'] = True
        
        # Extract basic info
        form_match = re.search(r"CONFORMED SUBMISSION TYPE:\s+([\w\-]+)", content)
        if form_match:
            validation_result['form_type'] = form_match.group(1)
            validation_result['supported'] = any(
                supported_type in form_match.group(1).upper() 
                for supported_type in ['13F', 'NPORT']
            )
        
        acc_match = re.search(r"ACCESSION NUMBER:\s+([\d\-]+)", content)
        if acc_match:
            validation_result['accession_number'] = acc_match.group(1)
        
        cik_match = re.search(r"CENTRAL INDEX KEY:\s+(\d+)", content)
        if cik_match:
            validation_result['cik'] = cik_match.group(1)
        
        company_match = re.search(r"COMPANY CONFORMED NAME:\s+(.+)", content)
        if company_match:
            validation_result['company_name'] = company_match.group(1).strip()
        
        date_match = re.search(r"FILED AS OF DATE:\s+(\d+)", content)
        if date_match:
            validation_result['filing_date'] = date_match.group(1)
        
        # Check for data types
        validation_result['has_xml_data'] = bool(re.search(r'<XML>.*?</XML>', content, re.DOTALL))
        validation_result['has_html_data'] = bool(re.search(r'<TABLE|<HTML', content, re.IGNORECASE))
        
    except Exception as e:
        validation_result['error'] = str(e)
    
    return validation_result


def create_output_directories(base_dir: str) -> Dict[str, Path]:
    """
    Create output directories for parsed data.
    
    Args:
        base_dir: Base directory path
    
    Returns:
        Dict mapping parser types to their output directories
    """
    base_path = Path(base_dir)
    
    directories = {
        '13f': base_path / '13f_parsed',
        'nport': base_path / 'nport_parsed',
        'logs': base_path / 'logs'
    }
    
    for dir_path in directories.values():
        dir_path.mkdir(parents=True, exist_ok=True)
    
    return directories