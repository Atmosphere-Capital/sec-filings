"""
piboufilings - A Python library for downloading and parsing SEC EDGAR filings.
"""

from typing import Optional, List, Dict, Any, Union
import pandas as pd
from datetime import datetime
import os
from pathlib import Path
from tqdm import tqdm
import requests

from .core.downloader import SECDownloader
from .core.logger import FilingLogger
from .core.parser import SECFilingParser  # For backward compatibility
# Import the new restructured parsers
from .parsers.form_13f_parser import Form13FParser
from .parsers.form_nport_parser import FormNPORTParser
from .parsers.parser_utils import get_parser_for_form_type, validate_filing_content
from .config.settings import DATA_DIR


def get_parser_for_form_type_internal(form_type: str, base_dir: str):
    """Get the appropriate parser for a form type using the new restructured parsers."""
    if "EX" in form_type:
        #Exhibit filings are parsed
        return None
    elif "13F" in form_type:
        return Form13FParser(output_dir=f"{base_dir}")
    elif "NPORT" in form_type:
        return FormNPORTParser(output_dir=f"{base_dir}")
    else:
        return None


def process_filings_for_cik(current_cik, downloaded, form_type, base_dir, logger, show_progress=True):
    """
    Process filings for a specific CIK with the restructured parsers.
    """
    logger.log_operation(
        operation_type="PROCESS_FILINGS_FOR_CIK_START",
        cik=current_cik,
        form_type_processed=form_type,
        download_error_message=f"Starting processing for CIK {current_cik}, Form {form_type}. Downloaded count: {len(downloaded) if downloaded is not None else 0}"
    )

    # Get parser for the form type
    parser = get_parser_for_form_type_internal(form_type, str(base_dir))
    if parser is None:
        logger.log_operation(
            cik=current_cik,
            operation_type="PARSER_LOOKUP", 
            download_success=True, 
            parse_success=False, 
            download_error_message=f"No parser available or needed for form type {form_type}. Skipping parsing."
        )
        return downloaded["raw_path"].tolist(), {}, downloaded
    
    # Determine parser_operation_type for logging, MUST be after parser is confirmed not None
    parser_operation_type = f"PARSER-{form_type.upper().replace('-', '_')}"

    # Filter valid filings (remove NaN paths)
    total_filings = len(downloaded)
    valid_filings = downloaded.dropna(subset=['raw_path'])
    remaining_filings = len(valid_filings)
    skipped_filings = total_filings - remaining_filings
    
    if skipped_filings > 0:
        logger.log_operation(
            cik=current_cik,
            accession_number=None, # This log is not per-accession
            operation_type="PARSER_INPUT_FILTER", # More generic type for pre-parsing step
            download_success=True, # This indicates the inputs to parsing might be problematic, not download itself
            download_error_message=f"Skipped {skipped_filings} filings (missing file paths). Proceeding with {remaining_filings} valid filings."
        )
        
    # Process filings with progress bar
    parsed_files = {}
    filing_iterator = tqdm(
        valid_filings.iterrows(),
        desc=f"Parsing {form_type} filings for CIK {current_cik}",
        total=remaining_filings,
        disable=not show_progress
    ) if show_progress else valid_filings.iterrows()
    
    successful_parses = 0
    total_holdings_extracted = 0
    
    for _, filing in filing_iterator:
        try:
            cik = filing["cik"]
            raw_path = filing["raw_path"]
            accession_number = filing["accession_number"]
            
            if not os.path.exists(raw_path):
                logger.log_operation(
                    cik=current_cik,
                    accession_number=accession_number,
                    operation_type=parser_operation_type,
                    download_success=True, 
                    parse_success=False,
                    download_error_message=f"Raw file not found at {raw_path}"
                )
                continue
            
            # Read and validate filing content
            with open(raw_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Quick validation
            validation = validate_filing_content(content)
            if not validation['is_valid_sec_filing']:
                logger.log_operation(
                    cik=current_cik,
                    accession_number=accession_number,
                    operation_type=parser_operation_type,
                    download_success=True,
                    parse_success=False,
                    download_error_message="Invalid SEC filing format"
                )
                continue
            
            # Parse the filing using the new parser structure
            parsed_data = parser.parse_filing(content)
            
            # Save parsed data
            parser.save_parsed_data(parsed_data, accession_number, cik)
            
            # Track parsing results
            holdings_count = len(parsed_data['holdings'])
            
            company_data_found = False
            # Check the type of parser to determine how to find company data
            if isinstance(parser, FormNPORTParser):
                if 'filing_info' in parsed_data and not parsed_data['filing_info'].empty:
                    # For NPORT, company info is part of filing_info.
                    # Check if 'COMPANY_NAME' column exists and has non-null values.
                    company_data_found = 'COMPANY_NAME' in parsed_data['filing_info'].columns and \
                                         not parsed_data['filing_info']['COMPANY_NAME'].dropna().empty
            elif 'company' in parsed_data: # Retain existing logic for other parsers (e.g., Form13FParser)
                 company_data_found = not parsed_data['company'].empty

            parsed_files[accession_number] = {
                'company_data_found': company_data_found,
                'filing_info_found': 'filing_info' in parsed_data and not parsed_data['filing_info'].empty,
                'holdings_count': holdings_count,
                'file_size_kb': len(content) // 1024,
                'form_type_detected': validation.get('form_type', form_type)
            }
            
            successful_parses += 1
            total_holdings_extracted += holdings_count
            
            # Log successful parse
            logger.log_operation(
                cik=current_cik,
                accession_number=accession_number,
                operation_type=parser_operation_type,
                download_success=True, 
                parse_success=True,
                download_error_message=f"Successfully parsed {holdings_count:,} holdings"
            )
            
        except Exception as e:
            logger.log_operation(
                cik=current_cik,
                accession_number=filing.get("accession_number", "unknown"),
                operation_type=parser_operation_type,
                download_success=True, 
                parse_success=False,
                download_error_message=f"Parse error: {str(e)}",
                error_code=getattr(e, 'response', {}).get('status_code', None) if isinstance(e, requests.RequestException) else None
            )
    
    logger.log_operation(
        operation_type="PROCESS_FILINGS_FOR_CIK_END",
        cik=current_cik,
        form_type_processed=form_type,
        download_error_message=f"Finished processing for CIK {current_cik}, Form {form_type}. Successful parses: {successful_parses}, Holdings: {total_holdings_extracted}"
    )
    return downloaded["raw_path"].tolist(), parsed_files, downloaded


def get_filings(
    user_agent: str,
    cik: Union[str, List[str], None] = None,
    form_type: Union[str, List[str]] = '13F-HR',
    start_year: int = None,
    end_year: Optional[int] = None,
    base_dir: str = "./data_parsed",
    log_dir: str = "./logs",
    show_progress: bool = True,
    max_workers: int = 5,
    keep_raw_files: bool = True
) -> None:
    """
    Download and parse SEC filings for one or more companies and form types.
    
    Args:
        user_agent: Email address for SEC's fair access rules (required)
        cik: Company CIK number(s) - can be a single CIK string, a list of CIKs, or None to get all CIKs
        form_type: Type of form(s) to download (e.g., '13F-HR', ['13F-HR', 'NPORT-P']). Defaults to '13F-HR'.
        start_year: Starting year (defaults to current year)
        end_year: Ending year (defaults to current year)
        base_dir: Base directory for parsed data (defaults to './data_parsed')
        log_dir: Directory to store log files (defaults to './logs')
        show_progress: Whether to show progress bars (defaults to True)
        max_workers: Maximum number of parallel download workers (defaults to 5)
        keep_raw_files: If False, raw filing files will be deleted after processing for each CIK. Defaults to True (files are kept).
    """
    
    
    ### VALIDATE INPUTS ###
    if start_year is None:
        start_year = datetime.today().year
        
    if end_year is None:
        end_year = start_year
        
    # Convert base_dir to absolute path
    base_dir = Path(base_dir).resolve()
    
    # Initialize downloader and logger
    downloader = SECDownloader(user_agent=user_agent, log_dir=log_dir, max_workers=max_workers)
    logger = FilingLogger(log_dir=log_dir)

    logger.log_operation(
        operation_type="GET_FILINGS_START",
        download_error_message=f"Starting get_filings. CIKs: {cik}, Forms: {form_type}, Years: {start_year}-{end_year}"
    )

    # Determine the list of form types to process
    form_type_list: List[str]
    if isinstance(form_type, str):
        form_type_list = [form_type]
    elif isinstance(form_type, list):
        form_type_list = form_type
    else:
        # This case should ideally not be reached if a default is set and type hints are followed,
        # but as a fallback or if None is explicitly passed after changes.
        logger.log_operation(
            operation_type="INPUT_VALIDATION_ERROR",
            cik=None,
            download_success=False,
            parse_success=False,
            download_error_message="Invalid form_type provided; defaulting to '13F-HR'."
        )
        form_type_list = ['13F-HR']


    ### GET ALL FILING FOR SPECIC DATE RANGE ###
    # Get index data once for all specified years
    full_index_data_for_years = downloader.get_sec_index_data(start_year, end_year)

    if full_index_data_for_years.empty:
        logger.log_operation(
            operation_type="INDEX_FETCH_FAIL",
            cik=None,
            download_success=False,
            parse_success=False,
            download_error_message=f"No index data found for years {start_year}-{end_year}. Cannot proceed."
        )
        return

    # Process each form type from the list
    for current_form_str in form_type_list:
        logger.log_operation(
            operation_type="FORM_TYPE_PROCESSING_START",
            cik=None,
            download_success=True,
            parse_success=None,
            download_error_message=f"Processing form type: {current_form_str}"
        )

        # Filter index data for the current form type
        index_data_for_current_form = full_index_data_for_years[
            full_index_data_for_years["Form Type"].str.contains(current_form_str, na=False)
        ]

        if index_data_for_current_form.empty:
            logger.log_operation(
                operation_type="INDEX_FILTER_NO_RESULTS",
                cik=None,
                form_type_processed=current_form_str,
                download_success=False,
                parse_success=False,
                download_error_message=f"No index entries found for form type {current_form_str} from years {start_year}-{end_year}"
            )
            continue # Move to the next form type in the list

        # Normalize CIKs for the current form's filtered index data
        index_data_for_current_form = index_data_for_current_form.copy() # Avoid SettingWithCopyWarning
        index_data_for_current_form.loc[:, "CIK"] = index_data_for_current_form["CIK"].astype(str)


        ### EXTRACT UNIQUE CIKS FROM FILTERED INDEX DATA FOR THE CURRENT FORM TYPE ###
        available_ciks_for_form = index_data_for_current_form["CIK"].unique().tolist()

        logger.log_operation(
            operation_type="CIK_IDENTIFICATION",
            download_success=True,
            parse_success=None,
            download_error_message=f"Found {len(available_ciks_for_form)} CIKs for form type {current_form_str} from years {start_year}-{end_year}"
        )

        ciks_to_process_for_current_form: List[str]
        if cik is not None: # User has specified CIK(s)
            user_ciks_input_list: List[str]
            if isinstance(cik, str):
                user_ciks_input_list = [str(cik).zfill(10)]
            elif isinstance(cik, list):
                user_ciks_input_list = [str(c).zfill(10) for c in cik]
            else: 
                logger.log_operation(
                    operation_type="INPUT_VALIDATION_ERROR",
                    cik=None,
                    download_success=False,
                    parse_success=False,
                    download_error_message="Invalid CIK input type."
                )
                continue 

            ciks_to_process_for_current_form = [
                c_val for c_val in user_ciks_input_list if c_val in available_ciks_for_form
            ]
            
            if not ciks_to_process_for_current_form:
                logger.log_operation(
                    operation_type="CIK_FILTER_NO_MATCH",
                    cik=", ".join(user_ciks_input_list),
                    form_type_processed=current_form_str,
                    download_success=False,
                    parse_success=False,
                    download_error_message=f"None of the provided CIK(s) filed form type {current_form_str} in the specified date range."
                )
                continue 
        else: 
            ciks_to_process_for_current_form = available_ciks_for_form

        if not ciks_to_process_for_current_form:
            logger.log_operation(
                operation_type="CIK_PROCESSING_SKIP",
                form_type_processed=current_form_str,
                download_success=False,
                parse_success=False,
                download_error_message=f"No CIKs to process for form type {current_form_str}."
            )
            continue 

        all_raw_files_for_cik_form = {} 
        all_parsed_files_for_cik_form = {}
        all_metadata_for_cik_form = {}
    
        cik_iterator = tqdm(
            ciks_to_process_for_current_form, 
            desc=f"Processing CIKs for {current_form_str}", 
            disable=not show_progress
        ) if show_progress else ciks_to_process_for_current_form
    
        for current_cik_str in cik_iterator:
            try:
                # Further filter the index_data_for_current_form for the specific CIK.
                # This will be the subset passed to download_filings.
                company_filings_to_download = index_data_for_current_form[
                    index_data_for_current_form["CIK"] == current_cik_str
                ]

                if company_filings_to_download.empty:
                    # This case should ideally be caught by the CIK processing logic above,
                    # but as a safeguard if a CIK was in ciks_to_process_for_current_form
                    # but somehow has no entries in index_data_for_current_form.
                    logger.log_operation(
                        cik=current_cik_str,
                        operation_type="DOWNLOAD_PRECHECK_FAIL",
                        form_type_processed=current_form_str,
                        download_success=False,
                        parse_success=False,
                        download_error_message=f"No specific index entries found for CIK {current_cik_str} and form {current_form_str} before download call."
                    )
                    # Ensure keys exist even if empty, then continue to next CIK
                    all_raw_files_for_cik_form[current_cik_str] = []
                    all_parsed_files_for_cik_form[current_cik_str] = {}
                    all_metadata_for_cik_form[current_cik_str] = pd.DataFrame()
                    continue

                downloaded_df = downloader.download_filings(
                    cik=current_cik_str,
                    form_type=current_form_str, 
                    start_year=start_year, # Still needed for fallback if index_data_subset is empty
                    end_year=end_year,   # Still needed for fallback
                    show_progress=False, 
                    index_data_subset=company_filings_to_download # Pass the pre-filtered subset
                )
            
                if downloaded_df.empty:
                    logger.log_operation(
                        cik=current_cik_str,
                        operation_type="DOWNLOAD_NO_FILES_FOR_CIK",
                        form_type_processed=current_form_str,
                        download_success=False,
                        parse_success=False,
                        download_error_message=f"No filings found for CIK {current_cik_str}, form {current_form_str}"
                    )
                    # Ensure keys exist even if empty
                    all_raw_files_for_cik_form[current_cik_str] = []
                    all_parsed_files_for_cik_form[current_cik_str] = {}
                    all_metadata_for_cik_form[current_cik_str] = pd.DataFrame()
                    continue # Next CIK
            
                # Process downloaded filings using the unified approach
                parser_specific_handling = False
                if any(substring in current_form_str.upper() for substring in ["13F", "NPORT"]):
                     parser_specific_handling = True
                
                if parser_specific_handling:
                    raw_files, parsed_files_data, metadata_df = process_filings_for_cik(
                        current_cik=current_cik_str,
                        downloaded=downloaded_df,
                        form_type=current_form_str, 
                        base_dir=base_dir,
                        logger=logger,
                        show_progress=False 
                    )
                
                    all_raw_files_for_cik_form[current_cik_str] = raw_files
                    all_parsed_files_for_cik_form[current_cik_str] = parsed_files_data
                    all_metadata_for_cik_form[current_cik_str] = metadata_df
                
                else:
                    logger.log_operation(
                        cik=current_cik_str,
                        operation_type="PARSING_SKIPPED_UNSUPPORTED_FORM",
                        form_type_processed=current_form_str,
                        download_success=True,
                        parse_success=False,
                        download_error_message=f"Form type '{current_form_str}' not specifically supported for parsing; storing raw files."
                    )
                    all_raw_files_for_cik_form[current_cik_str] = downloaded_df["raw_path"].tolist()
                    all_parsed_files_for_cik_form[current_cik_str] = {} 
                    all_metadata_for_cik_form[current_cik_str] = downloaded_df 
            
                # After processing (parsing or storing raw) for the current CIK and form type
                if not keep_raw_files and not downloaded_df.empty and 'raw_path' in downloaded_df.columns:
                    logger.log_operation(
                        cik=current_cik_str,
                        form_type_processed=current_form_str,
                        operation_type="RAW_FILE_DELETION_START",
                        download_success=True, 
                        parse_success=None, 
                        download_error_message=f"Attempting to delete {len(downloaded_df['raw_path'].dropna())} raw files for CIK {current_cik_str}, Form {current_form_str}."
                    )
                    deleted_count = 0
                    failed_count = 0
                    for raw_file_path in downloaded_df["raw_path"].dropna():
                        try:
                            if os.path.exists(raw_file_path):
                                os.remove(raw_file_path)
                                deleted_count += 1
                        except Exception as e_del:
                            failed_count += 1
                            logger.log_operation(
                                cik=current_cik_str,
                                form_type_processed=current_form_str,
                                operation_type="RAW_FILE_DELETION_ERROR",
                                download_success=True,
                                parse_success=False,
                                download_error_message=f"Failed to delete raw file {raw_file_path}: {str(e_del)}"
                            )
                    logger.log_operation(
                        cik=current_cik_str,
                        form_type_processed=current_form_str,
                        operation_type="RAW_FILE_DELETION_COMPLETE",
                        download_success=True,
                        parse_success=True if failed_count == 0 else False,
                        download_error_message=f"Deleted {deleted_count} raw files. Failed to delete {failed_count} files for CIK {current_cik_str}, Form {current_form_str}."
                    )

                    # Attempt to delete empty directories, from deepest to shallowest
                    raw_data_root = Path(DATA_DIR) / "raw"
                    cik_level_dir = raw_data_root / current_cik_str
                    
                    # Determine paths based on form type and potential amendment
                    base_form_name_for_dir = current_form_str.split('/A')[0]
                    form_level_dir = cik_level_dir / base_form_name_for_dir

                    dir_to_try_removing = []
                    if current_form_str.endswith("/A"):
                        dir_to_try_removing.append(form_level_dir / "A") # Innermost first (e.g. .../13F-HR/A)
                    dir_to_try_removing.append(form_level_dir) # Then parent form dir (e.g. .../13F-HR)
                    dir_to_try_removing.append(cik_level_dir)    # Then CIK dir (e.g. .../0001234567)

                    for dir_path in dir_to_try_removing:
                        if dir_path.exists():
                            try:
                                if not os.listdir(dir_path): # Check if empty
                                    os.rmdir(dir_path)
                                    logger.log_operation(
                                        cik=current_cik_str,
                                        form_type_processed=current_form_str, # Context for which operation led to this cleanup
                                        operation_type="DIR_DELETION_SUCCESS",
                                        download_error_message=f"Successfully deleted empty directory: {str(dir_path)}"
                                    )
                                # If not empty, os.rmdir would fail, so we don't need an explicit else log here for "not empty"
                            except OSError as e_rm_dir:
                                logger.log_operation(
                                    cik=current_cik_str,
                                    form_type_processed=current_form_str,
                                    operation_type="DIR_DELETION_ERROR",
                                    download_error_message=f"Error deleting directory {str(dir_path)} (it might not be empty or other issue): {str(e_rm_dir)}"
                                )
                        # If dir_path doesn't exist (e.g., already removed in a previous step), do nothing

            except Exception as e:
                logger.log_operation(
                    cik=current_cik_str,
                    operation_type="CIK_PROCESSING_ERROR",
                    form_type_processed=current_form_str,
                    download_success=False, 
                    parse_success=False,
                    download_error_message=f"Processing error for CIK {current_cik_str}, Form {current_form_str}: {str(e)}"
                )
                all_raw_files_for_cik_form[current_cik_str] = []
                all_parsed_files_for_cik_form[current_cik_str] = {}
                all_metadata_for_cik_form[current_cik_str] = pd.DataFrame()

    logger.log_operation(
        operation_type="GET_FILINGS_END",
        download_error_message=f"Finished get_filings. CIKs: {cik}, Forms: {form_type}, Years: {start_year}-{end_year}"
    )

__version__ = "0.1.1" # Incremented version due to new feature
__all__ = [
    "get_filings", 
    "SECDownloader", 
    "FilingLogger", 
    "Form13FParser",
    "FormNPORTParser",
    "SECFilingParser"  # For backward compatibility
]