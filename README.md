# PibouFilings

A Python library to download, parse, and analyze SEC EDGAR filings—especially 13F and N-PORT filings—at scale.

[![PyPI version](https://badge.fury.io/py/piboufilings.svg)](https://badge.fury.io/py/piboufilings)
[![License: Non-Commercial](https://img.shields.io/badge/License-Non_Commercial-blue.svg)](LICENSE)
[![Build Status](https://github.com/pibou/piboufilings/actions/workflows/python-package.yml/badge.svg)](https://github.com/pibou/piboufilings/actions/workflows/python-package.yml)


## Disclaimer

**This is an open-source project, not affiliated with the U.S. Securities and Exchange Commission (SEC) or EDGAR.**

It is provided for educational and research purposes only. Commercial use is not permitted under the license terms. Refer to the [SEC Fair Access guidelines](https://www.sec.gov/edgar/sec-api-documentation) for more details. Always ensure your usage complies with these guidelines, especially regarding the User-Agent string.



## Key Features

-   **Automated Downloads:** Fetch 13F and N-PORT filings by CIK, date range, or retrieve all available.
-   **Smart Parsing:**
    -   `Form13FParser`: Extracts detailed holdings and cover page data from 13F-HR filings.
    -   `FormNPORTParser`: Parses comprehensive fund/filer information and security holdings from N-PORT-P filings.
-   **Structured CSV Output:**
    -   `13f_info.csv`: Filer information and summary for 13F forms.
    -   `13f_holdings.csv`: Aggregated holdings data from all processed 13F forms.
    -   `nport_filing_info.csv`: Fund/filer information and summaries for N-PORT forms.
    -   `nport_holdings.csv`: Aggregated holdings data from all processed N-PORT forms.
-   **Robust EDGAR Interaction:**
    -   Adheres to SEC rate limits (10 req/sec) via a configurable global token bucket rate limiter.
    -   Comprehensive retry mechanism for network requests (handles connection errors, read errors, and specific HTTP status codes like 429, 5xx).
-   **Efficient & Configurable:**
    -   Parallelized downloads using `ThreadPoolExecutor` for faster processing of CIKs with multiple filings.
    -   Option to `keep_raw_files` (default True) or delete them after processing.
    -   Customizable directories for data and logs.
-   **Detailed Logging:**
    -   Records operations to a daily CSV log file (e.g., `logs/filing_operations_YYYYMMDD.csv`).
    -   Logs include timestamps, descriptive `operation_type` (e.g., `DOWNLOAD_SINGLE_FILING_SUCCESS`), CIK, accession number, success/failure status, error messages, and specific `error_code` (like HTTP status codes) where applicable.
-   **Data Analytics Ready:** Pandas DataFrames are used internally and for the final CSV outputs.
-   **Handles Amendments:** Automatically processes and correctly identifies amended filings (e.g., `13F-HR/A`, `NPORT-P/A`).


## Supported Form Types

| Category       | Supported Forms                               | Notes                                                                 |
|----------------|-----------------------------------------------|-----------------------------------------------------------------------|
| 13F Filings    | `13F-HR`, `13F-HR/A`                          | Institutional Investment Manager holdings reports.                    |
| N-PORT Filings | `NPORT-P`, `NPORT-P/A`                        | Monthly portfolio holdings for registered investment companies (funds). |
| Ignored        | `NPORT-EX`, `NPORT-EX/A`, `NT NPORT-P`, `NT NPORT-EX` | Exhibit-only or notice filings, typically not parsed for holdings.    |


## Installation

```bash
pip install piboufilings
```

## Quick Start

The primary way to use `piboufilings` is with the `get_filings()` function:

```python
from piboufilings import get_filings

# Remember to replace with your actual email for the User-Agent
USER_AGENT_EMAIL = "yourname@example.com"

get_filings(
    user_agent=USER_AGENT_EMAIL,
    cik="0001067983",              # Example: Berkshire Hathaway CIK
    form_type=["13F-HR", "NPORT-P"], # Can be a string or list of strings
    start_year=2023,
    end_year=2023,
    base_dir="./my_sec_data",       # Optional: Custom directory for parsed CSVs
    log_dir="./my_sec_logs",        # Optional: Custom directory for logs
    keep_raw_files=True            # Optional: Set to False to delete raw .txt files after parsing
)
```

After running, parsed data will be in `./my_sec_data` (or `./data_parsed` by default) and logs in `./my_sec_logs` (or `./logs` by default). Raw downloaded files (if kept) are stored in a separate `data_raw` directory (by default relative to the project root if `piboufilings` is run as a local script, or in a `data_raw` subdir of your current working dir if installed as a library and `DATA_DIR` setting is not customized).

## Data Organization

### Parsed Data
By default, parsed CSV files are saved in a directory named `data_parsed` within your current working directory (or as specified by `base_dir` in `get_filings`):

-   `./data_parsed/13f_info.csv`: Contains filer information and report summaries from all processed 13F filings.
-   `./data_parsed/13f_holdings.csv`: Contains all individual holdings from all processed 13F filings, appended together.
-   `./data_parsed/nport_filing_info.csv`: Contains fund/filer information and report summaries from all processed N-PORT filings.
-   `./data_parsed/nport_holdings.csv`: Contains all individual holdings from all processed N-PORT filings, appended together.

### Raw Data
Raw `.txt` filings (if `keep_raw_files=True`) are stored based on the `DATA_DIR` setting in `piboufilings.config.settings`. By default, this resolves to a `data_raw` directory relative to the project's root when running from source, or a `data_raw` subdirectory from where your script using the library is executed. The structure within is:

-   `.../data_raw/raw/{CIK}/{FORM_TYPE_BASE}/{FILENAME}.txt`
-   Example for an amended filing: `.../data_raw/raw/{CIK}/{FORM_TYPE_BASE}/A/{FILENAME}.txt`
    *(Note: `{FORM_TYPE_BASE}` is the form type without `/A`, e.g., "13F-HR")*


## Advanced Usage & Components

While `get_filings()` is the main interface, the library's components can be used individually if needed:

-   **`SECDownloader` (`piboufilings.core.downloader`):** Handles fetching index files and individual filings, incorporating rate limiting and retries.
    ```python
    from piboufilings import SECDownloader
    downloader = SECDownloader(user_agent="yourname@example.com")
    index_data = downloader.get_sec_index_data(start_year=2023, end_year=2023)
    #filing_content_info = downloader._download_single_filing(cik="...", accession_number="...", form_type="...")
    ```
-   **Form-Specific Parsers (`piboufilings.parsers`):**
    -   `Form13FParser`: For 13F-HR filings.
    -   `FormNPORTParser`: For N-PORT-P filings.
    ```python
    from piboufilings.parsers import Form13FParser # or FormNPORTParser
    parser = Form13FParser(output_dir="./my_parsed_data") # output_dir is where CSVs are saved
    with open("path/to/raw_13f_filing.txt", 'r') as f:
        content = f.read()
    parsed_data_dict = parser.parse_filing(content) # Returns {'filing_info': DataFrame, 'holdings': DataFrame}
    parser.save_parsed_data(parsed_data_dict, accession_number="...", cik="...")
    ```
-   **`FilingLogger` (`piboufilings.core.logger`):** Manages CSV logging.
    ```python
    from piboufilings import FilingLogger
    logger = FilingLogger(log_dir="./my_custom_logs")
    logs_df = logger.get_logs()
    cik_logs_df = logger.get_logs_by_cik("0001067983")
    ```

## Logging Details

Operations are logged to `./logs/filing_operations_{YYYYMMDD}.csv` (or your custom `log_dir`).
Key columns include:
-   `timestamp`: Time of the log entry.
-   `operation_type`: Descriptive type of operation (e.g., `DOWNLOAD_SINGLE_FILING_SUCCESS`, `INDEX_FETCH_HTTP_ERROR`, `PROCESS_FILINGS_FOR_CIK_START`).
-   `cik`: CIK involved (or "SYSTEM").
-   `form_type_processed`: Form type context for the log.
-   `accession_number`: Accession number if applicable.
-   `download_success`: Boolean.
-   `download_error_message`: Detailed error or informational message.
-   `parse_success`: Boolean.
-   `error_code`: Specific error code, like HTTP status codes (e.g., 404, 429).


## Roadmap / Future Enhancements
-   Support for additional SEC form types (e.g., 10-K, 10-Q, 8-K, Form 4).
-   More granular parsing for specific sections within N-PORT filings (e.g., derivatives, liquidity classifications).
-   Enhanced data validation and cleaning steps.
-   Option for different output formats (e.g., Parquet, database).
-   Tutorials and more detailed examples in a documentation site.

## License

This project is licensed under a Non-Commercial License. Please see the [LICENSE](LICENSE) file for details. Commercial use of this library is not authorized without explicit permission.

## Contributing

Contributions are welcome! If you'd like to contribute, please feel free to fork the repository, make your changes, and submit a Pull Request.
Consider a few areas:
1.  **New Parsers:** Implementing parsers for other form types.
2.  **Feature Enhancements:** Adding capabilities from the roadmap or new ideas.
3.  **Bug Fixes & Performance Improvements.**
4.  **Documentation & Examples.**

When contributing, please ensure your code is well-tested and follows the general structure of the library.

## Acknowledgments

-   The U.S. Securities and Exchange Commission (SEC) for providing public access to EDGAR filing data.
-   The Python community and the developers of libraries like `requests`, `pandas`, and `tqdm`.
