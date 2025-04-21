# PibouFilings

A Python library for downloading and parsing SEC EDGAR filings, with a focus on 13F filings.

[![PyPI version](https://badge.fury.io/py/piboufilings.svg)](https://badge.fury.io/py/piboufilings)
[![License: Non-Commercial](https://img.shields.io/badge/License-Non_Commercial-blue.svg)](LICENSE)

## Disclaimer

**This is an open-source project and is not affiliated with, endorsed by, or connected to the U.S. Securities and Exchange Commission (SEC) or EDGAR system.**

This library is provided for educational and research purposes only. Commercial use of this library is not authorized. Please refer to the [SEC's Fair Access rules](https://www.sec.gov/edgar/sec-api-documentation) for information about accessing SEC data for commercial purposes.

## Features

- Download SEC EDGAR filings with rate limiting and retry logic
- Parse company information from filings
- Extract holdings data from 13F filings
- Handle XML and text-based filing formats
- Efficient data processing with pandas
- Comprehensive logging and error handling
- Support for amended filings

## Installation

```bash
pip install piboufilings
```

## Quick Start

The simplest way to use piboufilings is with the high-level `get_filings()` function:

```python
from piboufilings import get_filings

# Replace with your email to comply with SEC Fair Access requirements
user_email = "your_email@example.com"

# Get all 13F-HR filings for a specific CIK
get_filings(
    cik="0001067983",  # Berkshire Hathaway
    form_type="13F-HR",
    start_year=2023,
    end_year=2023,
    user_agent=user_email
)
```

After running this, you will find the parsed data in the `./data_parse` directory.

## Advanced Usage

### Using SECDownloader directly

```python
from piboufilings import SECDownloader

# Initialize with your email
downloader = SECDownloader(user_agent="your_email@example.com")

# Get index data for a specific year range
index_data = downloader.get_sec_index_data(start_year=2020, end_year=2023)

# Filter for specific CIK and form type
filtered_data = index_data[
    (index_data["CIK"] == "0000320193") &  # Apple Inc.
    (index_data["Form Type"] == "13F-HR")
]

# Download the filtered filings
for _, filing in filtered_data.iterrows():
    cik = filing["CIK"]
    accession_number = filing["accession_number"]
    form_type = filing["Form Type"]
    downloader._download_single_filing(cik, accession_number, form_type)
```

### Using SECFilingParser for custom parsing

```python
from piboufilings import SECFilingParser

# Initialize the parser
parser = SECFilingParser()

# Read a previously downloaded filing
with open("./data_RAW/raw/0001067983/13F-HR/0001067983_13F-HR_0000950123-23-003772.txt", "r", encoding="utf-8") as f:
    filing_content = f.read()

# Parse company information
company_info_df = parser.parse_company_info(filing_content)

# Parse accession information
accession_info_df = parser.parse_accession_info(filing_content)

# Extract XML data
xml_data, accession_number, conformed_date = parser.extract_xml(filing_content)

# Parse holdings information
if xml_data:
    holdings_df = parser.parse_holdings(xml_data, accession_number, conformed_date)
    print(f"Found {len(holdings_df)} holdings")
```

### Working with amended filings

The library automatically detects amended filings (e.g., "13F-HR/A") and organizes them separately:

```python
from piboufilings import get_filings

# Get both original and amended filings
get_filings(
    cik="0001067983",
    form_type="13F-HR",  # Will also catch "13F-HR/A" filings
    start_year=2023,
    user_agent="your_email@example.com"
)

# To specifically target only amended filings
get_filings(
    cik="0001067983",
    form_type="13F-HR/A",
    start_year=2023,
    user_agent="your_email@example.com"
)
```

## Data Organization

After processing filings, the data is organized as follows:

- `./data_parse/company_info.csv` - Basic information about companies
- `./data_parse/accession_info.csv` - Filing metadata
- `./data_parse/holdings/{CIK}/{ACCESSION_NUMBER}.csv` - Parsed holdings data

Raw filings are stored in:

- `./data_RAW/raw/{CIK}/{FORM_TYPE}/{CIK}_{FORM_TYPE}_{ACCESSION_NUMBER}.txt`
- Amended filings: `./data_RAW/raw/{CIK}/{FORM_TYPE}/A/{CIK}_{FORM_TYPE}_{ACCESSION_NUMBER}.txt`

## Working with the Parsed Data

```python
import pandas as pd

# Load company information
company_info = pd.read_csv("./data_parse/company_info.csv")

# Load accession information
accession_info = pd.read_csv("./data_parse/accession_info.csv")

# Load holdings for a specific filing
cik = "0001067983"
accession_number = "0000950123-23-003772"
holdings = pd.read_csv(f"./data_parse/holdings/{cik}/{accession_number}.csv")

# Analyze the data
print(f"Total value of holdings: ${holdings['SHARE_VALUE'].sum():,}")
print(f"Number of unique securities: {holdings['NAME_OF_ISSUER'].nunique()}")

# Top 5 holdings by value
top_holdings = holdings.groupby('NAME_OF_ISSUER')['SHARE_VALUE'].sum().sort_values(ascending=False).head(5)
print("Top 5 holdings:")
print(top_holdings)
```

## Logging

Operations are logged to `./logs/filing_operations_{date}.csv`. You can analyze these logs with:

```python
from piboufilings import FilingLogger

# Initialize the logger
logger = FilingLogger()

# Get all logs
logs = logger.get_logs()

# Get logs for a specific CIK
cik_logs = logger.get_logs_by_cik("0001067983")

# Display success rate
success_rate = logs["download_success"].value_counts(normalize=True)
print(f"Download success rate: {success_rate.get('True', 0):.2%}")
```

## License

This project is licensed under the Non-Commercial License - see the [LICENSE](LICENSE) file for details. Commercial use of this library is not authorized.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Acknowledgments

- SEC EDGAR for providing the filing data
- The Python community for the excellent tools that made this possible