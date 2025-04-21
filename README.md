# piboufilings

A Python library for downloading and parsing SEC EDGAR filings. This library provides a simple and efficient way to access and analyze SEC filing data, with a particular focus on 13F filings.

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

## Installation

```bash
pip install piboufilings
```

## Quick Start

```python
from piboufilings import SECDownloader, SECFilingParser

# Initialize the downloader
downloader = SECDownloader(user_agent="your_email@example.com")

# Get index data
index_data = downloader.get_sec_index_data(start_year=2020, end_year=2023)

# Initialize the parser
parser = SECFilingParser()

# Parse a filing
with open("filing.txt", "r") as f:
    filing_data = f.read()
    
company_info = parser.parse_company_info(filing_data)
accession_info = parser.parse_accession_info(filing_data)
holdings = parser.parse_holdings(xml_data, accession_number, conformed_date)
```

## Usage

### Downloading Filings

```python
from piboufilings import SECDownloader

# Initialize with your email
downloader = SECDownloader(user_agent="your_email@example.com")

# Get index data for a specific year range
index_data = downloader.get_sec_index_data(start_year=2020, end_year=2023)

# Filter for specific CIK or form type
filtered_data = index_data[
    (index_data["CIK"] == "0000320193") &  # Apple Inc.
    (index_data["Form Type"] == "13F-HR")
]
```

### Parsing Filings

```python
from piboufilings import SECFilingParser

parser = SECFilingParser()

# Parse company information
company_info = parser.parse_company_info(filing_data)

# Parse accession information
accession_info = parser.parse_accession_info(filing_data)

# Parse holdings data
holdings = parser.parse_holdings(xml_data, accession_number, conformed_date)
```

## License

This project is licensed under the Non-Commercial License - see the LICENSE file for details. Commercial use of this library is not authorized.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- SEC EDGAR for providing the filing data
- The Python community for the excellent tools that made this possible
