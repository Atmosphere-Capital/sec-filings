import pytest
from piboufilings.core.downloader import SECDownloader
from unittest.mock import patch, MagicMock

# Test initialization
def test_sec_downloader_init():
    downloader = SECDownloader(user_agent="test@example.com")
    assert downloader.headers["User-Agent"] == "test@example.com"
    
# Test rate limiting
def test_respect_rate_limit():
    downloader = SECDownloader()
    
    # Store the original time
    with patch('time.time') as mock_time, patch('time.sleep') as mock_sleep:
        # Set up time mocking
        mock_time.side_effect = [10.0, 10.1]  # First call and second call
        
        # Call the method
        downloader._respect_rate_limit()
        
        # Check if sleep was called with the correct value
        # REQUEST_DELAY - elapsed = REQUEST_DELAY - 0.1
        expected_sleep = downloader.REQUEST_DELAY - 0.1
        if expected_sleep > 0:
            mock_sleep.assert_called_once_with(expected_sleep)
        else:
            mock_sleep.assert_not_called()

# Test with mocked responses
@patch('requests.Session.get')
def test_download_single_filing(mock_get):
    # Setup mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<XML>Test Content</XML>"
    mock_get.return_value = mock_response
    
    # Initialize downloader with mocked save method to avoid file operations
    downloader = SECDownloader()
    downloader._save_raw_filing = MagicMock(return_value="/path/to/file.txt")
    
    # Test the method
    result = downloader._download_single_filing(
        cik="0001234567",
        accession_number="000-12345-6789",
        form_type="13F-HR",
        save_raw=True
    )
    
    # Assertions
    assert result is not None
    assert result["cik"] == "0001234567"
    assert result["accession_number"] == "000-12345-6789"
    assert result["form_type"] == "13F-HR"
    assert result["raw_path"] == "/path/to/file.txt"
    
    # Verify the correct URL was used
    expected_url = "https://www.sec.gov/Archives/edgar/data/1234567/00012345678/000-12345-6789.txt"
    mock_get.assert_called_with(expected_url, headers=downloader.headers)