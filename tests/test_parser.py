import pytest
import pandas as pd
from piboufilings.core.parser import SECFilingParser
from unittest.mock import patch, MagicMock

# Sample test data
SAMPLE_FILING = """
CENTRAL INDEX KEY:          0001234567
COMPANY CONFORMED NAME:     TEST COMPANY INC
STATE OF INCORPORATION:     DE
STANDARD INDUSTRIAL CLASSIFICATION: BUSINESS SERVICES
FISCAL YEAR END:            1231
ACCESSION NUMBER:           000-12345-6789
CONFORMED SUBMISSION TYPE:  13F-HR
CONFORMED PERIOD OF REPORT: 20231231
FILED AS OF DATE:           20240215

<XML>
<informationTable>
<nameOfIssuer>APPLE INC</nameOfIssuer>
<titleOfClass>COM</titleOfClass>
<cusip>037833100</cusip>
<value>1000000</value>
<shrsOrPrnAmt>
  <sshPrnamt>10000</sshPrnamt>
  <sshPrnamtType>SH</sshPrnamtType>
</shrsOrPrnAmt>
<investmentDiscretion>SOLE</investmentDiscretion>
<votingAuthority>
  <Sole>10000</Sole>
  <Shared>0</Shared>
  <None>0</None>
</votingAuthority>
</informationTable>
</XML>
"""

def test_parse_company_info():
    parser = SECFilingParser()
    result = parser.parse_company_info(SAMPLE_FILING)
    
    assert not result.empty
    assert result["CIK"].iloc[0] == "0001234567"
    assert result["COMPANY_CONFORMED_NAME"].iloc[0] == "TEST COMPANY INC"
    assert result["STATE_INC"].iloc[0] == "DE"
    
def test_extract_xml():
    parser = SECFilingParser()
    xml_data, accession, date = parser.extract_xml(SAMPLE_FILING)
    
    assert xml_data is not None
    assert "<informationTable>" in xml_data
    assert accession == "000-12345-6789"
    assert date == "20231231"
    
def test_parse_holdings():
    parser = SECFilingParser()
    xml_data, accession, date = parser.extract_xml(SAMPLE_FILING)
    
    holdings = parser.parse_holdings(xml_data, accession, date)
    
    assert not holdings.empty
    assert holdings["NAME_OF_ISSUER"].iloc[0] == "APPLE INC"
    assert holdings["CUSIP"].iloc[0] == "037833100"
    assert holdings["SHARE_VALUE"].iloc[0] == 1000000