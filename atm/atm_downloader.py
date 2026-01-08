# ---- version-agnostic, idempotent patch for piboufilings logger ----
import importlib
import piboufilings as _pb

# Try to locate FilingLogger no matter where it is packaged
_FL = getattr(_pb, "FilingLogger", None)
if _FL is None:
    for modname in (
        "piboufilings.filing_logger",
        "piboufilings.logger",
        "piboufilings.utils.filing_logger",
    ):
        try:
            m = importlib.import_module(modname)
            _FL = getattr(m, "FilingLogger", None)
            if _FL:
                break
        except Exception:
            pass

if _FL is None:
    raise ImportError("Could not locate piboufilings.FilingLogger in this install.")

# Apply the patch only once
if not getattr(_FL.log_operation, "_pb_strip_custom_id", False):
    _orig = _FL.log_operation

    def _patched(self, *args, **kwargs):
        kwargs.pop("custom_identifier", None)
        return _orig(self, *args, **kwargs)

    _patched._pb_strip_custom_id = True
    _FL.log_operation = _patched
# ---- end patch ----

if __name__ == '__main__':
    from piboufilings import get_filings, SECDownloader
    USER_AGENT_EMAIL = "claudio.yano@atmospherecapital.com.br"
    USER_NAME = "Atmosphere Capital" # Add your name or company

    get_filings(
        user_name=USER_NAME,
        user_agent_email=USER_AGENT_EMAIL,
        form_type=["13F-HR"], # Can be a string or list of strings
        start_year=2025,
        end_year=2025,
        base_dir=r"C:\Users\ClaudioYano\CC\13F\data3",       # Optional: Custom directory for parsed CSVs
        log_dir=r"C:\Users\ClaudioYano\CC\13F\logs",        # Optional: Custom directory for logs
        keep_raw_files=True            # Optional: Set to False to delete raw .txt files after parsing
    )

    downloader = SECDownloader(
        user_name=USER_NAME,
        user_agent_email=USER_AGENT_EMAIL,
        package_version='0.2.1',
        log_dir=r"C:\Users\ClaudioYano\CC\13F\logs",
        max_workers=5
    )


#
# csv = pd.read_csv(r"C:\Users\ClaudioYano\CC\13F\data3\13f_holdings.csv")
#
# cusip_mapping = {
#     (
#         str(int(d['BBG']['ID_CUSIP']))
#         if isinstance(d['BBG']['ID_CUSIP'], (int, float))
#         else str(d['BBG']['ID_CUSIP'])
#     ): str(d['ticker']['bbg'])
#     for d in mdb.gestao.asset.metadata.find(
#         {'BBG.ID_CUSIP': {'$exists': True}, 'ticker.bbg': {'$exists': True}},
#         {'BBG.ID_CUSIP': 1, 'ticker.bbg': 1}
#     )
# }
#
# csv['Ticker'] = csv['CUSIP'].map(cusip_mapping)
#
# csv_2 = csv.dropna(subset= 'Ticker')
# csv_3 = csv_2.copy()
#
# csv_3 = csv_3.drop_duplicates(subset = [
#     'SEC_FILE_NUMBER',
#     'CONFORMED_DATE',
#     'NAME_OF_ISSUER',
#     'TITLE_OF_CLASS',
#     'CUSIP',
#     'SHARE_VALUE',
#     'SHARE_AMOUNT',
#     'CIK',
#     'NAME',
#     'Ticker'
# ])
#
# csv_3 = csv_3.groupby(by = [
#     'SEC_FILE_NUMBER',
#     'CONFORMED_DATE',
#     'NAME_OF_ISSUER',
#     'TITLE_OF_CLASS',
#     'CUSIP',
#     'CIK',
#     'NAME',
#     'Ticker'
# ]).sum()
#
#
#
# 'SHARE_VALUE' / 'SHARE_AMOUNT' == 'QTR_END_PRICe'