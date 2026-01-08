import pandas as pd
from MongoDB import mdb

def build_cusip_mapping(mdb):
    return {
        (
            str(int(d['BBG']['ID_CUSIP']))
            if isinstance(d['BBG']['ID_CUSIP'], (int, float))
            else str(d['BBG']['ID_CUSIP'])
        ): str(d['ticker']['bbg'])
        for d in mdb.gestao.asset.metadata.find(
            {'BBG.ID_CUSIP': {'$exists': True}, 'ticker.bbg': {'$exists': True}},
            {'BBG.ID_CUSIP': 1, 'ticker.bbg': 1}
        )
    }


if __name__ == '__main__':
    csv = pd.read_csv(r"C:\Users\ClaudioYano\CC\13F\data3\13f_holdings.csv")

    cusip_map = build_cusip_mapping(mdb)
    csv['Ticker'] = csv['CUSIP'].astype(str).map(cusip_map)


    csv_2 = csv.dropna(subset= 'Ticker')
    csv_3 = csv_2.copy()

    csv_3 = csv_3.drop_duplicates(subset = [
        'SEC_FILE_NUMBER',
        'CONFORMED_DATE',
        'NAME_OF_ISSUER',
        'TITLE_OF_CLASS',
        'CUSIP',
        'SHARE_VALUE',
        'SHARE_AMOUNT',
        'CIK',
        'NAME',
        'Ticker'
    ])

    csv_3 = csv_3.groupby(by = [
        'SEC_FILE_NUMBER',
        'CONFORMED_DATE',
        'NAME_OF_ISSUER',
        'TITLE_OF_CLASS',
        'CUSIP',
        'CIK',
        'NAME',
        'Ticker'
    ]).sum()



