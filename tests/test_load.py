import csv
import pytest
from unittest.mock import patch, MagicMock, ANY
import pandas as pd

from utils.load import (
    save_to_csv,
    save_to_postgresql,
    save_to_google_sheets,
    LoadError
)

# ------------ Test save_to_csv ------------

def test_save_to_csv_success(tmp_path):
    df = pd.DataFrame({'a': [1, 2]})
    file_path = tmp_path / "out.csv"
    with patch.object(pd.DataFrame, 'to_csv') as mock_csv:
        save_to_csv(df, str(file_path))
        mock_csv.assert_called_once_with(
            str(file_path),
            index=False,
            quoting=csv.QUOTE_ALL,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d %H:%M:%S'
        )
@pytest.mark.parametrize("df, path", [
    (pd.DataFrame(), "out.csv"),
    (None, "out.csv"),
    (pd.DataFrame({'a': [1]}), None)
])
def test_save_to_csv_invalid(df, path):
    with pytest.raises(LoadError):
        save_to_csv(df, path)

# ------------ Test save_to_postgresql ------------

def test_save_to_postgresql_success():
    df = pd.DataFrame({'a': [1, 2]})
    engine = MagicMock()
    with patch('utils.load.create_engine', return_value=engine) as mock_engine:
        with patch.object(pd.DataFrame, 'to_sql', return_value=None) as mock_sql:
            save_to_postgresql(df, "tbl", "conn")
            mock_engine.assert_called_once_with("conn")
            mock_sql.assert_called_once_with(
                "tbl", con=engine, if_exists='replace', index=False
            )

@pytest.mark.parametrize("df, table, conn", [
    (pd.DataFrame(), "tbl", "conn"),
    (None, "tbl", "conn"),
    (pd.DataFrame({'a': [1]}), None, "conn"),
    (pd.DataFrame({'a': [1]}), "tbl", None)
])
def test_save_to_postgresql_invalid(df, table, conn):
    with pytest.raises(LoadError):
        save_to_postgresql(df, table, conn)

@patch('utils.load.create_engine', side_effect=Exception("db_err"))
def test_save_to_postgresql_engine_error(mock_eng):
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(LoadError):
        save_to_postgresql(df, "tbl", "conn")

@patch('utils.load.create_engine')
def test_save_to_postgresql_sql_error(mock_eng):
    df = pd.DataFrame({'a': [1]})
    eng = MagicMock()
    mock_eng.return_value = eng
    with patch.object(pd.DataFrame, 'to_sql', side_effect=Exception("sql_err")):
        with pytest.raises(LoadError):
            save_to_postgresql(df, "tbl", "conn")

# ------------ Test save_to_google_sheets ------------

def test_save_to_google_sheets_success():
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    # Mock gspread and credentials
    fake_creds = MagicMock()
    fake_client = MagicMock()
    fake_sheet = MagicMock()
    # gspread.client.open_by_key returns a workbook, and workbook.sheet1 or .worksheet()
    fake_client.open_by_key.return_value = fake_client
    fake_client.worksheet.return_value = fake_sheet
    fake_sheet.update.return_value = None

    with patch('utils.load.Credentials.from_service_account_file', return_value=fake_creds) as mock_creds:
        with patch('utils.load.gspread.authorize', return_value=fake_client):
            fake_client.open_by_key.return_value = fake_client
            fake_client.worksheet.return_value = fake_sheet
            
            save_to_google_sheets(df, 'sheet_id', 'Sheet1!A1', 'cred.json')
            mock_creds.assert_called_once_with(
                'cred.json',
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            fake_sheet.batch_clear.assert_called_once_with(['A1:ZZZ100000'])
            fake_sheet.update.assert_called_once_with(
                range_name='A1',
                values=[df.columns.tolist()] + df.astype(str).values.tolist(),
                value_input_option='USER_ENTERED'
            )

@pytest.mark.parametrize("params", [
    ({'df': pd.DataFrame(), 'sid':'sheet','rng':'A1','cred':'c'}),
    ({'df': None, 'sid':'sheet','rng':'A1','cred':'c'}),
    ({'df': pd.DataFrame({'a':[1]}), 'sid':None,'rng':'A1','cred':'c'}),
    ({'df': pd.DataFrame({'a':[1]}), 'sid':'sheet','rng':None,'cred':'c'}),
    ({'df': pd.DataFrame({'a':[1]}), 'sid':'sheet','rng':'A1','cred':None}),
])
def test_save_to_google_sheets_invalid(params):
    with pytest.raises(LoadError):
        save_to_google_sheets(
            params['df'], params['sid'], params['rng'], params['cred']
        )

@patch('utils.load.Credentials.from_service_account_file')
def test_save_to_google_sheets_cred_error(mock_creds):
    df = pd.DataFrame({'a': [1]})
    mock_creds.side_effect = Exception('cred_err')
    with pytest.raises(LoadError):
        save_to_google_sheets(df, 'sheet', 'A1', 'cred.json')
        
@patch('utils.load.gspread.authorize', side_effect=Exception('auth_err'))
def test_save_to_google_sheets_auth_error(mock_auth):
    df = pd.DataFrame({'a': [1]})
    with patch('utils.load.Credentials.from_service_account_file', return_value=MagicMock()):
        with pytest.raises(LoadError):
            save_to_google_sheets(df, 'sheet', 'A1', 'cred.json')

@patch('utils.load.gspread.authorize')
def test_save_to_google_sheets_update_error(mock_auth):
    df = pd.DataFrame({'a': [1]})
    fake_creds = MagicMock()
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_client
    fake_client.worksheet.return_value = fake_client.sheet1
    fake_client.sheet1.update.side_effect = Exception('update_err')

    mock_auth.return_value = fake_client
    with patch('utils.load.Credentials.from_service_account_file', return_value=fake_creds):
        with pytest.raises(LoadError):
            save_to_google_sheets(df, 'sheet', 'Sheet1!A1', 'cred.json')
