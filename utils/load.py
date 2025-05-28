import logging
import pandas as pd
from sqlalchemy import create_engine
import gspread
from google.oauth2.service_account import Credentials
from typing import Optional
import csv

class LoadError(Exception):
    """Custom exception for load errors in ETL pipeline."""
    pass


def save_to_csv(df: pd.DataFrame, output_path: str):
    if not output_path or not isinstance(output_path, str):
        raise LoadError("Output path tidak valid untuk save_to_csv")
    
    try:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input harus berupa pandas DataFrame")
        if df.empty:
            raise ValueError("DataFrame kosong")
            
        # Clean special characters and format columns
        df = df.applymap(lambda x: x.replace('"', "'") if isinstance(x, str) else x)
        
        df.to_csv(
            output_path,
            index=False,
            quoting=csv.QUOTE_ALL,  # Handle special characters
            encoding='utf-8-sig',   # Excel compatibility
            date_format='%Y-%m-%d %H:%M:%S'
        )
        logging.info("CSV berhasil disimpan ke: %s", output_path)
        
    except Exception as e:
        logging.error("❌ Gagal menyimpan CSV: %s", e)
        raise LoadError(f"CSV Error: {str(e)}") from e

def save_to_postgresql(df: pd.DataFrame, table_name: str, connection_string: str):
    if not isinstance(df, pd.DataFrame):
        logging.error("Parameter df bukan DataFrame")
        raise LoadError("DataFrame tidak valid untuk save_to_postgresql")
    if df.empty:
        logging.error("DataFrame kosong, tidak ada data untuk disimpan")
        raise LoadError("DataFrame kosong dalam save_to_postgresql")
    if not table_name:
        logging.error("table_name tidak diberikan atau kosong")
        raise LoadError("Nama tabel tidak valid untuk save_to_postgresql")
    if not connection_string:
        logging.error("connection_string tidak diberikan atau kosong")
        raise LoadError("Connection string tidak valid untuk save_to_postgresql")
    try:
        engine = create_engine(connection_string)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info("DataFrame berhasil disimpan ke PostgreSQL di tabel: %s", table_name)
    except Exception as e:
        logging.error("❌ Gagal menyimpan DataFrame ke PostgreSQL: %s", e)
        raise LoadError(f"Gagal menyimpan ke PostgreSQL: {e}") from e

def save_to_google_sheets(
    df: pd.DataFrame,
    spreadsheet_id: str,
    range_name: str = "Sheet1!A1",
    credentials_path: Optional[str] = None
) -> None:
    """Save DataFrame to Google Sheets"""
    try:
        # Validate input
        if not credentials_path:
            raise ValueError("Path credentials harus diisi")
            
        if df.empty:
            raise ValueError("DataFrame tidak boleh kosong")

        # Authenticate with modern library
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(
            credentials_path, 
            scopes=scopes
        )
        
        gc = gspread.authorize(creds)

        # Open spreadsheet
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
        except gspread.SpreadsheetNotFound:
            raise ValueError(f"Spreadsheet ID tidak valid: {spreadsheet_id}")

        # Parse range name
        if '!' in range_name:
            sheet_name, cell_range = range_name.split('!')
            worksheet = spreadsheet.worksheet(sheet_name)
        else:
            worksheet = spreadsheet.sheet1
            cell_range = range_name

        # Clear existing data
        try:
            worksheet.batch_clear([f"{cell_range}:ZZZ100000"])
        except gspread.exceptions.APIError as e:
            logging.warning("Peringatan saat membersihkan sheet: %s", e)

        # Prepare data
        data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()

        # Update sheet
        worksheet.update(
            range_name=cell_range,
            values=data,
            value_input_option='USER_ENTERED'
        )
        
        logging.info("Data berhasil diupload ke Google Sheets: %s", spreadsheet_id)
        logging.info("URL: https://docs.google.com/spreadsheets/d/%s", spreadsheet_id)

    except Exception as e:
        logging.error("❌ Gagal menyimpan ke Google Sheets: %s", e)
        raise LoadError(f"Google Sheets Error: {str(e)}") from e
