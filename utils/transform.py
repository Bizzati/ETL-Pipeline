import pandas as pd
from typing import Dict, Any

class TransformationError(Exception):
    def __init__(self, message: str, errors: Dict[str, Any] = None):
        super().__init__(message)
        self.errors = errors or {}


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ['Title', 'Price', 'scrape_timestamp']

    # 1. Periksa kolom wajib sebelum pemrosesan
    if df.empty or not all(col in df.columns for col in required_cols):
        missing = list(set(required_cols) - set(df.columns)) if not df.empty else required_cols.copy()
        raise TransformationError(
            message="Kolom input tidak lengkap",
            errors={
                'missing_columns': missing,
                'input_sample': df.head(1).to_dict() if not df.empty else None
            }
        )

    df_tf = df.copy(deep=True)

    if 'Rating' not in df_tf.columns:
        df_tf['Rating'] = pd.NA

    # 2. Hapus duplikat persis (full row) seperti semula
    df_tf = df_tf.drop_duplicates()

    # 3. Filter invalid: buang yang judulnya tepat 'Unknown Product'
    df_tf = df_tf[df_tf['Title'].str.strip().str.lower() != 'unknown product']

    # 4. Pastikan Title tidak kosong
    df_tf = df_tf.dropna(subset=['Title'])
    df_tf = df_tf[df_tf['Title'].str.strip() != '']

    # 5. Konversi Price ke numeric dan > 0
    df_tf['Price'] = pd.to_numeric(df_tf['Price'], errors='coerce')
    df_tf = df_tf[df_tf['Price'].notna() & (df_tf['Price'] > 0)]

    # 6. Konversi harga ke IDR
    df_tf['Price'] = (
        df_tf['Price']
        .astype(str)
        .str.replace(r'[^\d.]', '', regex=True)
        .replace('', pd.NA)
        .astype(float)
        .mul(16000)
    )

    # 7. Filter rating
    df_tf['Rating'] = (
        df_tf['Rating']
        .astype(str)
        .str.extract(r'(\d+\.?\d*)')[0]
    )
    df_tf['Rating'] = pd.to_numeric(
            df_tf['Rating'],
            errors='coerce'
        ).astype('float64') 

    # 8. Parsing kolom opsional (toleransi null)
    optional_cols = ['Rating', 'Colors', 'Size', 'Gender']
    for col in optional_cols:
        if col in df_tf.columns:
            if col == 'Rating':
                df_tf[col] = pd.to_numeric(df_tf[col], errors='coerce')
            elif col == 'Colors':
                df_tf[col] = pd.to_numeric(df_tf[col], errors='coerce').astype('Int64')
            elif col == 'Size':
                df_tf[col] = df_tf[col].str.replace(r'(?i)^Size:\s*', '', regex=True)
            elif col == 'Gender':
                df_tf[col] = df_tf[col].str.replace(r'(?i)^Gender:\s*', '', regex=True)
        else:
            df_tf[col] = pd.NA

    df_tf.info()

    return df_tf


def validate_transformed_data(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        raise ValueError("Data kosong")

    df_val = df.copy()
    if 'Price' in df_val.columns:
        df_val['Price'] = pd.to_numeric(df_val['Price'], errors='coerce')

    # Hitung validasi
    duplicates = df_val.duplicated().sum()
    null_values = df_val[['Title', 'Price']].isnull().sum().to_dict()
    invalid_titles = df_val['Title'].str.strip().str.lower().eq('unknown product').sum()
    price_min = df_val['Price'].min() if not df_val.empty else None
    price_max = df_val['Price'].max() if not df_val.empty else None

    validation = {
        'total_rows': len(df_val),
        'duplicates': duplicates,
        'null_values': null_values,
        'invalid_titles': invalid_titles,
        'price_range': (price_min, price_max)
    }

    if duplicates > 0:
        raise ValueError("Terdapat data duplikat")
    if null_values.get('Title', 0) > 0 or null_values.get('Price', 0) > 0:
        raise ValueError("Kolom Title/Price mengandung null")
    if invalid_titles > 0:
        raise ValueError("Terdapat judul produk invalid")

    return validation
