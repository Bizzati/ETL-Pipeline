
## Konfigurasi Berkas

1. Persiapan postgresql:

- Buat database PostgreSQL dengan nama 'fashion_db' atau sesuaikan
- Pada file main.py, isi "PG_PASSWORD" dengan password anda. Contoh blok kode;

```python
PG_PASSWORD = "<your_password>"
PG_CONN = f"postgresql://postgres:{PG_PASSWORD}@localhost:5432/fashion_db"
PG_TABLE = "products"

```

2. Persiapan googlesheet:

Berkas api.json sudah ada pada folder submission, sehingga cukup dijalani saja.

## Instalasi requirements

Jalankan perintah di bawah ini pada terminal:

```python
pip install -r requirements.txt
```

## Cara Menjalankan ETL Pipeline

1. Menjalankan ETL pipeline:

```python
python main.py
```

2. Menjalankan unit test satu per-satu:

```python
python -m pytest tests/
```

3. Menjalankan Test Coverage:

```python
python -m pytest --cov=utils tests/
```

## Link Google Sheet:
https://docs.google.com/spreadsheets/d/1jq8ltXsjPSibt2uVLrdGxgjqQ5dextWFQoXIFUYnbOw/edit?gid=0#gid=0
