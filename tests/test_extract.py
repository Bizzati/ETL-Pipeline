import pytest
import pandas as pd
from unittest.mock import patch, Mock
from requests.exceptions import HTTPError, Timeout
import logging
from bs4 import BeautifulSoup
from utils.extract import extract_data
import re


# Test case tambahan

def test_extract_handles_missing_fields():
    """Test parsing produk dengan field yang hilang"""
    partial_html = """
    <div class="collection-card">
        <h3 class="product-title">Partial Product</h3>
        <span class="price">$10.00</span>
    </div>
    """
    
    with patch('utils.extract.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.text = partial_html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        df = extract_data(max_pages=1)
        
        assert len(df) == 1
        assert pd.isna(df.iloc[0]['Rating'])
        assert pd.isna(df.iloc[0]['Colors'])
        assert pd.isna(df.iloc[0]['Size'])
        assert pd.isna(df.iloc[0]['Gender'])

def test_extract_handles_pagination_break():
    """Test berhenti saat menemukan halaman kosong"""
    html_with_pagination = {
        1: "<div class='collection-card'>...</div>",
        2: "<html><body>No products</body></html>"
    }
    
    def side_effect(url, *args, **kwargs):
        m = re.search(r"(\d+)$", url)
        page = int(m.group(1)) if m else 1

        mock = Mock()
        mock.text = html_with_pagination.get(page, "")
        mock.raise_for_status.return_value = None
        return mock
    
    with patch('utils.extract.requests.get') as mock_get:
        mock_get.side_effect = side_effect
        df = extract_data(max_pages=3)
        
        assert mock_get.call_count == 3

def test_extract_handles_timeout():
    """Test penanganan error timeout"""
    with patch('utils.extract.requests.get') as mock_get:
        mock_get.side_effect = Timeout("Request timeout")
        
        df = extract_data(max_pages=1)
        assert df.empty

def test_extract_logs_critical_errors(caplog):
    """Test logging untuk error kritis"""
    with patch('utils.extract.requests.get') as mock_get:
        mock_get.side_effect = Exception("Critical error")
        
        df = extract_data()
        assert "Error kritis" in caplog.text
        assert df.empty

def test_extract_parses_all_fields_correctly():
    """Test parsing semua field dengan benar"""
    test_html = """
    <div class="collection-card">
        <h3 class="product-title">Test Jacket</h3>
        <span class="price">$99.99</span>
        <div class="product-details">
            <p>Rating: 4.8 / 5</p>
            <p>5 Colors</p>
            <p>Size: XL</p>
            <p>Gender: Men</p>
        </div>
    </div>
    """
    
    with patch('utils.extract.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.text = test_html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        df = extract_data(max_pages=1)
        
        assert df.iloc[0]['Title'] == "Test Jacket"
        assert df.iloc[0]['Price'] == '99.99'
        assert df.iloc[0]['Rating'] == 'Rating: 4.8 / 5'
        assert df.iloc[0]['Colors'] == 5
        assert df.iloc[0]['Size'] == "XL"
        assert df.iloc[0]['Gender'] == "Men"

def test_extract_handles_multiple_pages():
    """Test ekstraksi multi halaman dengan data valid"""
    html_pages = {
        1: """
        <html>
          <body>
            <div class="collection-card">
              <h3 class="product-title">Product 1</h3>
              <span class="price">$10.00</span>
            </div>
          </body>
        </html>
        """,
        2: """
        <html>
          <body>
            <div class="collection-card">
              <h3 class="product-title">Product 2</h3>
              <span class="price">$20.00</span>
            </div>
          </body>
        </html>
        """
    }

    def url_to_page(url):
        # Ekstrak parameter page dari URL
        m = re.search(r"/page(\d+)$", url)
        return int(m.group(1)) if m else 1
    
    with patch('utils.extract.requests.get') as mock_get:
        mock_get.side_effect = lambda url, *args, **kwargs: Mock(
            text=html_pages[url_to_page(url)],
            raise_for_status=Mock(),
            status_code=200
        )
        
        df = extract_data(max_pages=2)
        
        # Verifikasi hasil
        assert len(df) == 2, f"Actual data: {df.to_dict()}"
        assert set(df['Title']) == {"Product 1", "Product 2"}
        assert list(df['Price']) == ['10.00', '20.00']