import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from utils.transform import transform_data, validate_transformed_data, TransformationError


def test_valid_data_transformation():
    """Test transformasi data valid"""
    test_data = pd.DataFrame({
        'Title': ['Jaket Kulit Premium', 'Celana Jeans'],
        'Price': [29.99, 15.0],
        'scrape_timestamp': [
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat()
        ]
    })
    
    result = transform_data(test_data)
    
    expected_cols = {'Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'scrape_timestamp'}
    assert set(result.columns) == expected_cols
    assert len(result) == 2
    assert result['Price'].iloc[0] == float(29.99 * 16000)
    assert pd.api.types.is_float_dtype(result['Price'])


def test_missing_columns():
    """Test handling kolom yang hilang"""
    invalid_data = pd.DataFrame({
        'Title': ['Test Product'],
        'Price': [10.0]
    })
    with pytest.raises(TransformationError) as exc_info:
        transform_data(invalid_data)
    err = exc_info.value.errors.get('missing_columns', [])
    assert set(err) == {'scrape_timestamp'}


def test_unknown_product_filter():
    """Test filter Unknown Product"""
    test_data = pd.DataFrame({
        'Title': ['Unknown Product', 'Jaket Berkualitas'],
        'Price': [15.0, 20.0],
        'scrape_timestamp': [
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat()
        ]
    })
    result = transform_data(test_data)
    assert len(result) == 1
    assert 'Jaket Berkualitas' in result['Title'].values


def test_invalid_price_handling():
    """Test handling harga invalid"""
    test_data = pd.DataFrame({
        'Title': ['Product A', 'Product B', 'Product C'],
        'Price': ['$invalid', '-10', '15.99'],
        'scrape_timestamp': [datetime.now(timezone.utc).isoformat()]*3
    })
    result = transform_data(test_data)
    assert len(result) == 1
    assert result['Price'].iloc[0] == int(15.99 * 16000)


def test_duplicate_removal():
    """Test transform_data menghapus duplikat full-row jika identik sepenuhnya"""
    timestamps_same = ["2025-05-01T00:00:00Z"]*3
    test_data_same = pd.DataFrame({
        'Title': ['Jaket Kulit']*3,
        'Price': [29.99]*3,
        'scrape_timestamp': timestamps_same
    })
    result_same = transform_data(test_data_same)
    assert len(result_same) == 1

    timestamps_diff = ["2025-05-01T00:00:00Z", "2025-05-02T00:00:00Z", "2025-05-03T00:00:00Z"]
    test_data_diff = pd.DataFrame({
        'Title': ['Jaket Kulit']*3,
        'Price': [29.99]*3,
        'scrape_timestamp': timestamps_diff
    })
    result_diff = transform_data(test_data_diff)
    assert len(result_diff) == 3


def test_optional_fields_handling():
    """Test handling field opsional"""
    test_data = pd.DataFrame({
        'Title': ['Product A', 'Product B'],
        'Price': [10.0, 20.0],
        'scrape_timestamp': [datetime.now(timezone.utc).isoformat()]*2,
        'Rating': ['4.5', np.nan],
        'Colors': [3, 'five'],
        'Size': ['Size: XL', 'Invalid Size'],
        'Gender': ['Gender: Men', 'Gender: Unisex']
    })
    result = transform_data(test_data)
    assert pd.api.types.is_float_dtype(result['Rating'])
    assert pd.api.types.is_integer_dtype(result['Colors'])
    assert any('XL' in s for s in result['Size'].dropna().astype(str))
    assert any('Unisex' in s for s in result['Gender'].dropna().astype(str))


def test_validation_failure_due_to_duplicates():
    """Test validasi data gagal karena duplikat pada data asli"""
    timestamps = ["2025-05-01T00:00:00Z", "2025-05-01T00:00:00Z"]
    original = pd.DataFrame({
        'Title': ['Kemeja Flanel', 'Kemeja Flanel'],
        'Price': [150000, 150000],
        'scrape_timestamp': timestamps
    })
    with pytest.raises(ValueError) as exc_info:
        validate_transformed_data(original)
    assert "Terdapat data duplikat" in str(exc_info.value)


def test_empty_data_validation():
    """Test handling data kosong pada validasi"""
    with pytest.raises(ValueError) as exc_info:
        validate_transformed_data(pd.DataFrame())
    assert "Data kosong" in str(exc_info.value)
