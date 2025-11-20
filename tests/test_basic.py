"""Basic tests"""
import numpy as np


def test_numpy_available():
    """Test numpy is available"""
    assert np.__version__


def test_matrix_multiplication(sample_matrices):
    """Test basic matrix multiplication"""
    A, B, expected = sample_matrices
    result = np.matmul(A, B)
    np.testing.assert_array_equal(result, expected)
