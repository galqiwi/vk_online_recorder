import unittest
import numpy as np
from storage.bits2bytes import bits2bytes, bytes2bits


class Bits2BytesTests(unittest.TestCase):
    @staticmethod
    def bits2bytes2bits(bits):
        bytes_output = bits2bytes(bits)
        bits_output = bytes2bits(bytes_output)
        np.testing.assert_array_equal(bits, bits_output)

    def test_empty_array(self):
        self.bits2bytes2bits(np.array([], dtype=bool))

    def test_one_bit(self):
        self.bits2bytes2bits(np.array([True], dtype=bool))

    def test_type(self):
        bytes_output = bits2bytes(np.array([True], dtype=bool))
        self.assertIsInstance(bytes_output, np.ndarray)
        self.assertEqual(bytes_output.dtype, np.ubyte)
        bits_output = bytes2bits(bytes_output)
        self.assertEqual(bits_output.dtype, bool)

    def test_compression(self):
        input_size = 100
        bytes_output = bits2bytes(np.array(
            np.full(fill_value=False, shape=(input_size,)), dtype=bool))
        self.assertLess(len(bytes_output), input_size // 2)

    def test_different_bits(self):
        input_pattern = np.array([False, True])
        input_bits = np.repeat(input_pattern[None, :], repeats=10, axis=0)
        input_bits = input_bits.reshape(-1)
        self.bits2bytes2bits(input_bits)

    def test_zero_trim(self):
        input_bits = np.full(shape=(8,), fill_value=True, dtype=bool)
        self.bits2bytes2bits(input_bits)


if __name__ == '__main__':
    unittest.main()
