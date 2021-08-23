import numpy as np


def bits2bytes(bits_input):
    output_prefix = np.packbits(bits_input)
    return np.concatenate((
        output_prefix,
        np.array([len(bits_input) % 8], dtype=np.ubyte)
    ))


def bytes2bits(bytes_input):
    tail_length = bytes_input[-1]
    assert tail_length < 8
    trim = (8 - tail_length) % 8
    return np.array(np.unpackbits(bytes_input[:-1])[:-trim or None], dtype=bool)
