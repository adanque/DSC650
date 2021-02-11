"""
Author:     Alan Danque
Date:       20210128
Class:      DSC 650
Exercise:   10.1.c
Purpose:    Implement an one_hot_encode function to create a vector from a numerical vector from a list of tokens.
"""
import string
import nltk
from numpy import array
from numpy import argmax
from keras.utils import to_categorical


def onehtencode(data):
    data = array(data)
    print("Received array")
    print(data)
    # one hot encode
    encoded = to_categorical(data)
    return encoded

data = [1, 3, 2, 0, 3, 2, 2, 1, 0, 1]
encodedval = onehtencode(data)
print("One Hot Encoded values")
print(encodedval)
