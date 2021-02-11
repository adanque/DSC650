"""
Author:     Alan Danque
Date:       20210128
Class:      DSC 650
Exercise:   10.1.a
Purpose:    Create a tokenize function that splits a sentence into words. Ensure that your tokenizer removes basic punctuation.
"""
import string

def tokenize(sentence):
    # Split the sentence by spaces
    words = sentence.split()
    # Remove punctuation
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in words]
    return stripped

sentence = "This is my sentence, to parse. Get all punctuation out# of here!"
tokens = tokenize(sentence)
print(type(tokens))
print(tokens)


