"""
Author:     Alan Danque
Date:       20210128
Class:      DSC 650
Exercise:   10.1.b
Purpose:    Implement an `ngram` function that splits tokens into N-grams.
"""
import string
import nltk

def ngram(paragraph, n):
    # Split the sentence by spaces
    words = paragraph.split()
    # Remove punctuation
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in words]
    bi_grams = nltk.ngrams(stripped, n)
    return bi_grams

paragraph = "This is my sentence, to parse. Get all punctuation out# of here!"
bi_grams = ngram(paragraph, 3)
for gram in bi_grams:
    print(gram)
