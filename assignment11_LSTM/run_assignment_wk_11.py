"""
Author:     Alan Danque
Date:       20210203
Class:      DSC 650
Exercise:   11
Purpose:    Implement an LSTM text generator using text of our choice.
"""
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()
import keras
import numpy as np
from pathlib import Path
import time
start_time = time.time()
results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment11/')
results_dir.mkdir(parents=True, exist_ok=True)

# Needed the following as caused CUDA DNN errors
physical_devices = tf.config.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(physical_devices[0], True)
#alice_in_wonderland.txt //  https://gist.githubusercontent.com/phillipj/4944029/raw/75ba2243dd5ec2875f629bf5d79f6c1e4b5a8b46/alice_in_wonderland.txt
#path = keras.utils.get_file('nietzsche.txt', origin='https://s3.amazonaws.com/text-datasets/nietzsche.txt')
path = keras.utils.get_file('alice_in_wonderland.txt', origin='https://gist.githubusercontent.com/phillipj/4944029/raw/75ba2243dd5ec2875f629bf5d79f6c1e4b5a8b46/alice_in_wonderland.txt')
text = open(path).read().lower()
print('Corpus length:', len(text))

maxlen = 60
step = 3
sentences = []
next_chars = []

for i in range(0, len(text) - maxlen, step):
    sentences.append(text[i: i + maxlen])
    next_chars.append(text[i + maxlen])

print('Number of sequences:', len(sentences))

chars = sorted(list(set(text)))

print('Unique characters:', len(chars))

char_indices = dict((char, chars.index(char)) for char in chars)
print('Vectorization....')

x = np.zeros((len(sentences), maxlen, len(chars)), dtype=np.bool)
y = np.zeros((len(sentences), len(chars)), dtype=np.bool)

for i, sentence in enumerate(sentences):
    for t, char in enumerate(sentence):
        x[i, t, char_indices[char]] = 1
    y[i, char_indices[next_chars[i]]] = 1

# Build the model
from keras import layers
model = keras.models.Sequential()
model.add(layers.LSTM(128, input_shape=(maxlen, len(chars))))
model.add(layers.Dense(len(chars), activation='softmax'))


optimizer = keras.optimizers.RMSprop(lr=0.01)
model.compile(loss='categorical_crossentropy', optimizer=optimizer)

result_model_file = results_dir.joinpath('LSTM_Wk11.h5')
model.save(result_model_file)

def sample(preds, temperature=1.0):
    preds = np.asarray(preds).astype('float64')
    preds = np.log(preds) / temperature
    exp_preds = np.exp(preds)
    preds = exp_preds / np.sum(exp_preds)
    probas = np.random.multinomial(1, preds, 1)
    return np.argmax(probas)

import random
import sys
epochiteration = 1
inum = 1
inumlast = 21
for epoch in range(1, 60):
    print('epoch', epoch)
    model.fit(x, y, batch_size=128, epochs=1)
    start_index = random.randint(0, len(text) - maxlen -1)
    generated_text = text[start_index: start_index + maxlen]
    print('--- Generating with seed: "' + generated_text + '"')

    epochiteration = 0

    for temperature in [0.2, 0.5, 1.0, 1.2]:
        print('----- temperature:', temperature)
        print("start text")
        sys.stdout.write(generated_text)
        print("end text")
        epochiteration += 1
        print("**************Loop write start*************")
        for i in range(400):
            sampled = np.zeros((1, maxlen, len(chars)))
            for t, char in enumerate(generated_text):
                sampled[0, t, char_indices[char]] = 1.
            preds = model.predict(sampled, verbose = 0)[0]
            next_index = sample(preds, temperature)
            next_char = chars[next_index]
            generated_text += next_char
            generated_text = generated_text[1:]
            sys.stdout.write(next_char)
        print("**************Loop write end**************")

        if inum < inumlast and epochiteration == 2:
            print("writing the following to file")
            print(result_model_file)
            print(generated_text)
            gfilename = 'LSTM_Text_Generated_Temperature_'+ str(temperature) +'_EPOCH_'+str(epoch)+'_Sample_' + str(inum) + '.txt'
            result_model_file = results_dir.joinpath(gfilename)
            f = open(result_model_file, "w")
            f.write(generated_text)
            f.close()
            inum+= 1



print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))


