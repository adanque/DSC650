"""
Author:     Alan Danque
Date:       20201224
Class:      DSC 650
Exercise:   5.2
"""
# http://localhost:8082/static/visualiser/index.html
# python ./run_assignment_5.2.py --scheduler-host localhost reutersrun

import luigi.contrib.spark

class reuterspredict(luigi.Task):

    def requires(self):
        return []

    def output(self):
        # to get around the luigi unfulfilled dependency warning
        return luigi.LocalTarget('prediction2.txt')

    def run(self):
        # Loading the Reuters Dataset
        print("Loading the Reuters Dataset")
        from keras.datasets import reuters
        (train_data, train_labels), (test_data, test_labels) = reuters.load_data(num_words=10000)

        print(len(train_data))
        print(len(test_data))
        print(train_data[10])

        # Decoding newswires back to text
        print("Decoding newswires back to text")
        word_index=reuters.get_word_index()
        reverse_word_index = dict([(value, key) for (key, value) in word_index.items()])
        decoded_newswire = ' '.join([reverse_word_index.get(i-3, '?') for i in train_data[0]])
        print(train_labels[10])

        # 3.5.2
        # Encoding the data
        print("Encoding the data")
        import numpy as np
        def vectorize_sequences(sequences, dimension=10000):
            results = np.zeros((len(sequences), dimension))
            for i, sequence in enumerate(sequences):
                results[i, sequence]=1.
            return results

        x_train = vectorize_sequences(train_data)
        x_test = vectorize_sequences(test_data)

        def to_one_hot(labels, dimension=46):
            results = np.zeros((len(labels),dimension))
            for i, label in enumerate(labels):
                results[i, label]=1.
            return results

        one_hot_train_labels = to_one_hot(train_labels)
        one_hot_test_labels = to_one_hot(test_labels)

        from keras.utils.np_utils import to_categorical
        one_hot_train_labels = to_categorical(train_labels)
        one_hot_test_labels = to_categorical(test_labels)

        # Model definition
        print("Model definition")

        from keras import models
        from keras import layers
        model = models.Sequential()
        model.add(layers.Dense(64, activation='relu', input_shape=(10000,)))
        model.add(layers.Dense(64, activation='relu'))
        model.add(layers.Dense(46, activation='softmax'))

        # Compiling the model
        print("Compiling the model")
        model.compile(optimizer='rmsprop',
                      loss='categorical_crossentropy',
                      metrics='accuracy')

        # 3.5.4 Validating your approach
        # Setting aside a validation set
        print("Setting aside a validation set")
        x_val= x_train[:1000]
        partial_x_train = x_train[1000:]
        y_val = one_hot_train_labels[:1000]
        partial_y_train = one_hot_train_labels[1000:]

        # Training the model
        print("Training the model")
        history = model.fit(partial_x_train,
                            partial_y_train,
                            epochs=20,
                            batch_size=512,
                            validation_data=(x_val, y_val))

        # Plotting the training and validation loss
        print("Plotting the training and validation loss")
        import matplotlib.pyplot as plt
        loss = history.history['loss']
        val_loss = history.history['val_loss']
        epochs = range(1, len(loss) + 1)
        plt.plot(epochs, loss, 'bo', label='Training loss')
        plt.plot(epochs, val_loss, 'b', label='Validation loss')
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.show()

        # Plotting the training and validation accuracy
        print("Plotting the training and validation accuracy")

        plt.clf()
        # there is a typo in the book here
        for key in history.history.keys():
            print(key)
        acc = history.history['accuracy']
        val_acc = history.history['val_accuracy']
        plt.plot(epochs, acc, 'bo', label='Training acc')
        plt.plot(epochs, val_acc, 'b', label='Validation acc')
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.show()

        # Retraining a model from scratch
        print("Retraining a model from scratch")

        model = models.Sequential()
        model.add(layers.Dense(64, activation='relu', input_shape=(10000,)))
        model.add(layers.Dense(64, activation='relu'))
        model.add(layers.Dense(46, activation='softmax'))

        model.compile(optimizer='rmsprop',
                      loss='categorical_crossentropy',
                      metrics=['accuracy'])
        model.fit(partial_x_train,
                  partial_y_train,
                  epochs=9,
                  batch_size=152,
                  validation_data=(x_val,y_val))
        results=model.evaluate(x_test, one_hot_test_labels)

        print(results)


        import copy
        test_labels_copy = copy.copy(test_labels)
        np.random.shuffle(test_labels_copy)
        hits_array = np.array(test_labels) ==np.array(test_labels_copy)
        print(float(np.sum(hits_array))/len(test_labels))

        # Generating predictions on new data
        print("Generating predictions on new data")

        predictions = model.predict(x_test)
        print(predictions.shape)
        print(np.sum(predictions[0]))
        print(np.argmax(predictions[0]))

        # 3.5.6
        # A different way to handle the labels and the loss
        print("A different way to handle the labels and the loss")

        y_train = np.array(train_labels)
        y_test = np.array(test_labels)
        model.compile(optimizer='rmsprop',
                      loss='sparse_categorical_crossentropy',
                      metrics=['acc'])

        # A model with an information bottleneck
        print("A model with an information bottleneck")
        model = models.Sequential()
        model.add(layers.Dense(64, activation='relu', input_shape=(10000,)))
        model.add(layers.Dense(4, activation='relu'))
        model.add(layers.Dense(46, activation='softmax'))
        model.compile(optimizer = 'rmsprop',
                      loss='categorical_crossentropy',
                      metrics=['accuracy'])
        model.fit(partial_x_train,
                  partial_y_train,
                  epochs=20,
                  batch_size=128,
                  validation_data=(x_val, y_val))

        #model.fit(x_train, y_train, epochs=4, batch_size=512)
        #results = model.evaluate(x_test, y_test)
        #print(results)

        #print(x_test)
        pred = model.predict(x_test)
        print(pred)

        with self.output().open('w') as f:
            for word in pred:
                f.write('{word}\n'.format(word=word))


class reutersrun(luigi.Task):

    def requires(self):
        return reuterspredict()

    def output(self):
        # to get around the luigi unfulfilled dependency warning
        return luigi.LocalTarget('Complete2.txt')

    def run(self):
        words = []

        with self.output().open('w') as f:
            for word in words:
                f.write('{word}\n'.format(word=word))
        print("Complete")

if __name__ == "__main__":
    luigi.run()

