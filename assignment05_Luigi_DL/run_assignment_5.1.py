"""
Author:     Alan Danque
Date:       20201224
Class:      DSC 650
Exercise:   5.1
"""
# http://localhost:8082/static/visualiser/index.html
# python ./run_assignment_5.1.py --scheduler-host localhost imdbrun


import luigi.contrib.spark

class imdbpredict(luigi.Task):

    def requires(self):
        return []

    def output(self):
        # to get around the luigi unfulfilled dependency warning
        return luigi.LocalTarget('prediction1.txt')

    def run(self):
        # 3.4.1 Loading the imdb data set
        from keras.datasets import imdb
        (train_data, train_labels), (test_data, test_labels) = imdb.load_data (num_words=10000)
        print(train_data[0])
        print(train_labels[0])

        word_index = imdb.get_word_index()
        reverse_word_index = dict([(value, key) for (key, value) in word_index.items()])
        decoded_review = ' '.join([reverse_word_index.get(i-3, '?') for i in train_data[0]])

        # 3.4.2 Preparing the data
        import numpy as np
        def vectorize_sequences(sequences, dimension=10000):
            results = np.zeros((len(sequences), dimension))
            for i, sequence in enumerate(sequences):
                results[i, sequence] = 1.
            return results

        x_train = vectorize_sequences(train_data)
        x_test = vectorize_sequences(test_data)

        y_train = np.asarray(train_labels).astype('float32')
        y_test = np.asarray(test_labels).astype('float32')

        print(x_train[0])

        # 3.4.3 Building your network

        from keras import models
        from keras import layers

        # Model definition
        print("Model definition")
        model = models.Sequential()
        model.add(layers.Dense(16, activation='relu', input_shape=(10000,)))
        model.add(layers.Dense(16, activation='relu'))
        model.add(layers.Dense(1, activation='sigmoid'))

        # Compiling the model
        print("Compiling the model")
        model.compile(optimizer='rmsprop',
                      loss='binary_crossentropy',
                      metrics=['accuracy'])

        # Configuring the optimizer
        print("Configuring the optimizer")
        from keras import optimizers
        model.compile(optimizer=optimizers.RMSprop(lr=0.001),
                      loss='binary_crossentropy',
                      metrics=['accuracy'])

        # Using custom losses and metrics
        print("Using custom losses and metrics")
        from keras import losses
        from keras import metrics
        model.compile(optimizer=optimizers.RMSprop(lr=0.001),
                      loss=losses.binary_crossentropy,
                      metrics=[metrics.binary_accuracy])

        # 3.4.4 Validating your approach
        # Setting aside a validation set
        print("Setting aside a validation set")
        x_val = x_train[:10000]
        partial_x_train = x_train[10000:]
        y_val = y_train[:10000]
        partial_y_train = y_train[10000:]

        # Training your model
        print("Training your model")
        model.compile(optimizer='rmsprop',
                      loss='binary_crossentropy',
                      metrics=['acc'])

        history=model.fit(partial_x_train, partial_y_train, epochs=20, batch_size=512, validation_data=(x_val, y_val))

        history_dict = history.history
        print(history_dict.keys())

        # Plotting the training and validation loss
        print("Plotting the training and validation loss")
        import matplotlib.pyplot as plt
        history_dict = history.history
        loss_values = history_dict['loss']
        val_loss_values = history_dict['val_loss']
        acc = history_dict['acc']
        epochs = range(1, len(acc) + 1)
        plt.plot(epochs, loss_values, 'bo', label='Training loss')
        plt.plot(epochs, val_loss_values, 'b', label='Validation loss')
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.show()

        # Plotting the training and validation accuracy
        print("Plotting the training and validation accuracy")
        plt.clf()
        acc_values = history_dict['acc']
        val_acc = history_dict['val_acc']
        plt.plot(epochs, acc, 'bo', label = 'Training acc')
        plt.plot(epochs, val_acc, 'b', label= 'Validation acc')
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.show()

        # Retraining a model from scratch
        print("Retraining a model from scratch")
        model = models.Sequential()
        model.add(layers.Dense(16, activation='relu', input_shape=(10000,)))
        model.add(layers.Dense(16, activation='relu'))
        model.add(layers.Dense(1, activation='sigmoid',))
        model.compile(optimizer='rmsprop',
                      loss='binary_crossentropy',
                      metrics=['accuracy'])
        model.fit(x_train, y_train, epochs=4, batch_size=512)
        results = model.evaluate(x_test, y_test)
        print(results)

        print(x_test)
        pred = model.predict(x_test)
        print(pred)

        with self.output().open('w') as f:
            for word in pred:
                f.write('{word}\n'.format(word=word))



class imdbrun(luigi.Task):

    def requires(self):
        return imdbpredict()

    def output(self):
        # to get around the luigi unfulfilled dependency warning
        return luigi.LocalTarget('Complete1.txt')

    def run(self):
        words = []
        with self.output().open('w') as f:
            for word in words:
                f.write('{word}\n'.format(word=word))
        print("Complete")

if __name__ == "__main__":
    luigi.run()