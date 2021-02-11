"""
Author:     Alan Danque
Date:       20201224
Class:      DSC 650
Exercise:   5.3
"""
# http://localhost:8082/static/visualiser/index.html
# python ./run_assignment_5.3.py --scheduler-host localhost bostonhousingrun


import luigi.contrib.spark

class bostonpredict(luigi.Task):

    def requires(self):
        return []

    def output(self):
        # to get around the luigi unfulfilled dependency warning
        return luigi.LocalTarget('prediction3.txt')

    def run(self):
        # Loading the Boston housing dataset
        print("Loading the Boston housing dataset")
        from keras.datasets import boston_housing
        (train_data, train_targets), (test_data, test_targets) = boston_housing.load_data()
        print(train_data.shape)
        print(test_data.shape)

        print(train_targets)

        # Preparing the data
        # Normalizing the data
        print("Normalizing the data")

        mean = train_data.mean(axis = 0)
        train_data -= mean
        std = train_data.std(axis=0)
        train_data /= std
        test_data -= mean
        test_data /= std

        # 3.6 Building your network
        # Model definition
        print("Model definition")

        from keras import models
        from keras import layers

        def build_model():
            model = models.Sequential()
            model.add(layers.Dense(64, activation='relu', input_shape=(train_data.shape[1],)))
            model.add(layers.Dense(64, activation='relu'))
            model.add(layers.Dense(1))
            model.compile(optimizer='rmsprop', loss='mse', metrics=['mae'])
            return model

        # 3.6.4 Validating you approach using K-fold validation
        # K-fold validation
        print("K-Fold Validation")
        import numpy as np
        k = 4
        num_val_samples = len(train_data) // k
        num_epochs = 100
        all_scores = []

        for i in range(k):
            print('processing fold#', i)
            val_data = train_data[i * num_val_samples: (i+1) * num_val_samples]
            val_targets = train_targets[i * num_val_samples: (i+1) * num_val_samples]
            partial_train_data = np.concatenate(
                [train_data[:i * num_val_samples],
                 train_data[(i+1)* num_val_samples:]],
                axis =0)
            partial_train_targets = np.concatenate(
                [train_targets[:i * num_val_samples],
                 train_targets[(i + 1) * num_val_samples:]],
                axis=0)
            model = build_model()
            model.fit(partial_train_data, partial_train_targets,
                      epochs=num_epochs, batch_size=1, verbose=0)
            val_mse, val_mae = model.evaluate(val_data, val_targets, verbose=0)
            all_scores.append(val_mae)

        print(all_scores)

        # Saving the validation logs at each fold
        print("Saving the validation logs at each fold")

        num_epochs = 500
        all_mae_histories = []
        for i in range(k):
            print('processing fold #', i)
            val_data = train_data[i * num_val_samples: (i + 1) * num_val_samples]
            val_targets = train_targets[i*num_val_samples: (i + 1) * num_val_samples]
            partial_train_data = np.concatenate(
                [train_data[:i * num_val_samples],
                 train_data[(i+1) * num_val_samples:]],
                axis=0)
            partial_train_targets=np.concatenate(
                [train_targets[:i * num_val_samples],
                 train_targets[(i+1) * num_val_samples:]],
                axis=0)
            model=build_model()
            history=model.fit(partial_train_data, partial_train_targets,
                              validation_data=(val_data, val_targets),
                              epochs=num_epochs, batch_size=1, verbose=0)
            # there is a typo in the book here
            #for key in history.history.keys():
            #    print(key)
            mae_history = history.history['val_mae'] # typo val_mean_absolute_error']
            all_mae_histories.append(mae_history)

        # Building the history of successive mean k-fold validation scores
        print("Building the history of successive mean k-fold validation scores")
        average_mae_history = [
            np.mean([x[i] for x in all_mae_histories]) for i in range(num_epochs)]

        # Plotting validation scores
        print("Plotting validation scores")

        import matplotlib.pyplot as plt
        plt.plot(range(1, len(average_mae_history)+ 1), average_mae_history)
        plt.xlabel('Epochs')
        plt.ylabel('Validation MAE')
        plt.show()

        # Plotting validation scores, excluding the first 10 data points
        print("Plotting validation scores, excluding the first 10 data points")

        def smooth_curve(points, factor=0.9):
            smoothed_points = []
            for point in points:
                if smoothed_points:
                    previous = smoothed_points[-1]
                    smoothed_points.append(previous * factor + point * (1 - factor))
                else:
                    smoothed_points.append(point)
            return smoothed_points

        smooth_mae_history = smooth_curve(average_mae_history[10:])

        plt.plot(range(1, len(smooth_mae_history)+ 1), smooth_mae_history)
        plt.xlabel('Epochs')
        plt.ylabel('Validation MAE')
        plt.show()

        # Training the final model
        print("Training the final model")

        model = build_model()
        model.fit(train_data, train_targets,
                  epochs=80, batch_size=16, verbose=0)
        test_mse_score, test_mae_score = model.evaluate(test_data, test_targets)

        print(test_mae_score)

        with self.output().open('w') as f:
            for word in str(test_mae_score):
                f.write('{word}\n'.format(word=word))


class bostonhousingrun(luigi.Task):

    def requires(self):
        return bostonpredict()

    def output(self):
        # to get around the luigi unfulfilled dependency warning
        return luigi.LocalTarget('Complete3.txt')

    def run(self):
        words = []
        with self.output().open('w') as f:
            for word in words:
                f.write('{word}\n'.format(word=word))
        print("Complete")

if __name__ == "__main__":
    luigi.run()



