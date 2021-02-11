"""
Author:     Alan Danque
Date:       20201226
Class:      DSC 650
Exercise:   6.2B Includes Augmentation and Dropout
"""

import time
start_time = time.time()
from pathlib import Path
import numpy as np
import sys
from contextlib import redirect_stdout
from matplotlib import pyplot
from sklearn.metrics import confusion_matrix
import itertools

# save the final model to file
from keras.datasets import cifar10
from keras.utils import to_categorical
from keras.models import Sequential
from keras.layers import Conv2D
from keras.layers import MaxPooling2D
from keras.layers import Dense
from keras.layers import Dropout
from keras.layers import Flatten
from keras.optimizers import SGD

from keras.preprocessing.image import load_img
from keras.preprocessing.image import img_to_array
from keras.models import load_model

from keras.preprocessing.image import ImageDataGenerator
import tensorflow as tf
#import tensorflow.compat.v1 as tf
##tf.disable_v2_behavior()
print(tf.__version__)

physical_devices = tf.config.experimental.list_physical_devices('GPU')
assert len(physical_devices) > 0, "Not enough GPU hardware devices available"
config = tf.config.experimental.set_memory_growth(physical_devices[0], True)


# load train and test dataset
def load_dataset():
    # load dataset
    (trainX, trainY), (testX, testY) = cifar10.load_data()
    # one hot encode target values
    trainY = to_categorical(trainY)
    testY = to_categorical(testY)
    return trainX, trainY, testX, testY

# scale pixels
def prep_pixels(train, test):
    # convert from integers to floats
    train_norm = train.astype('float32')
    test_norm = test.astype('float32')
    # normalize to range 0-1
    train_norm = train_norm / 255.0
    test_norm = test_norm / 255.0
    # return normalized images
    return train_norm, test_norm


# plot diagnostic learning curves
def summarize_diagnostics(history):
    # plot loss
    pyplot.subplot(211)
    pyplot.title('Cross Entropy Loss')
    pyplot.plot(history.history['loss'], color='blue', label='train')
    pyplot.plot(history.history['val_loss'], color='orange', label='test')
    # plot accuracy
    pyplot.subplot(212)
    pyplot.title('Classification Accuracy')
    pyplot.plot(history.history['accuracy'], color='blue', label='train')
    pyplot.plot(history.history['val_accuracy'], color='orange', label='test')
    # save plot to file
    #filename = sys.argv[0].split('/')[-1]
    results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('results')
    filename = results_dir.joinpath('Assignment_6.2B_Summarized_Diagnostics_Plot.png')
    pyplot.savefig(filename)
    pyplot.close()

def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=pyplot.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    pyplot.imshow(cm, interpolation='nearest', cmap=cmap)
    pyplot.title(title)
    pyplot.colorbar()
    tick_marks = np.arange(len(classes))
    pyplot.xticks(tick_marks, classes, rotation=45)
    pyplot.yticks(tick_marks, classes)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        pyplot.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    pyplot.tight_layout()
    pyplot.ylabel('True label')
    pyplot.xlabel('Predicted label')

    results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('results')
    img_file = results_dir.joinpath('Assignment_6.2B_Confusion Matrix.png')
    pyplot.savefig(img_file)
    pyplot.show()


# define cnn model
def define_model():
    model = Sequential()
    model.add(Conv2D(32, (3, 3), activation='relu', kernel_initializer='he_uniform', padding='same', input_shape=(32, 32, 3)))
    model.add(Conv2D(32, (3, 3), activation='relu', kernel_initializer='he_uniform', padding='same'))
    model.add(MaxPooling2D((2, 2)))
    model.add(Conv2D(64, (3, 3), activation='relu', kernel_initializer='he_uniform', padding='same'))
    model.add(Conv2D(64, (3, 3), activation='relu', kernel_initializer='he_uniform', padding='same'))
    model.add(MaxPooling2D((2, 2)))
    model.add(Conv2D(128, (3, 3), activation='relu', kernel_initializer='he_uniform', padding='same'))
    model.add(Conv2D(128, (3, 3), activation='relu', kernel_initializer='he_uniform', padding='same'))
    model.add(MaxPooling2D((2, 2)))
    model.add(Flatten())
    model.add(Dense(128, activation='relu', kernel_initializer='he_uniform'))
    #model.add(Dropout(0.2, input_shape=(60,)))
    model.add(Dropout(0.2))
    model.add(Dense(10, activation='softmax'))
    # compile model
    opt = SGD(lr=0.001, momentum=0.9)
    model.compile(optimizer=opt, loss='categorical_crossentropy', metrics=['accuracy'])
    return model

# load and prepare the image
def load_image(filename):
    # load the image
    img = load_img(filename, target_size=(32, 32))
    # convert to array
    img = img_to_array(img)
    # reshape into a single sample with 3 channels
    img = img.reshape(1, 32, 32, 3)
    # prepare pixel data
    img = img.astype('float32')
    img = img / 255.0
    return img

# run the test harness for evaluating a model
def run_test_harness():
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')
    # load dataset
    print("loaing data set")
    trainX, trainY, testX, testY = load_dataset()
    print("--- %s seconds ---" % (time.time() - start_time))

    # prepare pixel data
    print("preparing pixel data")
    trainX, testX = prep_pixels(trainX, testX)

    # plot first few images
    for i in range(9):
        pyplot.subplot(330 + 1 + i)
        x = trainX[i]
        x = np.reshape(x, (32, 32, 3))
        pyplot.imshow(x)
        #pyplot.imshow(x.transpose(1, 2, 0))
        #plt.imshow(tf.shape(tf.squeeze(x)))
    # show the figure

    results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('results')
    img_file = results_dir.joinpath('Assignment_6.2B_Sample Review of 9 CiFar Image.png')
    pyplot.savefig(img_file)
    pyplot.show()
    print("--- %s seconds ---" % (time.time() - start_time))

    # define model
    print("defining the model")
    model = define_model()
    summary_file1 = results_dir.joinpath('Assignment_6.2B_ModelSummary.txt')
    with open(summary_file1, 'w') as f:
        with redirect_stdout(f):
            model.summary()
    print("--- %s seconds ---" % (time.time() - start_time))


    # create data generator
    print("adding augmentation creating data generator")
    #datagen = ImageDataGenerator(width_shift_range=0.1, height_shift_range=0.1, horizontal_flip=True)
    train_datagen = ImageDataGenerator(rescale=1. / 255, shear_range=0.2, zoom_range=0.2, horizontal_flip=True)
    test_datagen = ImageDataGenerator(rescale=1. / 255)
    print("--- %s seconds ---" % (time.time() - start_time))

    # prepare iterator
    print("preparing iterator")
    #it_train = datagen.flow(trainX, trainY, batch_size=64)
    train_generator = train_datagen.flow(trainX, trainY, batch_size=64)
    validation_generator = test_datagen.flow(trainX, trainY, batch_size=64)
    print("--- %s seconds ---" % (time.time() - start_time))

    # fit model
    print("fitting the model")
    #history = model.fit(trainX, trainY, epochs=100, batch_size=64, validation_data=(testX, testY), verbose=0)
    #history = model.fit(it_train, trainY, epochs=100, batch_size=64, validation_data=(testX, testY), verbose=0)

    nb_train_samples = 18000
    nb_validation_samples = 7000
    epochs = 20
    batch_size = 144
    history = model.fit_generator(
        train_generator,
        steps_per_epoch=nb_train_samples // batch_size,
        epochs=epochs,
        validation_data=validation_generator,
        validation_steps=nb_validation_samples // batch_size)

    #model.fit(trainX, trainY, epochs=100, batch_size=64, verbose=0)
    print("--- %s seconds ---" % (time.time() - start_time))

    # evaluate model
    print("evaluating the model")
    _, acc = model.evaluate(testX, testY, verbose=0)
    print('> %.3f' % (acc * 100.0))
    print("--- %s seconds ---" % (time.time() - start_time))

    # save model
    results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('results')
    result_model_file = results_dir.joinpath('Assignment_6.2B_model.h5')
    model.save(result_model_file)
    print('Saved trained model at %s ' % result_model_file)

    # learning curves
    print("preparing summary diagnostics")
    summarize_diagnostics(history)
    print("--- %s seconds ---" % (time.time() - start_time))

    print("preparing confusion matrix")
    # Predict the values from the validation dataset
    Y_pred = model.predict(testX)
    # Convert predictions classes to one hot vectors
    Y_pred_classes = np.argmax(Y_pred, axis=1)
    # Convert validation observations to one hot vectors
    Y_true = np.argmax(testY, axis=1)
    confusion_mtx = confusion_matrix(Y_true, Y_pred_classes)
    # plot the confusion matrix
    plot_confusion_matrix(confusion_mtx, classes=range(10))
    print("--- %s seconds ---" % (time.time() - start_time))

    print("preparing correct vs incorrect classifications review")
    correct_indices = np.nonzero(Y_pred_classes == Y_true)[0]
    incorrect_indices = np.nonzero(Y_pred_classes != Y_true)[0]
    print(len(Y_pred_classes))
    print(len(Y_true))
    print(len(correct_indices), " classified correctly")
    print(len(incorrect_indices), " classified incorrectly")

    # adapt figure size to accomodate 18 subplots
    pyplot.rcParams['figure.figsize'] = (7, 14)
    figure_evaluation = pyplot.figure()

    # plot 9 correct predictions
    for i, correct in enumerate(correct_indices[:14]):
        pyplot.subplot(6, 3, i + 1)
        pyplot.imshow(testX[correct], cmap='gray', interpolation='none')
        pyplot.title(
            "Predicted: {}, Truth: {}".format(classes[Y_pred[correct].argmax()],
                                              classes[testY[correct].argmax()]))
        pyplot.xticks([])
        pyplot.yticks([])
    img_file = results_dir.joinpath('Assignment_6.2B_Correct Predictions Sample.png')
    pyplot.savefig(img_file)
    pyplot.show()

    # plot 9 incorrect predictions
    for i, incorrect in enumerate(incorrect_indices[:9]):
        pyplot.subplot(6, 3, i + 10)
        pyplot.imshow(testX[incorrect], cmap='gray', interpolation='none')
        pyplot.title(
            "Predicted {}, Truth: {}".format(classes[Y_pred[incorrect].argmax()],
                                             classes[testY[incorrect].argmax()]))
        pyplot.xticks([])
        pyplot.yticks([])

    img_file = results_dir.joinpath('Assignment_6.2B_Incorrect Predictions.png')
    pyplot.savefig(img_file)
    pyplot.show()
    print("--- %s seconds ---" % (time.time() - start_time))


def run_example_prediction():
    # predict the class
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')
    print("Attempting load model to predict based on image: sample_image-1.png")
    results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('results')
    result_model_file = results_dir.joinpath('Assignment_6.2B_model.h5')
    model = load_model(result_model_file)
    summary_file1 = results_dir.joinpath('Assignment_6.2B_ModelSummary_AfterLoad.txt')
    with open(summary_file1, 'w') as f:
        with redirect_stdout(f):
            model.summary()

    data_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('Data')
    filename = data_dir.joinpath('sample_image-1.png')
    img = load_image(filename)

    result = model.predict_classes(img)
    print("The picture prediction is:......")
    print(classes[result[0]])
    print("--- %s seconds ---" % (time.time() - start_time))

# entry point, run the test harness
run_test_harness()

run_example_prediction()
