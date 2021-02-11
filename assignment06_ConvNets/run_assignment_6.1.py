"""
Author:     Alan Danque
Date:       20201226
Class:      DSC 650
Exercise:   6.1
"""

# TensorFlow and tf.keras
import csv
from contextlib import redirect_stdout
import pandas as pd
import imageio
import keras
import tensorflow as tf
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
import itertools
# Helper libraries
import numpy as np
import matplotlib.pyplot as plt
print(tf.__version__)

import time
start_time = time.time()
from pathlib import Path

# let's keep our keras backend tensorflow quiet
import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='3'
# for testing on CPU
#os.environ['CUDA_VISIBLE_DEVICES'] = ''

# keras imports for the dataset and building our neural network
from keras.datasets import mnist
from keras.models import Sequential, load_model
from keras.utils import to_categorical
from keras.layers.core import Dense, Dropout, Activation
from keras.utils import np_utils
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()
print(tf.__version__)

#import tensorflow as tf
#from keras.backend.tensorflow_backend import set_session
config = tf.compat.v1.ConfigProto # tf.ConfigProto()
#config.gpu_options.per_process_gpu_memory_fraction = 0.3
#config.gpu_options.allow_growth = True
#set_session(tf.Session(config=config))
physical_devices = tf.config.experimental.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(physical_devices[0], True)

from keras import layers
from keras import models

current_dir = Path('C:/Users/aland/class/DSC650/dsc650/')
results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)


model = models.Sequential()
model.add(layers.Conv2D(32, (3, 3), activation='relu', input_shape=(28, 28, 1)))
model.add(layers.MaxPooling2D((2,2)))
model.add(layers.Conv2D(64, (3, 3), activation='relu'))
model.add(layers.MaxPooling2D((2,2)))
model.add(layers.Conv2D(64, (3, 3), activation='relu'))
summary_file1 = results_dir.joinpath('Assignment_6.1_ModelSummary1.txt')
with open(summary_file1, 'w') as f:
    with redirect_stdout(f):
        model.summary()



model.add(layers.Flatten())
model.add(layers.Dense(64, activation='relu'))
model.add(layers.Dense(10, activation='softmax'))
#model.summary()
summary_file2 = results_dir.joinpath('Assignment_6.1_ModelSummary2.txt')
with open(summary_file2, 'w') as f:
    with redirect_stdout(f):
        model.summary()


(train_images, train_labels), (test_images, test_labels) = mnist.load_data()

print("Review a few of the training images and labels")
fig = plt.figure()
for i in range(9):
    plt.subplot(3,3,i+1)
    plt.tight_layout()
    plt.imshow(train_images[i], cmap='gray', interpolation='none')
    plt.title("Digit: {}".format(train_labels[i]))
    plt.xticks([])
    plt.yticks([])
img_file = results_dir.joinpath('Assignment_6.1_Sample Review of 9 Digits.png')
plt.savefig(img_file)
plt.show()

# Pixel value distribution
fig = plt.figure()
plt.subplot(2,1,1)
plt.imshow(train_images[0], cmap='gray', interpolation='none')
plt.title("Digit: {}".format(train_labels[0]))
plt.xticks([])
plt.yticks([])
img_file = results_dir.joinpath('Assignment_6.1_Digit Review.png')
plt.savefig(img_file)
plt.show()

plt.subplot(2,1,2)
plt.hist(train_images[0].reshape(784))
plt.title("Pixel Value Distribution")
img_file = results_dir.joinpath('Assignment_6.1_Pixel Value Distribution.png')
plt.savefig(img_file)
plt.show()

# reshape and normalize the train and test images
train_images = train_images.reshape((60000, 28, 28, 1))
train_images = train_images.astype('float32') / 255
test_images = test_images.reshape((10000, 28, 28, 1))
test_images = test_images.astype('float32') / 255

# Convert to categorical
train_labels = to_categorical(train_labels)
test_labels = to_categorical(test_labels)
print("Train_Labels")
print(train_labels)
print("Test_Labels")
print(test_labels)

model.compile(optimizer='rmsprop',
              loss='categorical_crossentropy',
              metrics=['accuracy'])


# training the model and saving metrics in history
history = model.fit(train_images, train_labels,
          batch_size=128,
          epochs=20,
          verbose=2,
          validation_data=(test_images, test_labels))

test_loss, test_acc = model.evaluate(test_images, test_labels)
print('Test Accuracy: %s ' % test_acc)

result_model_file = results_dir.joinpath('Assignment_6.1_model.h5')
model.save(result_model_file)
print('Saved trained model at %s ' % result_model_file)



# plotting the metrics
fig = plt.figure()
#plt.subplot(2,1,1)
plt.plot(history.history['acc'])
plt.plot(history.history['val_acc'])
plt.title('model accuracy')
plt.ylabel('accuracy')
plt.xlabel('epoch')
plt.legend(['train', 'test'], loc='lower right')
img_file = results_dir.joinpath('Assignment_6.1_Model Accuracy Validation.png')
plt.savefig(img_file)
plt.show()

#plt.subplot(2,1,2)
plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])
plt.title('model loss')
plt.ylabel('loss')
plt.xlabel('epoch')
plt.legend(['train', 'test'], loc='upper right')
plt.tight_layout()
img_file = results_dir.joinpath('Assignment_6.1_Model Loss Validation.png')
plt.savefig(img_file)
plt.show()

#fig

# Load the model from file and continue
mnist_model = load_model(result_model_file)
loss_and_metrics = mnist_model.evaluate(test_images, test_labels, verbose=2)
print("Test Loss", loss_and_metrics[0])
print("Test Accuracy", loss_and_metrics[1])
# https://www.kaggle.com/adityaecdrid/mnist-with-keras-for-beginners-99457
print("--- %s seconds ---" % (time.time() - start_time))



# Look at confusion matrix
#Note, this code is taken straight from the SKLEARN website, an nice way of viewing confusion matrix.
def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    img_file = results_dir.joinpath('Assignment_6.1_Confusion Matrix.png')
    plt.savefig(img_file)
    plt.show()


# convert class vectors to binary class matrices One Hot Encoding
num_classes = 10
y_train = keras.utils.to_categorical(train_labels, num_classes)
X_train, X_val, Y_train, Y_val = train_test_split(train_images, train_labels, test_size = 0.1, random_state=42)

"""
mnist_model.fit(X_train)
h = model.fit_generator(mnist_model.flow(X_train,Y_train, batch_size=batch_size),
                              epochs = epochs, validation_data = (X_val,Y_val),
                              verbose = 1, steps_per_epoch=X_train.shape[0] // batch_size
                              , callbacks=[learning_rate_reduction],)

final_loss, final_acc = model.evaluate(X_val, Y_val, verbose=0)
print("Final loss: {0:.6f}, final accuracy: {1:.6f}".format(final_loss, final_acc))
"""

# Predict the values from the validation dataset
Y_pred = mnist_model.predict(X_val)
# Convert predictions classes to one hot vectors
Y_pred_classes = np.argmax(Y_pred, axis = 1)
# Convert validation observations to one hot vectors
Y_true = np.argmax(Y_val, axis = 1)
# compute the confusion matrix
confusion_mtx = confusion_matrix(Y_true, Y_pred_classes)
# plot the confusion matrix
plot_confusion_matrix(confusion_mtx, classes = range(10))
#sample['PR'] = sample['PR'].apply(lambda x: 'NaN' if x < 90 else x)



correct_indices = np.nonzero(Y_pred_classes == Y_true)[0]
incorrect_indices = np.nonzero(Y_pred_classes != Y_true)[0]

print(len(Y_pred_classes))
print(len(Y_true))
print(len(correct_indices)," classified correctly")
print(len(incorrect_indices)," classified incorrectly")


# adapt figure size to accomodate 18 subplots
plt.rcParams['figure.figsize'] = (7,14)
figure_evaluation = plt.figure()

# plot 9 correct predictions
for i, correct in enumerate(correct_indices[:14]):
    plt.subplot(6,3,i+1)
    plt.imshow(X_val[correct], cmap='gray', interpolation='none')
    plt.title(
      "Predicted: {}, Truth: {}".format(Y_pred[correct].argmax(),
                                        Y_val[correct].argmax()))
    plt.xticks([])
    plt.yticks([])
img_file = results_dir.joinpath('Assignment_6.1_Correct Predictions Sample.png')
plt.savefig(img_file)
plt.show()


# plot 9 incorrect predictions
for i, incorrect in enumerate(incorrect_indices[:9]):
    plt.subplot(6,3,i+10)
    plt.imshow(X_val[incorrect], cmap='gray', interpolation='none')
    plt.title(
      "Predicted {}, Truth: {}".format(Y_pred[incorrect].argmax(),
                                       Y_val[incorrect].argmax()))
    plt.xticks([])
    plt.yticks([])

img_file = results_dir.joinpath('Assignment_6.1_Incorrect Predictions.png')
plt.savefig(img_file)
plt.show()

# Prediction
print("Prediction using an images via url")
im = imageio.imread("https://i.imgur.com/a3Rql9C.png")
gray = np.dot(im[...,:3], [0.299, 0.587, 0.114])
plt.imshow(gray, cmap = plt.get_cmap('gray'))
plt.show()
# reshape the image
img_rows, img_cols = 28, 28
gray = gray.reshape(1, img_rows, img_cols, 1)
# normalize image
gray /= 255

# predict digit
predictionimg = mnist_model.predict(gray)
print(predictionimg.argmax())


print("Prediction using one of the mnist images")
w = 0.28
h = 0.28
fig = plt.figure(frameon=False)
fig.set_size_inches(w,h)
img_file = results_dir.joinpath('Assignment_6.1_imagetopredict.png')
reviewdigit3 = X_val[80]

#plt.imshow(reviewdigit3, cmap=plt.cm.binary)
#plt.imshow(reviewdigit3, cmap='gray', interpolation='none')
fig.canvas.draw()

plt.imshow(reviewdigit3, cmap = plt.get_cmap('gray'))
plt.savefig(img_file)
plt.show()

im = imageio.imread(img_file)
gray = np.dot(im[...,:3], [0.299, 0.587, 0.114])
plt.imshow(gray, cmap = plt.get_cmap('gray'))
plt.show()

# reshape the image
img_rows, img_cols = 28, 28
gray = gray.reshape(1, img_rows, img_cols, 1)
# normalize image
gray /= 255

# predict digit
predictionimg = mnist_model.predict(gray)
print(predictionimg.argmax())
print("--- %s seconds ---" % (time.time() - start_time))

print(type(Y_pred))
print(len(Y_pred))

print("Gathering Predictions")

prediction_values = []
indices = np.nonzero(Y_pred)[0]
for i, v in enumerate(indices[:]):
    #prediction_values[i].append(Y_pred[v].argmax())
    #print(Y_pred[v].argmax())
    prediction_values.append(Y_pred[v].argmax())
    #print(Y_pred[i].argmax())

d = {}
for index, value in enumerate(prediction_values):
    d[index] = value

csv_file_out = results_dir.joinpath('Assignment_6.1_PredictedValues.csv')
with open(csv_file_out, 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in d.items():
       writer.writerow([key, value])

