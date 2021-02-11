"""
Author:     Alan Danque
Date:       20201226
Class:      DSC 650
Exercise:   6.3
"""

import glob, os
import time
start_time = time.time()
from pathlib import Path

#import tensorflow as tf
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()
print(tf.__version__)
import matplotlib.pyplot as plt
from tensorflow.keras.preprocessing.image import load_img
from tensorflow.keras.preprocessing.image import img_to_array
from tensorflow.keras.applications.imagenet_utils import decode_predictions


physical_devices = tf.config.experimental.list_physical_devices('GPU')
assert len(physical_devices) > 0, "Not enough GPU hardware devices available"
config = tf.config.experimental.set_memory_growth(physical_devices[0], True)


import numpy as np
# import the models for further classification experiments
from tensorflow.keras.applications import (
        vgg16,
        resnet50,
        mobilenet,
        inception_v3
    )

# init the models
vgg_model = vgg16.VGG16(weights='imagenet')
inception_model = inception_v3.InceptionV3(weights='imagenet')
resnet_model = resnet50.ResNet50(weights='imagenet')
mobilenet_model = mobilenet.MobileNet(weights='imagenet')


def run_load_prediction(filename):
    # assign the image path for the classification experiments
    # load an image in PIL format
    original = load_img(filename, target_size=(224, 224))
    print('PIL image size',original.size)
    plt.imshow(original)
    plt.show()

    # convert the PIL image to a numpy array
    # IN PIL - image is in (width, height, channel)
    # In Numpy - image is in (height, width, channel)
    numpy_image = img_to_array(original)
    plt.imshow(np.uint8(numpy_image))
    plt.show()
    print('numpy array size',numpy_image.shape)

    # Convert the image / images into batch format
    # expand_dims will add an extra dimension to the data at a particular axis
    # We want the input matrix to the network to be of the form (batchsize, height, width, channels)
    # Thus we add the extra dimension to the axis 0.
    image_batch = np.expand_dims(numpy_image, axis=0)
    print('image batch size', image_batch.shape)
    plt.imshow(np.uint8(image_batch[0]))

    # prepare the image for the VGG model
    processed_image = resnet50.preprocess_input(image_batch.copy())

    # get the predicted probabilities for each class
    predictions = resnet_model.predict(processed_image)
    # print predictions
    # convert the probabilities to class labels
    # we will get top 5 predictions which is the default
    label = decode_predictions(predictions)
    # print VGG16 predictions
    for prediction_id in range(len(label[0])):
        print(label[0][prediction_id])

data_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/').joinpath('Data')
os.chdir(data_dir)
for filename in glob.glob("*.png"):
    print(filename)
    #filename = 'C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment06/Data/sample_image-1.png'
    run_load_prediction(filename)
