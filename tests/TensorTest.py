# importing the tensorflow package
import tensorflow as tf

tf.test.is_built_with_cuda()

tf.test.is_gpu_available(cuda_only=False, min_cuda_compute_capability=None)
"""
C:\Users\aland\AppData\Local\Continuum\anaconda3\python.exe C:/Users/aland/class/DSC650/dsc650/tests/TensorTest.py
2020-11-27 16:59:08.008070: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cudart64_101.dll
WARNING:tensorflow:From C:/Users/aland/class/DSC650/dsc650/tests/TensorTest.py:6: is_gpu_available (from tensorflow.python.framework.test_util) is deprecated and will be removed in a future version.
Instructions for updating:
Use `tf.config.list_physical_devices('GPU')` instead.
2020-11-27 16:59:11.811396: I tensorflow/core/platform/cpu_feature_guard.cc:142] This TensorFlow binary is optimized with oneAPI Deep Neural Network Library (oneDNN)to use the following CPU instructions in performance-critical operations:  AVX2
To enable them in other operations, rebuild TensorFlow with the appropriate compiler flags.
2020-11-27 16:59:11.843253: I tensorflow/compiler/xla/service/service.cc:168] XLA service 0x1a8b2552f70 initialized for platform Host (this does not guarantee that XLA will be used). Devices:
2020-11-27 16:59:11.843652: I tensorflow/compiler/xla/service/service.cc:176]   StreamExecutor device (0): Host, Default Version
2020-11-27 16:59:11.847701: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library nvcuda.dll
2020-11-27 16:59:11.886385: I tensorflow/core/common_runtime/gpu/gpu_device.cc:1716] Found device 0 with properties: 
pciBusID: 0000:01:00.0 name: GeForce GTX 1650 with Max-Q Design computeCapability: 7.5
coreClock: 1.245GHz coreCount: 16 deviceMemorySize: 4.00GiB deviceMemoryBandwidth: 104.34GiB/s
2020-11-27 16:59:11.886990: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cudart64_101.dll
2020-11-27 16:59:11.922033: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cublas64_10.dll
2020-11-27 16:59:14.380506: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cufft64_10.dll
2020-11-27 16:59:15.025658: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library curand64_10.dll
2020-11-27 16:59:15.055334: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cusolver64_10.dll
2020-11-27 16:59:15.074849: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cusparse64_10.dll
2020-11-27 16:59:15.151124: I tensorflow/stream_executor/platform/default/dso_loader.cc:48] Successfully opened dynamic library cudnn64_7.dll
2020-11-27 16:59:15.152081: I tensorflow/core/common_runtime/gpu/gpu_device.cc:1858] Adding visible gpu devices: 0
2020-11-27 16:59:16.364664: I tensorflow/core/common_runtime/gpu/gpu_device.cc:1257] Device interconnect StreamExecutor with strength 1 edge matrix:
2020-11-27 16:59:16.364961: I tensorflow/core/common_runtime/gpu/gpu_device.cc:1263]      0 
2020-11-27 16:59:16.365136: I tensorflow/core/common_runtime/gpu/gpu_device.cc:1276] 0:   N 
2020-11-27 16:59:16.367535: I tensorflow/core/common_runtime/gpu/gpu_device.cc:1402] Created TensorFlow device (/device:GPU:0 with 2907 MB memory) -> physical GPU (device: 0, name: GeForce GTX 1650 with Max-Q Design, pci bus id: 0000:01:00.0, compute capability: 7.5)
2020-11-27 16:59:16.374351: I tensorflow/compiler/xla/service/service.cc:168] XLA service 0x1a8f2bc1970 initialized for platform CUDA (this does not guarantee that XLA will be used). Devices:
2020-11-27 16:59:16.374797: I tensorflow/compiler/xla/service/service.cc:176]   StreamExecutor device (0): GeForce GTX 1650 with Max-Q Design, Compute Capability 7.5

Process finished with exit code 0

"""