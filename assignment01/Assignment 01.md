---
title: Assignment 1
subtitle: Computer performance, reliability, and scalability calculation
author: Alan Danque
---

## 1.2 

#### a. Data Sizes

| Data Item                                  | Size per Item | Explanation |
|--------------------------------------------|--------------:|
| 128 character message.                     | 1024 Bytes    |= 8bits per character * 128 characters
| 1024x768 PNG image                         | 1.536 MB      |= ((1024 horizontal * 768 vertical * 16 bit ~ if 16bit color) ) / ( 8 * 1024) ) / 1024 =  1.536 MB 
| 1024x768 RAW image                         | 2.25 MB       |= ((1024 horizontal * 768 vertical * 24bit if 24bit raw ) ) / ( 8 * 1024) ) / 1024 = 2.25 MB 
| HD (1080p) HEVC Video (15 minutes)         | 160 MB        |= 1920 * 1080 * 24 bit * 30 fps * 900 duration in seconds = 1,343,692,800,000 / 8192 = 164,025,000 KB / 1024 = 160,180 MB / 1000 (HEVC compression ratio) = 160 MB
| HD (1080p) Uncompressed Video (15 minutes) | 160,180 MB    |= 1920 * 1080 * 24 bit * 30 fps * 900 duration in seconds = 1,343,692,800,000 / 8192 = 164,025,000 KB / 1024 = 160,180 MB
| 4K UHD HEVC Video (15 minutes)             | 640 MB        |= 3840 * 2160 * 24 bit * 30 fps * 900 duration in seconds = 5,374,771,200,000 / 8192 = 656,100,000 KB / 1024 = 640,722 MB / 1000 = 640 MB
| 4k UHD Uncompressed Video (15 minutes)     | 640,722 MB    |= 3840 * 2160 * 24 bit * 30 fps * 900 duration in seconds = 5,374,771,200,000 / 8192 = 656,100,000 KB / 1024 = 640,722 MB
| Human Genome (Uncompressed)                | 200 GB        | https://medium.com/precision-medicine/how-big-is-the-human-genome-e90caa3409b0#:~:text=VCF%20file%20size%20of%20about,looking%20at%20genome%20storage%20size

#### b. Scaling

|                                           | Size     |   # HD | Explanation |
|-------------------------------------------|---------:|-------:|
| Daily Twitter Tweets (Uncompressed)       | 512 TB   |    154 |= (500,000,000 *  1024 Bytes) / 1000 =  512,000,000 MB     |   (512,000,000 MB  * 3 hdfs sharded copies) = 1,536,000,000 MB| 1,536,000,000 MB / 10,000,000 MB HDs    = 154 HDs   
| Daily Twitter Tweets (Snappy Compressed)  | 342 TB   |    103 |= (500,000,000 *  1024 Bytes) / 1000 =  512,000,000 MB / 1.5 compression ratio = 341,333,333 MB * 3 hdfs sharded copies = 1,024,000,000 MB | 1,024,000,000 MB / 10,000,000 MB HDs    = 103 HDs   
| Daily Instagram Photos                    | 4 PB     |  1,234 |= 75,000,000 * 1.536 MB PNGs + 25,000,000 * 160 MB HD 1080P HEVC video = 115,200,000,000,000 + 4,000,000,000,000,000 = 4,115,200,000 MB * 3 hdfs sharded copies = 12,345,600,000 MB  |  12,345,600,000 MB  / 10,000,000 MB = 1235 10TB HDs 
| Daily YouTube Videos                      | 7.6 TB   |      3 |= 500Hrs * 60min = 30,000 min / 15 min video = 2,000 videos * 160 MB for each 15minute = 320GB per HR * 24 hours = 7,680,000 MB per day * 3 sharded hdfs copies = 23,040,000 MB   
| Yearly Twitter Tweets (Uncompressed)      | 560 PB   | 56,064 |= 1,536,000,000 MB * 365 = 560,640,000,000 MB | 560,640,000,000 MB / 10,000,000 MB = 56,064 10TB HDs   
| Yearly Twitter Tweets (Snappy Compressed) | 373 PB   | 37,376 |= 1,024,000,000 MB * 365 = 373,760,000,000 MB  | 373,760,000,000 MB  / 10,000,000 MB HDs = 37,376 10TB HDs 
| Yearly Instagram Photos                   | 12.3 PB  |   1234 |= 4,115,200,000 MB * 365  =  12,345,600,000 MB |  12,345,600,000 MB / 10,000,000 MB 10TB HDs = 1,234 HDs   
| Yearly YouTube Videos                     | 2.8 PB   |    841 |= 7,680,000 MB * 365 =  2,803,200,000 MB  | 2,803,200,000 MB * 3 copies hdfs= 8,409,600,000  / 10,000,000 MB 10TB Hds = 841 10TB HDs  

#### c. Reliability
|                                    |  # HD  | # Failures | Explanation |
|------------------------------------|-------:|-----------:|
| Twitter Tweets (Uncompressed)      | 56,064 |    140     |.99% AFR for 10TB drives Q3 2020 for every 1200 drives 3 failed thus (56,064 / 1,200) = 46.72 * 3 = 140 Drives Predicted to Fail  
| Twitter Tweets (Snappy Compressed) | 37,376 |     93     |.99% AFR for 10TB drives Q3 2020 for every 1200 drives 3 failed thus (37,376 / 1,200) = 31.14 * 3 =  93.44 Drives Predicted to Fail
| Instagram Photos                   |  1,234 |      3     |.99% AFR for 10TB drives Q3 2020 for every 1200 drives 3 failed thus (1,234 / 1,200) = 1.028 * 3 = 3 Drives Predicted to Fail
| YouTube Videos                     |    841 |      2     |.99% AFR for 10TB drives Q3 2020 for every 1200 drives 3 failed thus (841 / 1,200) = .700 * 3 = 2 Drives Predicted to Fail

#### d. Latency

|                           | One Way Latency      | Source
|---------------------------|---------------------:|
| Los Angeles to Amsterdam  | 146.779 ms           | https://wondernetwork.com/pings/Los%20Angeles/Amsterdam
| Low Earth Orbit Satellite | 638 ms               | https://corpblog.viasat.com/satellite-internet-latency-whats-the-big-deal/
| Geostationary Satellite   | 476 ms               | https://www.groundcontrol.com/Satellite_Low_Latency.htm#:~:text=The%20theoretically%20fastest%20possible%20ping,as%20consumer%20grade%20service%20providers.
| Earth to the Moon         | 1260 ms              | https://www.forbes.com/sites/quora/2016/08/31/when-we-eventually-get-to-mars-this-is-how-the-internet-will-work-there/?sh=319d84467dae
| Earth to Mars             | 3.03 minutes         | https://www.forbes.com/sites/quora/2016/08/31/when-we-eventually-get-to-mars-this-is-how-the-internet-will-work-there/?sh=319d84467dae

