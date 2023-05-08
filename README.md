# Project: STEDI Human Balance Analytics

## Introduction

Spark and AWS Glue allow you to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes. As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model. 

## Project Details

The STEDI Team is diligently working on developing a hardware STEDI Step Trainer that serves three purposes. Firstly, the equipment will train the user to perform a STEDI balance exercise. Secondly, the device comes equipped with sensors that collect data, which will then train a machine-learning algorithm to detect steps. Lastly, there is a companion mobile app that collects customer data and interacts with the device sensors.

Millions of early adopters are willing to purchase the STEDI Step Trainers and use them, with several customers having already received their Step Trainers, installed the mobile application, and begun using them to test their balance. The Step Trainer only operates as a motion sensor, measuring the distance of the detected object. Meanwhile, the mobile app utilizes a mobile phone accelerometer, detecting motion in the X, Y, and Z directions.

The STEDI team aims to train a machine learning model to accurately detect steps in real-time using motion sensor data as input while considering privacy as a primary issue when deciding which data can be employed. For research purposes, some early adopters have agreed to share their data. Only data from these customers' Step Trainer and accelerometer should be used as the ML model's training data.

## Implementation

### Landing Zone

**Glue Tables**:

- [customer_landing.sql](script/customer_landing.sql)
- [accelerometer_landing.sql](script/accelerometer_landing.sql)

**Athena**:
Landing Zone data query results

*Customer Landing*:

<figure>
  <img src="images/customer_landing.png" alt="Customer Landing data" width=60% height=60%>
</figure>

*Accelerometer Landing*:

<figure>
  <img src="images/accelerometer_landing.png" alt="Accelerometer Landing data" width=60% height=60%>
</figure>

### Trusted Zone

**Glue job scripts**:

- [customer_landing_to_trusted.py](scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted_zone.py](scripts/accelerometer_landing_to_trusted.py)

**Athena**:
Trusted Zone Query results:

<figure>
  <img src="images/customer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>

### Curated Zone

**Glue job scripts**:

- [customer_trusted_to_curated.py](scripts/customer_trusted_to_curated.py)
- [trainer_trusted_to_curated.py](scripts/trainer_trusted_to_curated.py)
