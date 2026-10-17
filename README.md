# DSA5208-Rainfall-Analysis
This repository contains codes and set up for DSA5208 Assignment 2 - Rainfall Analysis and Prediction

### Software Used
![Python](https://img.shields.io/badge/Python-Programming-blue.svg?style=flat&logo=python&logoColor=FFF)
![PySpark](https://img.shields.io/badge/Apache-Spark-E4682A.svg?style=flat&logo=apachespark&logoColor=FFF)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626.svg?style=flat&logo=Jupyter)
![Google Cloud Platform](https://img.shields.io/badge/Google-Cloud-34A853.svg?style=flat&logo=google-cloud&logoColor=FFF)

### Team Members
|  Matriculation Number | Name                |
| ---                   | ---                 |
| A0220825M             | Koh Si Xing         |
| A0221663L             | Ashley Lee Ruoxin   |
| A0333470L             | Kevin Royce Thomson | 

## Main Folders

| Folder Name                           | Purpose                                                                 |
| ---                                   | ---                                                                     |
| Part 1. EDA                           | Codes for Data Cleaning and EDA using PySpark                           |
| Part 2. Modeling                      | Codes for Feature Engineering, Grid Search, and Model Prediction        |
| Project Setup                         | Guide for setting up GCP                                                | 

**Archive** : Folder containing the past codes and logic on EDA, testing GCP connection, etc. NOT TO BE USED FOR ASSESSMENT.

## Quick Start on GCP Clusters (Each Run)
**For Full Setup, please head to Project Setup > GCP Setup.md**

1. Google Cloud Login
```shell
gcloud auth login
```

2. Cluster Creation
```shell
gcloud dataproc clusters create rainfall-dev --region=asia-southeast1 --zone=asia-southeast1-b --num-workers=3 --master-machine-type=n2-standard-4 --master-boot-disk-type=pd-standard --master-boot-disk-size=100 --worker-machine-type=n2-highmem-4 --worker-boot-disk-type=pd-balanced --worker-boot-disk-size=100 --image-version=2.2-debian12 --enable-component-gateway --optional-components=JUPYTER --max-idle=30m
```

3. SSH Tunneling
```shell
gcloud compute ssh --zone "asia-southeast1-b" "rainfall-dev-m" --tunnel-through-iap --project "<<Project Name>>" -- -L 8123:localhost:8123 -N
```

4. Cluster Deletion
```shell
gcloud dataproc clusters delete rainfall-dev --region=asia-southeast1
```