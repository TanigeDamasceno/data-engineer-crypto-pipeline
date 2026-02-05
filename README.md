# Crypto Data Pipeline (ETL)

This project demonstrates a professional 
Data Enginnering pipeline
that extracts real-time cryptocurrecy
data and simulates a cloud-native
storage process using **LocalStack** 
(AWS S3 simulation)

## Tech Stack

* **Language:** Python 3.x

* **Data Source:** CoinGecko API

* **Cloud Simulation:** LocalStack
(AWS S3)

* **Libraries:** Boto3, Requests,
Pandas

* **Version Control:** Git

# The Pipeline Flow

1. **Extraction:** Python script calls 
the GoinGecko API to fetch live Bitcoin
data.

2. **Transformation:** The raw JSON is
simplified to keep only essential fields 
(Price, Market Cap, and Timestamp).

3. **Local Landing:** Data is saved locally
 as 'raw_data.json' for initial validation.

4. **Cloud Loading:** The scripts uses 
**Boto3** to upload the processed file
to a simulated AWS S3 bucket ('data-lake-raw').

# Medallion Architecture Implemented

**Bronze:** Raw JSON storage.

**Silver** Optimized Parquet files for big data.

**Gold** Business-level aggregations and market satus indicators. 

## How to Run

1. Clone this repository.

2. Create and activate a virtual
environment:

```bash
python -m venv venv 
source venv /Scripts/activate
