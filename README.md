# Crypto Data Pipeline (ETL)

This project demonstrates a professional 
Data Engineering pipeline
that extracts real-time cryptocurrency
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
the CoinGecko API to fetch multiple cryptocurrencies. 

2. **Transformation:** The raw JSON is
simplified to keep only essential fields.

3. **Local Landing:** Data is saved locally
 as 'raw_data.json' for initial validation.

4. **Cloud Loading:** The scripts uses 
**Boto3** to upload the processed file
to a simulated AWS S3 bucket ('data-lake-raw').

# Medallion Architecture Implemented

**Bronze:** Raw JSON storage.

**Silver:** Optimized Parquet files for big data.

**Gold:** Business-level aggregations and market status indicators. 

## Data Dictionary (Gold Layer)

The final output is a consolidated table with the following structure:

| Column | Type | Description |
| :--- | :--- | :--- |
| **id** | string | Cryptocurrency name (e.g., 'bitcoin', 'ethereum') |
| **symbol** | string | Ticker symbol (e.g., 'btc', 'eth') |
| **price_usd** | float | Current price in US Dollars |
| **price_range** | string | Price classification: 'Ultra Alto' (>$100k), 'Alto' (>$50k), 'Médio' (>$10k), 'Baixo' (<$10k) |
| **market_status** | string | Market indicator: 'High' (price > $50k) or 'Low' (price ≤ $50k) |
| **market_cap_usd** | float | Total market capitalization in USD |
| **market_cap_status** | string | Market cap classification: 'Mega' (>$1T), 'Grande' (>$100B), 'Médio' (>$10B), 'Pequeno' (<$10B) |
| **data_source** | string | Source API used (e.g., 'CoinGecko API') |
| **extracted_at** | timestamp | Date and time when data was extracted from the API |
| **last_update** | timestamp | Date and time when the record was last updated in the Gold layer |

## How to Run

1. Clone this repository.

2. Create and activate a virtual
environment:

```bash
python -m venv venv 
source venv /Scripts/activate
