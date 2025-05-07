# Crypto Token Data Collection & BigQuery Integration

This project was developed as part of a freelance assignment focused on collecting and preprocessing 5-minute interval price data for 38 cryptocurrency tokens. The final dataset includes OHLCV and market cap information, structured and delivered as a BigQuery-compatible SQL DataFrame.

## 🔍 Project Objective

To gather accurate and comprehensive price data from multiple sources and prepare it in a format suitable for advanced data analysis using BigQuery.

## 📊 Data Collected

For each of the 38 tokens, the following data was collected:
- Open, High, Low, Close (OHLC) prices
- Total Volume
- Volume from Top 5 Exchanges (where available)
- Market Capitalization

## 🧰 Tech Stack & Tools

- **Python**
- **Pandas** for data preprocessing
- **APIs Used**:
  - [Binance API](https://binance-docs.github.io/apidocs/)
  - [CryptoCompare API](https://min-api.cryptocompare.com/)
  - [KuCoin API](https://docs.kucoin.com/)
- **Google BigQuery** for data delivery format
- `.env` file for secure API key management

## 🚀 How It Works

1. Python scripts fetch token data from the APIs.
2. Raw data is cleaned and normalized using Pandas.
3. DataFrames are formatted into BigQuery-compatible structures.
4. Output is ready for further analysis or direct upload to BigQuery.

## 🛡️ Security

Sensitive credentials such as API keys are stored in a `.env` file and **not included** in the repository.


