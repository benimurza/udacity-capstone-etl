# Udacity Capstone ETL
This project contains the Spark ETL pipeline for the Udacity Capstone project which loads stock datasets to Amazon S3. A datamart built on top of the data in S3 runs using Airflow, and loads the data in a Redshift database.

### Data sources
There are a total of three data sources:
* stocks and their symbols from S&P 500 companies, Source [https://www.kaggle.com/qks1lver/amex-nyse-nasdaq-stock-histories]
* federal interest rates, Source [https://www.kaggle.com/federalreserve/interest-rates]
* most voted (top 25) Reddit world news, per day, Source [https://www.kaggle.com/aaron7sun/stocknews#RedditNews.csv]

### Process
The pipeline loads the data, processes it, and writes it to S3. 
For each company present in the stock data, a separate dataset is created, containing its symbol, symbol description and exchange where it is traded. Furthermore, for each stock entry, the difference between the opening price and adjusted closing price is calculated. The stock data is then partitioned by `day`, `month` and `year`, due to it being very large.  This also enables specific days/months/years to be analysed, without having to load the entire dataset. 
The news are partitioned by year, while the other datasets - symbols and federal interest rates - are not partitioned, due to their small size.

### Configuration and running
A sample configuration (`sample_config.ini`) file is included with the project, and must be filled before running the ETL pipeline. The script is written in python3, and it is expected that it will run in a Spark cluster.