from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from os import path
from glob import glob
import configparser


def create_spark_session(aws_access_key: str, aws_secret_key: str):
    """
    Creates or retrieves an existing local spark session.

    :param aws_access_key: AWS access key - must be able to write to the S3 bucket used in this script.
    :param aws_secret_key: secret key corresponding to the AWS access key.
    :return: spark session configured to use S3 buckets.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.conf.set("spark.speculation", "false")

    # Add AWS access keys in order to be able to write to S3
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

    return spark


def load_stock_symbols(spark: SparkSession, input_path: str, bucket: str):
    """
    Loads stock symbols, their description, and respective Stock Exchange (based on the file name) to S3.

    :param spark: spark session - must be configured to use S3 (AWS credentials, file system)
    :param input_path: input path from where the files should be read.
    :param bucket: S3 bucket where to write data
    :return: None
    """

    # Read each symbol file separately, since the name of the file is the exchange on which the symbol is published
    schema = StructType([
        StructField("Symbol", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("exchange", StringType(), True)
    ])

    df_all_symbols = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    for file in glob(path.join(input_path, '*.csv')):
        print(f'Now processing {file}')
        exchange_name = path.basename(path.splitext(file)[0])
        df_symbols = spark.read.format("csv").option("header", "true").load(file)
        df_symbols = df_symbols.withColumn('exchange', functions.lit(exchange_name))

        df_all_symbols = df_all_symbols.union(df_symbols)

    df_all_symbols \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .csv(f"s3a://{bucket}/data/symbols")


def load_stock_values(spark: SparkSession, input_path: str, start_date: str, end_date: str, bucket: str) -> None:
    """
    Load stocks from S&P 500 companies to S3.

    :param spark: spark session - must be configured to use S3 (AWS credentials, file system)
    :param input_path: input path from where the files should be read.
    :param start_date: start date for filtering entries
    :param end_date: end date for filtering entries
    :param bucket: S3 bucket where to write data
    :return: None
    """
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("volume", IntegerType(), True),
        StructField("open", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("adjclose", DoubleType(), True),
        StructField("Symbol", StringType(), True)
    ])

    df_all_trades = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    # Parse each file separately, because the file name is the symbol name
    for file in glob(path.join(input_path, '*.csv')):
        print(f'Now processing {file}')
        symbol_name = path.basename(path.splitext(file)[0])
        trade_df = spark.read.format("csv").option("header", "true").load(file)
        trade_df = trade_df.withColumn('Symbol', functions.lit(symbol_name))

        df_all_trades = df_all_trades.union(trade_df)

    # Filter date
    df_all_trades = df_all_trades \
        .filter(functions.to_date(df_all_trades['date'], format=None) > functions.lit(start_date))

    df_all_trades = df_all_trades \
        .filter(functions.to_date(df_all_trades['date'], format=None) < functions.lit(end_date))

    # Create diff column, containing the difference between adjusted close and open price
    df_all_trades = df_all_trades.withColumn('diff', df_all_trades['adjclose'] - df_all_trades['open'])

    # Add partitioning columns (year, month, day) in order to be able to easily trigger parametrized airflow tasks
    df_all_trades = df_all_trades.withColumn('year', functions.year(df_all_trades['date']))
    df_all_trades = df_all_trades.withColumn('month', functions.month(df_all_trades['date']))
    df_all_trades = df_all_trades.withColumn('day', functions.dayofmonth(df_all_trades['date']))

    # Sort according to diff (descending), in order to easily select the top winners for each day
    df_all_trades = df_all_trades.orderBy('diff', ascending=False)

    df_all_trades \
        .coalesce(1) \
        .write \
        .partitionBy('year', 'month', 'day') \
        .mode('overwrite') \
        .csv(f"s3a://{bucket}/data/stocks")


def load_federal_interest_rates(spark: SparkSession, input_path: str, start_date: str, end_date: str, bucket: str) \
        -> None:
    """
    Loads the effective federal funds rate to S3.

    :param spark: spark session - must be configured to use S3 (AWS credentials, file system)
    :param input_path: input path from where the files should be read.
    :param start_date: start date for filtering entries
    :param end_date: end date for filtering entries
    :param bucket: S3 bucket where to write data
    :return: None
    """
    df_interest_rates = spark.read.option("header", "true").csv(input_path)

    # Select only relevant columns
    df_interest_rates = df_interest_rates.select('Year', 'Month', 'Day', 'Effective Federal Funds Rate')

    # Rename columns to remain consistent with the other dataframes
    df_interest_rates = df_interest_rates \
        .withColumnRenamed('Year', 'year') \
        .withColumnRenamed('Month', 'month') \
        .withColumnRenamed('Day', 'day') \
        .withColumnRenamed('Effective Federal Funds Rate', 'rate')

    # Select only columns containing effective rates
    df_interest_rates = df_interest_rates.where(functions.col('rate').isNotNull())

    # Add timestamp column, with the same format as other data frames
    df_interest_rates = df_interest_rates.withColumn('date',
                                                     functions.concat(functions.col('year'), functions.lit('-'),
                                                                      functions.col('month'), functions.lit('-'),
                                                                      functions.col('day')))

    # Filter according to start and end date
    df_interest_rates = df_interest_rates.filter(
        functions.to_date(df_interest_rates['date'], format=None) > functions.lit(start_date))

    df_interest_rates = df_interest_rates.filter(
        functions.to_date(df_interest_rates['date'], format=None) < functions.lit(end_date))

    # Due to being a small table, interest rates are not partitioned
    df_interest_rates \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .csv(f"s3a://{bucket}/data/rates")


def load_world_news(spark: SparkSession, input_path: str, bucket: str) -> None:
    """
    Loads the world news to S3. There are 25 news pieces per day, which are concatenated in a single row, using the
    asterisk (*) as a separator.

    :param spark: spark session - must be configured to use S3 (AWS credentials, file system)
    :param input_path: input path from where the files should be read.
    :param bucket: S3 bucket where to write data
    :return: None
    """
    df_world_news = spark.read.option("header", "true").option("delimiter", ",").csv(input_path)

    df_world_news = df_world_news.groupBy('Date').agg(
        functions.concat_ws('*', functions.collect_list(functions.col('News'))).alias('news'))

    # Filter out badly formatted Date columns
    df_world_news = df_world_news \
        .filter(functions.to_date(functions.col('Date')).isNotNull()) \
        .orderBy('Date', ascending=False)

    df_world_news = df_world_news.withColumn('year', functions.year('Date'))

    df_world_news \
        .coalesce(1) \
        .write \
        .partitionBy('year') \
        .mode('overwrite') \
        .csv(f"s3a://{bucket}/data/news")


def run_etl():
    config = configparser.ConfigParser()
    config.read('config.ini')

    aws_access_key = config['AWS']['access_key']
    aws_secret_key = config['AWS']['secret_key']

    bucket = config['S3']['bucket']

    start_date = config['Data']['start_date']
    end_date = config['Data']['end_date']

    input_path_stocks = config['Data']['input_path_s_and_p']
    input_path_symbols = config['Data']['input_path_symbols']
    input_path_interest_rates = config['Data']['input_path_interest_rates']
    input_path_world_news = config['Data']['input_path_world_news']

    spark_session = create_spark_session(aws_access_key, aws_secret_key)

    print("Loading stocks...")
    load_stock_values(spark_session, input_path_stocks, start_date, end_date, bucket)
    print("...done")

    print("Loading symbols...")
    load_stock_symbols(spark_session, input_path_symbols, bucket)
    print("...done")

    print("Loading world news...")
    load_world_news(spark_session, input_path_world_news, bucket)
    print("...done")

    print("Loading federal interest rates...")
    load_federal_interest_rates(spark_session, input_path_interest_rates, start_date, end_date, bucket)
    print("...done")


if __name__ == "__main__":
    run_etl()
