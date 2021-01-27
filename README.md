# spark-script-to-calculate-currency

Read the JSON files in the subfolders of data directory

      ```
      scala> val df1 = sqlContext.read.json("/Users/khan/zervant/*/*.json")
      df1: org.apache.spark.sql.DataFrame = [count: bigint, currency: string ... 2 more fields]
      ```

Read the csv file for exchange rates of currency

      scala> val e_rate = spark.read.format("csv").option("header", "true").load("/Users/khan/zervant/exchange_rate.csv")
      e_rate: org.apache.spark.sql.DataFrame = [INSERTTIME: string, EXCAHNGERATE: string ... 1 more field]
      
Display the rates for individual days at different timestamps

      scala> e_rate.show()
      +-------------------+------------+--------+
      |INSERTTIME         |EXCAHNGERATE|CURRENCY|
      +-------------------+------------+--------+
      |2021-01-01 00:01:38|    0.731422|     GBP|
      |2021-01-01 00:00:57|    0.731422|     GBP|
      |2021-01-02 00:00:18|    0.731368|     GBP|
      |2021-01-03 00:00:31|    0.731368|     GBP|
      |2021-01-03 00:00:06|    0.731368|     GBP|
      |2021-01-02 00:00:26|    0.731368|     GBP|
      |2021-01-04 00:00:48|    0.730922|     GBP|
      |2021-01-04 00:04:46|    0.730922|     GBP|
      |2021-01-05 00:00:17|    0.736865|     GBP|
      |2021-01-07 00:00:36|    0.733994|     GBP|
      |2021-01-07 00:00:01|    0.733994|     GBP|
      |2021-01-06 00:01:59|    0.733808|     GBP|
      |2021-01-06 00:01:20|    0.733808|     GBP|
      |2021-01-05 00:00:42|    0.736865|     GBP|
      |2021-01-08 00:00:04|    0.736966|     GBP|
      |2021-01-08 00:00:31|    0.736966|     GBP|
      |2021-01-09 00:01:00|    0.737247|     GBP|
      |2021-01-10 00:01:27|    0.737165|     GBP|
      |2021-01-10 00:00:00|    0.737164|     GBP|
      |2021-01-09 00:02:30|    0.737247|     GBP|
      +-------------------+------------+--------+
      only showing top 20 rows
      
Aggregate exchange rates by dates for differeent curriencies

      scala> val exchange_rate = e_rate.groupBy($"CURRENCY",$"INSERTTIME".cast("date").as("INSERTTIME")).agg(mean("EXCAHNGERATE").alias("EXCAHNGERATE")).orderBy("INSERTTIME")
      exchange_rate: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [CURRENCY: string, INSERTTIME: date ... 1 more field]
  
Display the aggregated exchange rates by dates

      scala> exchange_rate.show()
      +--------+----------+------------+
      |CURRENCY|INSERTTIME|EXCAHNGERATE|
      +--------+----------+------------+
      |     GBP|2021-01-01|    0.731422|
      |     EUR|2021-01-01|    0.818632|
      |     EUR|2021-01-02|    0.822681|
      |     GBP|2021-01-02|    0.731368|
      |     EUR|2021-01-03|    0.824063|
      |     GBP|2021-01-03|    0.731368|
      |     GBP|2021-01-04|    0.730922|
      |     EUR|2021-01-04|    0.816323|
      |     GBP|2021-01-05|    0.736865|
      |     EUR|2021-01-05|    0.816154|
      |     EUR|2021-01-06|    0.813107|
      |     GBP|2021-01-06|    0.733808|
      |     GBP|2021-01-07|    0.733994|
      |     EUR|2021-01-07|    0.810465|
      |     EUR|2021-01-08|    0.815052|
      |     GBP|2021-01-08|    0.736966|
      |     EUR|2021-01-09|    0.817996|
      |     GBP|2021-01-09|    0.737247|
      |     EUR|2021-01-10|    0.817996|
      |     GBP|2021-01-10|   0.7371645|
      +--------+----------+------------+
      only showing top 20 rows
 
Make Temporary views of dataframes

      scala> df.createOrReplaceTempView("df1")
      scala> exchange_rate.createOrReplaceTempView("df2")
      
Convert the values of EUR and GBP to USD and make a "Converted" Column to store the values in USD

      val result = spark.sql("""
      select count, 'USD' as currency, date, value,
          value * coalesce(
              (select min(df2.EXCAHNGERATE)
               from df2
               where df1.date = df2.INSERTTIME and df1.currency = df2.CURRENCY),
              1  -- use 1 as exchange rate if no exchange rate found
          ) as converted
      from df1
      """)
      result: org.apache.spark.sql.DataFrame = [count: bigint, currency: string ... 3 more fields]
 
 Display the converted values
 
      scala> result.show()
      +-----+--------+----------+-----+------------------+
      |count|currency|      date|value|         converted|
      +-----+--------+----------+-----+------------------+
      |    3|     USD|2021-01-14|    4|2.9311893333333336|
      |  102|     USD|2021-01-14|    3|               3.0|
      |  234|     USD|2021-01-14|    5| 3.663986666666667|
      |   68|     USD|2021-01-14|    6| 4.933771999999999|
      |   20|     USD|2021-01-14|    1|0.7327973333333334|
      |   28|     USD|2021-01-14|    5| 3.663986666666667|
      |   48|     USD|2021-01-14|    7|               7.0|
      |   33|     USD|2021-01-14|    2|1.6445906666666665|
      |  106|     USD|2021-01-14|   10| 7.327973333333334|
      |   97|     USD|2021-01-14|    3|          2.198392|
      |   84|     USD|2021-01-14|    5| 3.663986666666667|
      |  172|     USD|2021-01-14|    3|               3.0|
      |   79|     USD|2021-01-14|    4| 3.289181333333333|
      |  140|     USD|2021-01-14|    2|1.4655946666666668|
      |   50|     USD|2021-01-14|   10| 7.327973333333334|
      |  118|     USD|2021-01-14|    2|1.4655946666666668|
      |   31|     USD|2021-01-14|    3|               3.0|
      |  233|     USD|2021-01-14|    7|               7.0|
      |  101|     USD|2021-01-14|    9|          6.595176|
      |  181|     USD|2021-01-14|    8| 5.862378666666667|
      +-----+--------+----------+-----+------------------+
      only showing top 20 rows
      
 Takes data for the last 3 available days, finds mean value in USD
 
      scala> val usd_mean = result.withColumn("rank", dense_rank().over(Window.partitionBy("currency").orderBy(desc("date")))).filter("rank <= 3 and currency = 'USD'").groupBy("date").agg(mean("converted"))
      usd_mean: org.apache.spark.sql.DataFrame = [date: string, avg(converted): double]
  
 Display the mean value in USD for the last 3 days
 
      scala> usd_mean.show()
      +----------+-----------------+
      |      date|   avg(converted)|
      +----------+-----------------+
      |2021-01-15|4.603359414062508|
      |2021-01-13|4.314121513671881|
      |2021-01-14|4.552833897135422|
      +----------+-----------------+
      
      
      
      
