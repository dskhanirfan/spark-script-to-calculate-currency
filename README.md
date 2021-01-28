# spark-shell-scala-script-to-Aggregate-currency-Average-on-Daily-Routine

**Read the JSON files in the subfolders of data directory**
     
      scala> val df1 = sqlContext.read.json("/Users/khan/zervant/*/*.json")
      df1: org.apache.spark.sql.DataFrame = [count: bigint, currency: string ... 2 more fields]
      
**Read the csv file for exchange rates of currency**

      scala> val e_rate = spark.read.format("csv").option("header", "true").load("/Users/khan/zervant/exchange_rate.csv")
      e_rate: org.apache.spark.sql.DataFrame = [INSERTTIME: string, EXCAHNGERATE: string ... 1 more field]
      
**Display the rates for individual days at different timestamps**

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
      
**Aggregate exchange rates by dates for differeent curriencies**

      scala> val exchange_rate = e_rate.groupBy($"CURRENCY",$"INSERTTIME".cast("date").as("INSERTTIME")).agg(mean("EXCAHNGERATE").alias("EXCAHNGERATE")).orderBy("INSERTTIME")
      exchange_rate: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [CURRENCY: string, INSERTTIME: date ... 1 more field]
  
**Display the aggregated exchange rates by dates**

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
 
**Make Temporary views of dataframes**

      scala> df.createOrReplaceTempView("df1")
      scala> exchange_rate.createOrReplaceTempView("df2")
      
**Convert the values of EUR and GBP to USD and make a "Converted" Column to store the values in USD** 
*correlated subquery (a fancy way of doing joins)*

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
 
 **Display the converted values**
 
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
      
      
 **Multiply converted values and count to get exact valuation in mul column**
      
      scala> val countmulval = result.withColumn("mul", result("count")*result("converted"))
      countmulval: org.apache.spark.sql.DataFrame = [count: bigint, currency: string ... 4 more fields]
      scala> countmulval.show()
     +-----+--------+----------+-----+------------------+------------------+         
     |count|currency|      date|value|         converted|               mul|
     +-----+--------+----------+-----+------------------+------------------+
     |    3|     USD|2021-01-14|    4|2.9311893333333336|          8.793568|
     |  102|     USD|2021-01-14|    3|               3.0|             306.0|
     |  234|     USD|2021-01-14|    5| 3.663986666666667| 857.3728800000001|
     |   68|     USD|2021-01-14|    6| 4.933771999999999|        335.496496|
     |   20|     USD|2021-01-14|    1|0.7327973333333334|14.655946666666669|
     |   28|     USD|2021-01-14|    5| 3.663986666666667|102.59162666666668|
     |   48|     USD|2021-01-14|    7|               7.0|             336.0|
     |   33|     USD|2021-01-14|    2|1.6445906666666665|54.271491999999995|
     |  106|     USD|2021-01-14|   10| 7.327973333333334| 776.7651733333335|
     |   97|     USD|2021-01-14|    3|          2.198392|213.24402400000002|
     |   84|     USD|2021-01-14|    5| 3.663986666666667|307.77488000000005|
     |  172|     USD|2021-01-14|    3|               3.0|             516.0|
     |   79|     USD|2021-01-14|    4| 3.289181333333333|259.84532533333334|
     |  140|     USD|2021-01-14|    2|1.4655946666666668|205.18325333333337|
     |   50|     USD|2021-01-14|   10| 7.327973333333334| 366.3986666666667|
     |  118|     USD|2021-01-14|    2|1.4655946666666668| 172.9401706666667|
     |   31|     USD|2021-01-14|    3|               3.0|              93.0|
     |  233|     USD|2021-01-14|    7|               7.0|            1631.0|
     |  101|     USD|2021-01-14|    9|          6.595176|        666.112776|
     |  181|     USD|2021-01-14|    8| 5.862378666666667|1061.0905386666668|
     +-----+--------+----------+-----+------------------+------------------+
     only showing top 20 rows

      
 **Takes data for the last 3 available days, finds mean value in USD**
 
      scala> val usd_mean = countmulval.withColumn("rank", dense_rank().over(Window.partitionBy("currency").orderBy(desc("date")))).filter("rank <= 3 and currency = 'USD'").groupBy("date").agg(mean("mul"))
      usd_mean: org.apache.spark.sql.DataFrame = [date: string, avg(converted): double]
  
 **Display the mean value in USD for the last 3 days**
 
      scala> usd_mean.show()
     +----------+-----------------+                                                  
     |      date|         avg(mul)|
     +----------+-----------------+
     |2021-01-15|593.2859392031252|
     |2021-01-13|552.4245816640627|
     |2021-01-14|613.6391253125003|
     +----------+-----------------+
      
      
 ## Potential Improvements
 > Automatically create spark cluster with script, build and deploy spark job with a CI/CD pipeline from code repo. When job completes terminate cluster
 
 
 > creating a aws emr/ or databricks cluster with script. Run job that reads data from S3 or blob storage and then store results back and kill cluster
      
