package com.availity.spark.provider

import org.apache.spark.sql.functions.{col, date_format, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProviderRoster {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[5]").appName("Provider Visit").getOrCreate()

    // Read Input(Provider and visits)

    val provider = spark.read
      .option("header", true)
      .option("sep", "|")
      .csv("C:\\Users\\19136\\Downloads\\providers.csv")
    provider.printSchema()

    val visits = spark.read
      .csv("C:\\Users\\19136\\Downloads\\visits.csv")
      .toDF("visit_id", "provider_id", "date_of_service")
    visits.printSchema()

    // Write the output in Json

    // Task 1 no_of_visits_per_provider
    no_of_visits_per_provider(visits, provider).write
      .mode("overwrite")
      .partitionBy("provider_specialty")
      .json("C:\\Users\\19136\\Downloads\\no_of_visits_per_provider")

    spark.read.json("C:\\Users\\19136\\Downloads\\no_of_visits_per_provider").show(100)

    // Task 2 no_of_visits_per_provider_per_month
    no_of_visits_per_provider_per_month(visits).write
      .mode("overwrite")
      .json("C:\\Users\\19136\\Downloads\\no_of_visits_per_provider_per_month")
    spark.read.json("C:\\Users\\19136\\Downloads\\no_of_visits_per_provider_per_month").show(100)
  }
  def no_of_visits_per_provider(visits: DataFrame, provider: DataFrame): DataFrame = {
    /*
    Given the two data datasets, calculate the total number of visits per provider.
    The resulting set should contain the provider's ID, name, specialty, along with the number of visits.
    Output the report in json, partitioned by the provider's specialty.
    */

    val noOfVisitsDf = visits
      .groupBy("provider_id")
      .count()
      .withColumnRenamed("count", "no_of_visits")

    val result1Df = noOfVisitsDf
      .join(provider, Seq("provider_id"))
      .selectExpr("provider_id", "concat(first_name,' ',middle_name,' ',last_name) as name", "provider_specialty", "no_of_visits")
    result1Df

  }

  def no_of_visits_per_provider_per_month(visits: DataFrame): DataFrame = {
    /*
      Given the two datasets, calculate the total number of visits per provider per month.
      The resulting set should contain the provider's ID, the month, and total number of visits.
      Output the result set in json.
    */
    val monthlyVisitsDf = visits
      .withColumn("month_of_service", date_format(to_date(col("date_of_service"), "yyyy-MM-dd"), "MMMM"))

    val result2Df = monthlyVisitsDf
      .groupBy("provider_id", "month_of_service")
      .count()
      .selectExpr("provider_id","month_of_service","count as no_of_visits_per_month")
    result2Df
  }
}
