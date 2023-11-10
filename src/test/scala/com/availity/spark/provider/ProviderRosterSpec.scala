package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach: Unit = {
    spark = SparkSession.builder().master("local[5]").appName("Provider Visit").getOrCreate()

  }

  describe("no_of_visits_per_provider") {
    it("should return the visit count as 26 and name as Tiara A Leannon for provider_id=46275 ") {
      val p = spark.read
        .option("header", true)
        .option("sep", "|")
        .csv("C:\\Users\\19136\\Downloads\\providers.csv")


      val v = spark.read
        .csv("C:\\Users\\19136\\Downloads\\visits.csv")
        .toDF("visit_id", "provider_id", "date_of_service")


      val output = ProviderRoster.no_of_visits_per_provider(v,p).filter("provider_id=46275 ")
        .select("no_of_visits", "name").collectAsList()
      val actualNoOfVisits = output.get(0)(0).toString
      val actualName = output.get(0)(1).toString
      val expectedNoOfVisits = "26"
      val expectedName = "Tiara A Leannon"
      assert(actualNoOfVisits == expectedNoOfVisits && actualName == expectedName)
    }
  }
  describe("no_of_visits_per_month_per_provider") {
    it("should return 1 row and the visit count as 1 for provider_id=46275 and month_of_service='December'") {
      val v = spark.read
        .csv("C:\\Users\\19136\\Downloads\\visits.csv")
        .toDF("visit_id", "provider_id", "date_of_service")


      val output = ProviderRoster.no_of_visits_per_provider_per_month(v).filter("provider_id=46275 and month_of_service='December'")
        .select("no_of_visits_per_month")
      val actual_output = output.collectAsList().get(0)(0).toString
      val actual_count = output.count()
      val expectedOutput = "1"
      assert(actual_output == expectedOutput && actual_count == 1)
    }
  }
}
