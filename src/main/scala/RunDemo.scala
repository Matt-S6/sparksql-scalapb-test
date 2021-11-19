package myexample

import com.example.protos.demo._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scalapb.spark.Implicits._
import scalapb.spark.ProtoSQL

object RunDemo {

  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()

    val sc = spark.sparkContext

    val personsDF: DataFrame = ProtoSQL.createDataFrame(spark, testData)

    val personsDS1: Dataset[Person] = personsDF.as[Person]

    val personsDS2: Dataset[Person] = spark.createDataset(testData)

    personsDS1.show()

    personsDS2.show()

    personsDF.createOrReplaceTempView("persons")

    spark.sql("SELECT name, age, gender, size(addresses) FROM persons").show()

    spark.sql("SELECT name, age, gender, size(addresses) FROM persons WHERE age > 30")
      .collect
      .foreach(println)
  }

  val OptionalString = (s:String) => Some(NullableString(s))

  val testData: Seq[Person] = Seq(
    Person().update(
      _.name.value := "Joe",
      _.age.value := 32,
      _.gender := Gender.MALE),
    Person().update(
      _.name.value := "Mark",
      _.age.value := 21,
      _.gender := Gender.MALE,
      _.addresses := Seq(
          Address(city = OptionalString("San Francisco"), street=OptionalString("3rd Street"))
      )),
    Person().update(
      _.name.value := "Steven",
      _.gender := Gender.MALE,
      _.addresses := Seq(
          Address(city = OptionalString("San Francisco"), street=OptionalString("5th Street")),
          Address(city = OptionalString("Sunnyvale"), street=OptionalString("Wolfe"))
      )),
    Person().update(
      _.name.value := "Batya",
      _.age.value := 11,
      _.gender := Gender.FEMALE))
}
