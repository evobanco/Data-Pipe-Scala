package com.evobanco.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

object Pivot {
  val TypeMeasurement = "measurement"
  val TypeTest = "test"

  val TestPassed = "passed"
  val TestFailed = "failed"

  val AggFirst = "first"
  val AggLast = "last"

  implicit def pivot(df: DataFrame) = new PivotImplicit(df)
}

class PivotImplicit(df: DataFrame) extends Serializable {

  /**
    * Pivots machine data
    *
    * @return machine data pivoted
    */
  def getTests(): DataFrame = {

    val testDF = df.filter(col("mType") === Pivot.TypeTest).drop("mType","value")

    val testWindow = Window.partitionBy("part","location","name")

    val firstAndLastAggTests = getFirstAndLastAggregation(testDF, testWindow).withColumnRenamed("name","test")

    val measurementWindow = Window.partitionBy("part","name")

    val measurements = getFirstAndLastAggregation(df.filter(col("mType") =!= Pivot.TypeTest), measurementWindow)
      .groupBy("part","aggregation").agg(collect_list(struct("name","value")).as("Features"))

    firstAndLastAggTests.join(measurements, Seq("part", "aggregation"), "left")

  }

  def getFirstAndLastAggregation( df: DataFrame, window: WindowSpec) : DataFrame = {
    df
      .withColumn("maxUnixTimeStamp", max(col("unixTimestamp")).over(window))
      .withColumn("minUnixTimeStamp", min(col("unixTimestamp")).over(window))
      .filter(col("unixTimestamp") === col("minUnixTimeStamp") || col("unixTimestamp") === col("maxUnixTimeStamp"))
      .withColumn("aggregation", explode(
        when(col("maxUnixTimeStamp") === col("minUnixTimeStamp"), array(lit("first"), lit("last")))
          .otherwise(when(col("maxUnixTimeStamp") === col("unixTimestamp"), array(lit("last"))).otherwise(array(lit("first"))))))
    .drop("minUnixTimeStamp", "maxUnixTimeStamp", "unixTimestamp")
  }

}