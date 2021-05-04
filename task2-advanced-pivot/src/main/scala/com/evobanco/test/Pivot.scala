package com.evobanco.test

import com.evobanco.test.Pivot.{AggFirst, AggLast, TypeMeasurement, TypeTest}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
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

    /**
     * Identificar las rows del primer y ultimo test
     */

    val window = Window.partitionBy("part", "name")

    val df_first_last =
      df.withColumn("rank", row_number().over(window.orderBy("unixTimestamp")))
        .filter(col("rank").equalTo(lit("1")))
        .withColumn("aggregation", lit(AggFirst))
        .drop("rank")
        .union(df.withColumn("rank", row_number().over(window.orderBy(col("unixTimestamp").desc)))
              .filter(col("rank").equalTo(lit("1")))
              .withColumn("aggregation", lit(AggLast))
              .drop("rank"))

    /**
     * Agrupar las mediciones de cada test
     */

    val df_measurement =
      df_first_last.filter(col("mType")===TypeMeasurement)
      .groupBy("part", "aggregation")
      .agg(collect_list(struct("name", "value")).as("features"))

    /**
     * Unir las mediciones con los test
     */

    df_first_last.filter(col("mType")===TypeTest)
        .join(df_measurement, Seq("part", "aggregation"))
        .select(
          col("part"),
          col("location"),
          col("name").as("test"),
          col("testResult"),
          col("aggregation"),
          col("features"))

  }

}