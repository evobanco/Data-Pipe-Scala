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

  val FieldName = "Name"
  val FieldPart = "Part"
  val FieldUnixTimestamp = "UnixTimestamp"
  val FieldAggregation = "Aggregation"
  val FieldValue = "Value"
  val FieldmType = "mType"
  val FieldTest = "Test"
  val FieldFeatures  = "Features"
  val auxFieldRowDesc = "rowDesc"
  val auxFieldRowAsc = "rowAsc"
  /**
    * Pivots machine data
    *
    * @return machine data pivoted
    */
  def getTests(): DataFrame = {

    val dfWithFirstAndLastExecutions = getFirstAndLastExecutions(df)

    val dfMeasures = getMeasures(dfWithFirstAndLastExecutions)

    val dfTest = getTests(dfWithFirstAndLastExecutions)

    dfTest.join(dfMeasures,Seq(FieldPart,FieldAggregation))

  }

  def getFirstAndLastExecutions(df: DataFrame): DataFrame = {

    val windowPartitionByNameAndPart = Window.partitionBy(FieldName,FieldPart)

    val dfWithFirstExecutions = df.withColumn(auxFieldRowDesc, min(col(FieldUnixTimestamp)).over(windowPartitionByNameAndPart))
      .withColumn(FieldAggregation, lit(AggFirst))
      .where( col(auxFieldRowDesc) === col(FieldUnixTimestamp))
      .drop(auxFieldRowDesc)


    val dfWithLastExecutions = df.withColumn(auxFieldRowAsc, max(col(FieldUnixTimestamp)).over(windowPartitionByNameAndPart))
      .withColumn(FieldAggregation, lit(AggLast))
      .where(col(auxFieldRowAsc) === col(FieldUnixTimestamp))
      .drop(auxFieldRowAsc)

    dfWithFirstExecutions.union(dfWithLastExecutions)

  }

  def getMeasures(dfWithFirstAndLastExecutions: DataFrame): DataFrame = {

    dfWithFirstAndLastExecutions.filter(col(FieldmType) === TypeMeasurement )
      .select(FieldName,FieldPart,FieldValue, FieldAggregation)
      .withColumn(FieldFeatures, struct(col(FieldName), col(FieldValue)))
      .groupBy(col(FieldPart),col(FieldAggregation))
      .agg(collect_list(FieldFeatures).as(FieldFeatures))

  }

  def getTests(dfWithFirstAndLastExecutions: DataFrame): DataFrame = {

    dfWithFirstAndLastExecutions.filter(col(FieldmType) === TypeTest )
      .withColumnRenamed(FieldName, FieldTest)
      .drop(FieldValue,FieldmType,FieldUnixTimestamp)

  }
}