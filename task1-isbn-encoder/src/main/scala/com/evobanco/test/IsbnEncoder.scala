package com.evobanco.test

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{explode, substring, _}

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}
class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  val REGEXP_ISBN_VALIDATION="^(?:ISBN(?:-13)?:?\\ )([0-9]{3}-[0-9]{10}$)"
  val ISBN_FIELD_NAME="isbn"
  val NAME_FIELD_NAME="name"
  val YEAR_FIELD_NAME="year"
  val EMPTY_ISBN=""

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {

    def dfFormatKoIsbn = df.filter(!col(ISBN_FIELD_NAME).rlike(REGEXP_ISBN_VALIDATION))

    def dfFormatOkIsbn= df.filter(col(ISBN_FIELD_NAME).rlike(REGEXP_ISBN_VALIDATION))

    getDfOkResult(dfFormatOkIsbn).union(getDfKoResult(dfFormatKoIsbn))

  }

  def getDfOkResult(inputDf: Dataset[Row]) : DataFrame = {

    inputDf.
      select(col(NAME_FIELD_NAME), col(YEAR_FIELD_NAME),
        explode(
          array(
            col(ISBN_FIELD_NAME),
            concat(lit("ISBN-EAN: "),substring(col(ISBN_FIELD_NAME),7,3)),
            concat(lit("ISBN-GROUP: "),substring(col(ISBN_FIELD_NAME),11,2)),
            concat(lit("ISBN-PUBLISHER: "),substring(col(ISBN_FIELD_NAME),13,4)),
            concat(lit("ISBN-TITLE: "),substring(col(ISBN_FIELD_NAME),17,3))
          )
        ).as(ISBN_FIELD_NAME)
      )

  }

  def getDfKoResult(inputDf: Dataset[Row]) : DataFrame = {

     inputDf.select(col(NAME_FIELD_NAME), col(YEAR_FIELD_NAME), col(ISBN_FIELD_NAME))

  }

}
