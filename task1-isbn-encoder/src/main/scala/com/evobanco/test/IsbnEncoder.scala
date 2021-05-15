package com.evobanco.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {

    val udfGetIsbn = udf((originalISBN: String) => {

      val isbnRegexPattern = "^(?:ISBN(?:-13)?:?\\ )?(?=[0-9]{3}-?[0-9]{10}$)(97[89])[-\\ ]?([0-9]{2})([0-9]{4})([0-9]{3})[0-9]{1}$".r
      (originalISBN match {
        case isbnRegexPattern(ean, group, publisher, title) => List(s"ISBN-EAN: $ean", s"ISBN-GROUP: $group", s"ISBN-PUBLISHER: $publisher",
          s"ISBN-TITLE: $title")
        case _ => List[String]()
      }).:+(originalISBN)
    }
    )

    df.withColumn("isbn", explode(udfGetIsbn(col("isbn"))))
  }
}