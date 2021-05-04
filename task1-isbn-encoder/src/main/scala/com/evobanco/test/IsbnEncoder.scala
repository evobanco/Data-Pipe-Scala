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

    /**
     * Step 1: Filtrar las filas con ISBNs bien formados
     */

    val regexISBN = "^(?:ISBN(?:-13)?:?\\ )?(?=[0-9]{3}-[0-9]{10}$|(?=(?:[0-9]+[-\\ ]){4})[-\\ 0-9]{17}$)97[89][-\\ ]?[0-9]{1,5}[-\\ ]?[0-9]+[-\\ ]?[0-9]+[-\\ ]?[0-9]$"

    val df_isbn = df.filter(col("isbn").rlike(regexISBN))

    /**
     * Step 2: Transformacion de los datos: Explode del isbn
     */

    val df_isbn_explode =
      df_isbn.select(
        col("name"),
        col("year"),
        explode(array(
          concat(lit("ISBN-EAN: "),substring(col("isbn"),7,3)),
          concat(lit("ISBN-GROUP: "),substring(col("isbn"),11,2)),
          concat(lit("ISBN-PUBLISHER: "),substring(col("isbn"),13,4)),
          concat(lit("ISBN-TITLE: "),substring(col("isbn"),17,3))
        )).as("isbn"))

    /**
     * Step 3: Union con el df de entrada para no perder isbn malformados
     */

    df_isbn_explode.union(df)

  }
}
