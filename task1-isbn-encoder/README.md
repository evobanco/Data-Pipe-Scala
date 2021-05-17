# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution
In my solution I've defined an UDF function to get each part from a ISBN code using regex and matching groups. 
It creates a column containing an array with the ISBN parts (if the ISBN code matches the regex) and the original ISBN field value.

The UDF function is run on the original DataFrame, and then the UDF column is exploded to obtain a row for each value in its array.

**Advantages:**

Leveraged DataFrame and its functions. 

It works with the test and README formats (978-1449358624 or 9781449358624)

**Disadvantages:**

UDF functions may have worst performance results than Spark functions.

Due to the flexibility of the 13-digit ISBN code format, not all the possible formats are taking into account because of the variability of the groups lenghts. 
A very complex regex shoud be used to fix this.


### Example

#### Input

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| EL NOMBRE DE LA ROSA      | 2015 | ISBN: 9788426403568 |

#### Output

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| EL NOMBRE DE LA ROSA      | 2015 | ISBN: 9788426403568 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-EAN: 978 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-GROUP: 84 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-PUBLISHER: 2640 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-TITLE: 356 |
