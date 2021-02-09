# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution by Santiago Pinto Castro
In the solution, I first separate the data into two different dataframes according to which of they match the correct 
format of the ISBN field or not. In the case of the rows that had a valid format of the ISBN field, the substring 
corresponding to each one of the parts that compouse the ISBN was exploited. For non-compliant rows, the input row was 
returned without any additional operations. The result is the union between the two dataframes.

**Advantages:**
Leveraged Dataframes and its functions 

**Disadvantages:**
Explode a large amount of data could cause performance problems, maybe a better solution is change spark SQL by 
streams and use flatmap to generate the new records.

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
