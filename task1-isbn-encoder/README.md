# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution
In my solution I first filter out records having valid ISBN using a regex expression.
Run map function on theses records to create 3 more records as per the ISBN specifications.
Union of the two dataframes.

**Advantages:**
Leveraged Dataframes and its functions 

**Disadvantages:**
Can we done by the explode function as per our discussion.

__TODO:__ Please explain your solution briefly and highlight the advantages and disadvantages of your implementation.

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
