# spark_input_lib
A useful tool for reading data with Spark.

It contains `it.mgaido.spark.io.FileWithHeaderReader` class which allows to read files withan header ignoring it. 
It requires to set properly `it.mgaido.spark.io.InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES` property in Hadoop job configuration with the 
number of lines each file contains as header.

This class doesn't work fine if the header is spread over multiple block, but this should not happen.... In such a case
only the lines in the first block are discarded.

Scala object `it.mgaido.spark.io.IOHelper` performs the same operation reading a path and returning a `RDD` of strings
without the header lines.


