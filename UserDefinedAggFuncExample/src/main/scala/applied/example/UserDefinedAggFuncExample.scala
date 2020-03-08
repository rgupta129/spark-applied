package applied.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructField, StructType}

/**
  * UDAF Example to find limited number of highest values
  */
class UserDefinedAggFuncExample(sizeLimit: Int = 3) extends UserDefinedAggregateFunction {

  // A StructType represents data types of input arguments of this aggregate function.
  override def inputSchema: StructType = StructType(Array(StructField("in_value", IntegerType)))

  // A StructType represents data types of values in the aggregation buffer.
  override def bufferSchema: StructType = StructType(Array(StructField("buf_values", ArrayType(IntegerType))))

  // The DataType of the returned value of this UserDefinedAggregateFunction.
  override def dataType: DataType = ArrayType(IntegerType)

  // Returns true iff this function is deterministic
  override def deterministic: Boolean = true

  // Initializes the given aggregation buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Seq[Int]())
  }

  // A private method to update array with possibly higher value
  private[this] def getUpdatedValues(currValues: Array[Int], value: Int) = {
    if (currValues.size < sizeLimit) {
      currValues :+ value // append
    } else {
      val minValue = currValues.min
      if (minValue > value) {
        currValues // no updates
      } else {
        val idx = currValues.indexOf(minValue)
        currValues(idx) = value // replace first min value
        currValues
      }
    }
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currValues = buffer.getAs[Seq[Int]](0).toArray
    val value = input.getAs[Int](0)
    val updatedValues = getUpdatedValues(currValues, value)
    buffer.update(0, updatedValues.toSeq)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buff1Values = buffer1.getAs[Seq[Int]](0).toArray
    val buff2Values = buffer2.getAs[Seq[Int]](0).toArray
    val updatedValues = buff1Values.foldLeft(buff2Values)((acc, value) => getUpdatedValues(acc, value))
    buffer1.update(0, updatedValues.toSeq)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Seq[Int]](0).sorted
  }
}
