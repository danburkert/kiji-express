package org.kiji.express.flow

import org.kiji.schema._
import org.kiji.express.{EntityId, KijiSlice, KijiSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.kiji.schema.layout.KijiTableLayout
import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration

@RunWith(classOf[JUnitRunner])
class AvroSourceTypesSuite extends KijiClientTest with KijiSuite with BeforeAndAfter {
  setupKijiTest()
  val kiji: Kiji = createTestKiji()
  val layout: KijiTableLayout = layout("layout/avro-types-complete.json")
  val table: KijiTable = {
    kiji.createTable(layout.getDesc)
    kiji.openTable(layout.getName)
  }
  val conf: Configuration = getConf
  val uri = table.getURI.toString
  val reader: KijiTableReader = table.openTableReader()
  val writer: KijiTableWriter = table.openTableWriter()

  val timestamp = 100L
  val family = "family"

  def entityId(s: String) = table.getEntityId(s)


  def writeValue[T](eid: String, column: String, value: T) = {
    writer.put(entityId(eid), family, column, timestamp, value)
    writer.flush()
  }

  def getValue[T](eid: String, column: String): T = {
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, column))
    require(get.containsCell(family, column, timestamp)) // Require the cell exists for null case
    get.getValue(family, column, timestamp)
  }

  /**
   * Unwraps latest value from [[org.kiji.express.KijiSlice]] and verifies that the type is as
   * expected.
   * @param slice [[org.kiji.express.KijiSlice]] containing value to unwrap.
   * @tparam T expected type of value contained in KijiSlice.
   * @return unwrapped value of type T.
   */
  private def unwrap[T](slice: KijiSlice[T]): (T, Long) = {
    val cell = slice.getFirst()
    (cell.datum, cell.version)
  }

  def testExpressReadWrite[T](column: String, value: T) = {
    val input = column + "-in"
    val output = column + "-out"
    val colfam = "family:" + column
    writeValue(input, column, value)

    class ReadWriteJob(args: Args) extends KijiJob(args) {
      KijiInput(uri, colfam -> 'slice)
        .read
        .mapTo('slice -> ('value, 'time))(unwrap[T])
        .map('value -> 'entityId) { _: T => EntityId(output)}
        .write(KijiOutput(uri, 'time, 'value -> colfam))
    }
    new ReadWriteJob(Args("--hdfs")).run
    assert(value === getValue[T](output, column))
  }

  test("counter type column results in a KijiSlice[Long]") {
    testExpressReadWrite[Long]("counter", 13L)
  }

  test("raw bytes type column results in a KijiSlice[Array[Byte]]") {
    testExpressReadWrite[Array[Byte]]("raw", "Who do voodoo?".getBytes("UTF8"))
  }

  test("null avro type column results in a KijiSlice[Null]") {
    testExpressReadWrite[Null]("null", null)
  }

  test("boolean avro type column results in a KijiSlice[Boolean]") {}
  test("int avro type column results in a KijiSlice[Int]") {}
  test("long avro type column results in a KijiSlice[Long]") {}
  test("float avro type column results in a KijiSlice[Float]") {}
  test("double avro type column results in a KijiSlice[Double]") {}
  test("bytes avro type column results in an KijiSlice[Array[Byte]]") {}
  test("string avro type column results in a KijiSlice[String]") {}
  test("specific record T avro type column results in a KijiSlice[T]") {}
  test("generic record avro type column results in a KijiSlice[GenericRecord]") {}
  test("enum avro type column results in a KijiSlice[String field") {}
  test("array[T] avro type column results in a KijiSlice[List[T]]") {}
  test("map[T] avro type column results in a KijiSlice[Map[String, T]]") {}
  test("union avro type column results in a ??? field") {}
  test("fixed avro type column results in an KijiSlice[Array[Byte]] field") {}

  //strings.foreach { string =>
//val eid = entityId(string.take(10))
//
//writer.put(eid, family, "column1",
//new GenericData.Fixed(column1Schema, string.take(16).getBytes))
//writer.put(eid, family, "column2", ByteBuffer.wrap(string.getBytes))
//writer.put(eid, family, "column3",
//new HashSpec(HashType.MD5, string.sum % string.length + 1, string.length % 2 == 0))
//writer.put(eid, family, "column4",
//new GenericRecordBuilder(column4Schema).set("contained_string", string).build()
//)
}
