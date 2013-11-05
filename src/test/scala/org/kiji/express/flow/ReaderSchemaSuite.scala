/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.flow

import com.twitter.scalding.Mode
import com.twitter.scalding.Args
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiClientTest
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite


@RunWith(classOf[JUnitRunner])
class ReaderSchemaSuite extends KijiClientTest with KijiSuite {
  import org.kiji.express.util.AvroTypesComplete._
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

  private def entityId(s: String) = table.getEntityId(s)

  private def writeValue(eid: String, column: String, value: Any) = {
    writer.put(entityId(eid), family, column, value)
    writer.flush()
  }

  private def getValue[T](eid: String, column: String): T = {
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, column))
    require(get.containsColumn(family, column)) // Require the cell exists for null case
    get.getMostRecentValue(family, column)
  }

  private def testExpressReadWrite[T](column: String, value: Any, schema: ReaderSchema,
                                      overrideSchema: Option[Schema] = None) = {
    val readEid = column + "-in"
    val writeEid = column + "-out"
    writeValue(readEid, column, value)

    val outputSchema = if (overrideSchema.isDefined) {
      overrideSchema
    } else {
      schema match {
        case Default => None
        case Specific(klass) => Some(klass.getMethod("getSchema").invoke(klass).asInstanceOf[Schema])
        case Generic(json) => Some(Generic.schema(json))
      }
    }

    val inputCol = QualifiedColumnRequestInput(family, column, schema = schema)
    val outputCol = QualifiedColumnRequestOutput(family, column, outputSchema)

    val args = Args("--hdfs")
    Mode.mode = Mode(args, conf)
    new ReadWriteJob[T](uri, inputCol, outputCol, writeEid, args).run
    assert(value === getValue[T](writeEid, column))
  }

  test("A KijiJob can read a counter column with the default reader schema.") {
    testExpressReadWrite[Long](counterColumn, longs.head, Default)
  }

  test("A KijiJob can read a raw bytes column with the default reader schema.") {
    testExpressReadWrite[Array[Byte]](rawColumn, bytes.head, Default)
  }

  test("A KijiJob can read a null column with the default reader schema.") {
    testExpressReadWrite[Null](nullColumn, null, Default)
  }

  test("A KijiJob can read a null column with a generic reader schema.") {
    testExpressReadWrite[Null](nullColumn, null, Generic(nullSchema))
  }

  test("A KijiJob can read a boolean column with the default reader schema.") {
    testExpressReadWrite[Boolean](booleanColumn, booleans.head, Default)
  }

  test("A KijiJob can read a boolean column with a generic reader schema.") {
    testExpressReadWrite[Boolean](booleanColumn, booleans.head, Generic(booleanSchema))
  }

  test("A KijiJob can read an int column with the default reader schema.") {
    testExpressReadWrite[Int](intColumn, ints.head, Default)
  }

  test("A KijiJob can read an int column with a generic reader schema.") {
    testExpressReadWrite[Int](intColumn, ints.head, Generic(intSchema))
  }

  test("A KijiJob can read a long column with the default reader schema.") {
    testExpressReadWrite[Long](longColumn, longs.head, Default)
  }

  test("A KijiJob can read a long column with a generic reader schema.") {
    testExpressReadWrite[Long](longColumn, longs.head, Generic(longSchema))
  }

  test("A KijiJob can read a float column with the default reader schema.") {
    testExpressReadWrite[Float](floatColumn, floats.head, Default)
  }

  test("A KijiJob can read a float column with a generic reader schema.") {
    testExpressReadWrite[Float](floatColumn, floats.head, Generic(floatSchema))
  }

  test("A KijiJob can read a double column with the default reader schema.") {
    testExpressReadWrite[Double](doubleColumn, doubles.head, Default)
  }

  test("A KijiJob can read a double column with a generic reader schema.") {
    testExpressReadWrite[Double](doubleColumn, doubles.head, Generic(doubleSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can read a bytes column with the default reader schema.") {
    testExpressReadWrite[Array[Byte]](bytesColumn, bytes.head, Default)
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can read a bytes column with a generic reader schema.") {
    testExpressReadWrite[Array[Byte]](bytesColumn, bytes.head, Generic(bytesSchema))
  }

  test("A KijiJob can read a string column with the default reader schema.") {
    testExpressReadWrite[String](stringColumn, strings.head, Default)
  }

  test("A KijiJob can read a string column with a generic reader schema.") {
    testExpressReadWrite[String](stringColumn, strings.head, Generic(stringSchema))
  }

  test("A KijiJob can read a specific record column with the default reader schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head, Default)
  }

  test("A KijiJob can read a specific record column with a generic reader schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head,
        Generic(specificSchema))
  }

  test("A KijiJob can read a generic record column with the default reader schema.") {
    testExpressReadWrite[GenericRecord](genericColumn, genericRecords.head, Default)
  }

  test("A KijiJob can read a generic record column with a generic reader schema.") {
    testExpressReadWrite[GenericRecord](genericColumn, genericRecords.head, Generic(genericSchema))
  }

  test("A KijiJob can read an enum column with the default reader schema.") {
    testExpressReadWrite[GenericEnumSymbol](enumColumn, enums.head, Default, Some(enumSchema))
  }

  test("A KijiJob can read an enum column with a generic reader schema.") {
    testExpressReadWrite[String](enumColumn, enums.head, Generic(enumSchema))
  }

  test("A KijiJob can read an array column with the default reader schema.") {
    testExpressReadWrite[List[String]](arrayColumn, avroArrays.head, Default, Some(arraySchema))
  }

  test("A KijiJob can read an array column with a generic reader schema.") {
    testExpressReadWrite[List[String]](arrayColumn, avroArrays.head, Generic(arraySchema))
  }

  test("A KijiJob can read a union column with the default reader schema.") {
    testExpressReadWrite[Any](unionColumn, unions.head, Default, Some(unionSchema))
  }

  test("A KijiJob can read a union column with a generic reader schema.") {
    testExpressReadWrite[Any](arrayColumn, unions.head, Generic(unionSchema))
  }

  test("A KijiJob can read a fixed column with the default reader schema.") {
    testExpressReadWrite[GenericFixed](fixedColumn, fixeds.head, Default, Some(fixedSchema))
  }

  test("A KijiJob can read a fixed column with a generic reader schema.") {
    testExpressReadWrite[GenericFixed](fixedColumn, fixeds.head, Generic(fixedSchema))
  }
}

// Must be its own top-level class for mystical serialization reasons
class ReadWriteJob[T](
    uri: String,
    input: ColumnRequestInput,
    output: ColumnRequestOutput,
    writeEid: String,
    args: Args
) extends KijiJob(args) {

  /**
   * Unwraps latest value from [[org.kiji.express.KijiSlice]] and verifies that the type is as
   * expected.
   * @param slice [[org.kiji.express.KijiSlice]] containing value to unwrap.
   * @return unwrapped value of type T.
   */
  private def unwrap(slice: KijiSlice[T]): (T, Long) = {
    require(slice.size == 1)
    val cell = slice.getFirst()
    if (cell.datum != null) { require(cell.datum.isInstanceOf[T]) }
    (cell.datum, cell.version)
  }

  KijiInput(uri, Map(input -> 'slice))
      .read
      .mapTo('slice -> ('value, 'time))(unwrap)
      .map('value -> 'entityId) { _: T => EntityId(writeEid)}
      .write(KijiOutput(uri, 'time, Map('value -> output)))
}