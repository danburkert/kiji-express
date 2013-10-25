package org.kiji.express.flow

import org.kiji.express.{KijiSuite, KijiSlice, EntityId}
import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
import scala.collection.mutable.Buffer
import org.kiji.express.avro.SimpleRecord
import org.apache.avro.generic.{GenericRecordBuilder, GenericRecord}

/**
 * Common setup used by the REPL and KijiJob tests in [[org.kiji.express.flow.KijiPipeSuite]]
 * and [[org.kiji.express.flow.KijiJobSuite]], respectivly.
 */
object FlowTestUtil extends KijiSuite {
  /** Name of dummy file for test job input. */
  val inputFile: String = "inputFile"

  /** Name of dummy file for test job output. */
  val outputFile: String = "outputFile"

  /** Input tuples to use for word count tests. */
  val wordCountInput: List[(EntityId, KijiSlice[String])] = List(
    ( EntityId("row01"), slice("family:column1", (1L, "hello")) ),
    ( EntityId("row02"), slice("family:column1", (2L, "hello")) ),
    ( EntityId("row03"), slice("family:column1", (1L, "world")) ),
    ( EntityId("row04"), slice("family:column1", (3L, "hello")) ))

  /** Table layout for word count tests. */
  val wordCountLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Output validator for word count tests. */
  def validateWordCount(outputBuffer: Buffer[(String, Int)]) {
    val outMap = outputBuffer.toMap

    // Validate that the output is as expected.
    assert(3 === outMap("hello"))
    assert(1 === outMap("world"))
  }

  /** Schema of SimpleRecord Avro record. */
  val simpleSchema = SimpleRecord.getClassSchema

  /** Sample inputs for testing Avro packing / unpacking in tuple form. */
  val simpleInput: List[(Long, String)] = List(
    (1, "foobar"),
    (2, "shoe"),
    (3, "random"),
    (99, "baloons"),
    (356, "sumerians"))

  /** Sample inputs for testing Avro packing / unpacking in Specific Record form. */
  val simpleSpecificRecords: List[SimpleRecord] = simpleInput.map { t =>
    SimpleRecord.newBuilder.setL(t._1).setS(t._2).build()
  }

  /** Sample inputs for testing Avro packing / unpacking in Generic Record form. */
  val simpleGenericRecords: List[GenericRecord] = simpleInput.map { t =>
    new GenericRecordBuilder(simpleSchema).set("l", t._1).set("s", t._2).build()
  }

  def validateSpecificSimpleRecord(outputs: Buffer[(Long, String, String, SimpleRecord)]) {
    outputs.foreach { t =>
      val (l, s, o, r) = t
      assert(simpleSpecificRecords.contains(r))
      assert(r.getL === l)
      assert(r.getS === s)
      assert(r.getO === o)
    }
  }

  def validateGenericSimpleRecord(outputs: Buffer[(Long, String, String, GenericRecord)]) {
    outputs.foreach { t =>
      val (l, s, o, r) = t
      assert(simpleGenericRecords.contains(r))
      assert(r.get("l") === l)
      assert(r.get("s") === s)
      assert(r.get("o") === o)
    }
  }
}
