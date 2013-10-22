package org.kiji.express.flow

import com.twitter.scalding._
import org.scalatest.Suite
import cascading.tuple.Fields
import org.kiji.express.avro.SimpleRecord
import org.apache.avro.generic.GenericRecord
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.avro.specific.SpecificRecord
import scala.Predef._
import com.twitter.scalding.Tsv

@RunWith(classOf[JUnitRunner])
class AvroTupleConvertersSuite extends Suite {

  class PackSpecificJob(args: Args) extends KijiJob(args) {
    val input = Tsv(args("input"), new Fields("ls", "s"))
    val output = Tsv(args("output"))

//    println("GenericTupleUnpacker: (should be "+avroGenericTupleUnpacker+") "+
//      implicitly[TupleUnpacker[GenericRecord]])
//    println("SpecificTuplePacker: (should be "+avroSpecificTuplePacker+") "+
//      implicitly[TuplePacker[SpecificRecord]])
//    println("SpecificTuplePacker: (should be "+avroSpecificTuplePacker+") "+
//      implicitly[TuplePacker[SimpleRecord]])
//
//    require(implicitly[TupleUnpacker[GenericRecord]].getClass == avroGenericTupleUnpacker.getClass)
//    require(implicitly[TuplePacker[SpecificRecord]].getClass == avroSpecificTuplePacker.getClass)
//    require(implicitly[TuplePacker[SimpleRecord]].getClass == avroSpecificTuplePacker.getClass)

    input
      .read
      .map('ls -> 'l) { longString: String => longString.toLong }
      .packTo[SimpleRecord](('l, 's) -> 'a)
      .write(output)
  }

  class PackGenericJob(args: Args) extends KijiJob(args) {
    val input = Tsv(args("input"), new Fields("ls", "s"))
    val output = Tsv(args("output"))
    val schema = SimpleRecord.getClassSchema

    input
      .read
      .map('ls -> 'l) { longString: String => longString.toLong }
      .packGenericRecordTo(('l, 's) -> 'a)(schema)
      .write(output)
  }

  class UnpackSpecificJob(args: Args) extends KijiJob(args) {
    val input = Tsv(args("input"), new Fields("ls", "s"))
    val output = Tsv(args("output"))

    input
      .read
      .map('ls -> 'l) { longString: String => longString.toLong }
      .packTo[SimpleRecord](('l, 's) -> 'a)
      .unpackTo[SimpleRecord](('a) -> ('l, 's, 'o))
      .write(output)
  }

  class UnpackGenericJob(args: Args) extends KijiJob(args) {
    val input = Tsv(args("input"), new Fields("ls", "s"))
    val output = Tsv(args("output"))
    val schema = SimpleRecord.getClassSchema

    input
      .read
      .map('ls -> 'l) { longString: String => longString.toLong }
      .packGenericRecordTo(('l, 's) -> 'a)(schema)
      .unpackTo[GenericRecord]('a -> ('l, 's, 'o))
      .write(output)
  }

  val dataIn = "/Users/dan/Desktop/simple-data.tsv"
  val specificOut = "/Users/dan/Desktop/specific-out.tsv"
  val genericOut = "/Users/dan/Desktop/generic-out.tsv"
  val specificUnpack = "/Users/dan/Desktop/specific-unpack.tsv"
  val genericUnpack = "/Users/dan/Desktop/generic-unpack.tsv"

  new PackSpecificJob(new Args(Map("input" -> List(dataIn), "output" -> List(specificOut)))).run
  new PackGenericJob(new Args(Map("input" -> List(dataIn), "output" -> List(genericOut)))).run
  new UnpackSpecificJob(new Args(Map("input" -> List(dataIn), "output" -> List(specificUnpack)))).run
  new UnpackGenericJob(new Args(Map("input" -> List(dataIn), "output" -> List(genericUnpack)))).run
}
