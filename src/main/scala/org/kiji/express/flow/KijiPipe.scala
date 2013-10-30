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

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import cascading.flow.Flow
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.twitter.chill.MeatLocker
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.Mode
import com.twitter.scalding.TupleConversions
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

import org.kiji.express.AvroRecord
import org.kiji.express.AvroValue
import org.kiji.express.repl.ExpressShell
import org.kiji.express.repl.Implicits
import org.kiji.express.repl.Implicits.pipeToRichPipe
import org.kiji.express.util.AvroGenericTupleConverter
import org.kiji.express.util.AvroUtil
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * A class that adds Kiji-specific functionality to a Cascading pipe. This includes running pipes
 * outside of the context of a Scalding Job.
 *
 * A `KijiPipe` should be obtained by end-users during the course of authoring a Scalding flow via
 * an implicit conversion available in [[org.kiji.express.repl.Implicits]].
 *
 * @param pipe enriched with extra functionality.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
class KijiPipe(private[express] val pipe: Pipe) extends TupleConversions {
  /**
   * Gets a job that can be used to run the data pipeline.
   *
   * @param args that should be used to construct the job.
   * @return a job that can be used to run the data pipeline.
   */
  private[express] def getJob(args: Args): Job = new KijiJob(args) {
    // The job's constructor should evaluate to the pipe to run.
    pipe

    /**
     *  The flow definition used by this job, which should be the same as that used by the user
     *  when creating their pipe.
     */
    override implicit val flowDef = Implicits.flowDef

    /**
     * Obtains a configuration used when running the job.
     *
     * This overridden method uses the same configuration as a standard Scalding job,
     * but adds options specific to KijiExpress, including adding a jar containing compiled REPL
     * code to the distributed cache if the REPL is running.
     *
     * @param mode used to run the job (either local or hadoop).
     * @return the configuration that should be used to run the job.
     */
    override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
      // Use the configuration from Scalding Job as our base.
      val configuration = super.config(mode)

      /** Appends a comma to the end of a string. */
      def appendComma(str: Any): String = str.toString + ","

      // If the REPL is running, we should add tmpjars passed in from the command line,
      // and a jar of REPL code, to the distributed cache of jobs run through the REPL.
      val replCodeJar = ExpressShell.createReplCodeJar()
      val tmpJarsConfig =
        if (replCodeJar.isDefined) {
          Map("tmpjars" -> {
              // Use tmpjars already in the configuration.
              configuration
                  .get("tmpjars")
                  .map(appendComma)
                  .getOrElse("") +
              // And tmpjars passed to ExpressShell from the command line when started.
              ExpressShell.tmpjars
                  .map(appendComma)
                  .getOrElse("") +
              // And a jar of code compiled by the REPL.
              "file://" + replCodeJar.get.getAbsolutePath
          })
        } else {
          // No need to add the tmpjars to the configuration
          Map[String, String]()
        }

      val userClassPathFirstConfig = Map("mapreduce.task.classpath.user.precedence" -> "true")

      configuration ++ tmpJarsConfig ++ userClassPathFirstConfig
    }

    /**
     * Builds a flow from the flow definition used when creating the pipeline run by this job.
     *
     * This overridden method operates the same as that of the super class,
     * but clears the implicit flow definition defined in [[org.kiji.express.repl.Implicits]]
     * after the flow has been built from the flow definition. This allows additional pipelines
     * to be constructed and run after the pipeline encapsulated by this job.
     *
     * @param mode the mode in which the built flow will be run.
     * @return the flow created from the flow definition.
     */
    override def buildFlow(implicit mode: Mode): Flow[_] = {
      val flow = super.buildFlow(mode)
      Implicits.resetFlowDef()
      flow
    }
  }

  /**
   * Runs this pipe as a Scalding job.
   */
  def run() {
    getJob(new Args(Map())).run(Mode.mode)
  }

  /**
   * Packs the specified fields into an Avro [[org.apache.avro.generic.GenericRecord]].
   *
   * @param fields is the mapping of input fields (to be packed into the
   *               [[org.apache.avro.generic.GenericRecord]]) to output field which will contain
   *               the [[org.apache.avro.generic.GenericRecord]].
   * @return a pipe containing all input fields, and an additional field containing an
   *         [[org.apache.avro.generic.GenericRecord]].
   */
  def packGenericRecord(fields: (Fields, Fields))(schema: Schema): Pipe = {
    require(fields._2.size == 1, "Cannot pack generic record to more than a single field.")
    require(schema.getType == Schema.Type.RECORD, "Cannot pack non-record Avro type.")
    pipe.map(fields) { input: GenericRecord => input } (
      new AvroGenericTupleConverter(new MeatLocker(schema)), implicitly[TupleSetter[GenericRecord]])
  }

  /**
   * Packs the specified fields into an Avro [[org.apache.avro.generic.GenericRecord]] and drops
   * other fields from the flow.
   *
   * @param fields is the mapping of input fields (to be packed into the
   *               [[org.apache.avro.generic.GenericRecord]]) to new output field which will
   *               contain the [[org.apache.avro.generic.GenericRecord]].
   * @return a pipe containing a single field with an Avro
   *         [[org.apache.avro.generic.GenericRecord]].
   */
  def packGenericRecordTo(fields: (Fields, Fields))(schema: Schema): Pipe = {
    require(fields._2.size == 1, "Cannot pack generic record to more than a single field.")
    require(schema.getType == Schema.Type.RECORD, "Cannot pack to non-record Avro type.")
    pipe.mapTo(fields) { input: GenericRecord => input } (
      new AvroGenericTupleConverter(new MeatLocker(schema)), implicitly[TupleSetter[GenericRecord]])
  }

  /**
   * TODO: remove
   */
  private[express] object AvroRecordTupleConverter extends TupleConverter[AvroRecord] {
    /**
     * Converts a TupleEntry into an AvroRecord.
     *
     * @param entry to pack.
     * @return an AvroRecord with field names and values corresponding to those in `entry`.
     */
    override def apply(entry: TupleEntry): AvroRecord = {
      val fieldMap: Map[String, AvroValue] =
          entry.getFields.asScala.toBuffer.map { field: Comparable[_] =>
            (field.toString, AvroUtil.scalaToGenericAvro(entry.getObject(field)))
          }.toMap
      AvroRecord(fieldMap)
    }

    // Arity is unknown.
    override def arity(): Int = -1
  }

  /**
   * TODO: remove
   */
  private[express] object UnpackTupleSetter extends TupleSetter[Any] {
    /**
     * Unpacks an AvroRecord into tuple fields.
     *
     * @param arg to unpack.
     * @return a tuple containing a shallowly-unpacked AvroRecord.
     * @throws IllegalArgumentException if the entry is not an AvroRecord.
     */
    override def apply(arg: Any): Tuple = {
      arg match {
        case AvroRecord(underlyingMap) => {
          val result = new Tuple()
          underlyingMap.values.foreach { value => result.add(value) }
          result
        }
        case _ => {
          throw new IllegalArgumentException(
              "KijiPipe cannot unpack unless the field is an AvroRecord.")
        }
      }
    }

    // Arity is unknown.
    override def arity(): Int = -1
  }

  /**
   * TODO: remove
   */
  def packAvro(fieldSpec: (Fields, Fields)): Pipe = {
    val (fromFields, toFields) = fieldSpec
    require(toFields.size == 1, "Cannot pack to more than one field.")
    pipe.map(fieldSpec) { input: AvroRecord => input } (AvroRecordTupleConverter,
      implicitly[TupleSetter[AvroRecord]])
  }

  /**
   * TODO: remove
   */
  def unpackAvro(fieldSpec: (Fields, Fields)): Pipe = {
    val (fromFields, toFields) = fieldSpec
    require(fromFields.size == 1, "Cannot unpack from more than one field.")
    pipe.map(fieldSpec) { input: Any => input } (implicitly[TupleConverter[Any]],
      UnpackTupleSetter)
  }
}
