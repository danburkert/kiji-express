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

package org.kiji.express.flow.framework

import java.io.{OutputStream, InputStream}
import java.util.HashMap
import java.util.{Map => JMap}
import java.util.Properties

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.TupleEntry
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.util.GenericCellSpecs
import org.kiji.express.flow.util.Resources._
import org.kiji.express.flow.util.SpecificCellSpecs
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.{EntityId => JEntityId, KijiTableReader, EntityIdFactory, Kiji, KijiColumnName, KijiRowData, KijiRowScanner, KijiTable, KijiURI}
import org.kiji.schema.layout.CellSpec

/**
 * A local version of [[org.kiji.express.flow.framework.KijiScheme]] that is meant to be used with
 * Cascading's local job runner. [[org.kiji.express.flow.framework.KijiScheme]] and
 * LocalKijiScheme both define how to read and write the data stored in a Kiji table.
 *
 * This scheme is meant to be used with [[org.kiji.express.flow.framework.LocalKijiTap]] and
 * Cascading's local job runner. Jobs run with Cascading's local job runner execute on
 * your local machine instead of a cluster. This can be helpful for testing or quick jobs.
 *
 * In KijiExpress, LocalKijiScheme is used in tests.  See [[org.kiji.express.flow.KijiSource]]'s
 * `TestLocalKijiScheme` class.
 *
 * This scheme is responsible for converting rows from a Kiji table that are input to a
 * Cascading flow into Cascading tuples (see
 * `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Kiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * Note: LocalKijiScheme logs every row that was skipped because of missing data in a column. It
 * lacks the parameter `loggingInterval` in [[org.kiji.express.flow.framework.KijiScheme]] that
 * configures how many skipped rows will be logged.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiScheme is
 * serialized, the result is not persisted anywhere making serialVersionUID unnecessary.
 *
 * @see [[org.kiji.express.flow.framework.KijiScheme]]
 *
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param inputColumns is a one-to-one mapping from field names to Kiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 * @param outputColumns is a one-to-one mapping from field names to Kiji columns. Values from the
 *     tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] case class LocalKijiScheme(
    private[express] val timeRange: TimeRange,
    private[express] val timestampField: Option[Symbol],
    private[express] val inputColumns: Map[Symbol, ColumnInputSpec] = Map(),
    private[express] val outputColumns: Map[Symbol, ColumnOutputSpec] = Map())
extends Scheme[Properties, InputStream, OutputStream, InputContext, DirectKijiSinkContext] {

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(KijiScheme.buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(KijiScheme.buildSinkFields(outputColumns, timestampField))

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process flow being built.
   * @param tap that is being used with this scheme.
   * @param conf is an unused Properties object that is a stand-in for a job configuration object.
   */
  override def sourceConfInit(
      process: FlowProcess[Properties],
      tap: Tap[Properties, InputStream, OutputStream],
      conf: Properties) {
    // No-op. Setting options in a java Properties object is not going to help us read from
    // a Kiji table.
  }

  /**
   * Sets up any resources required to read from a Kiji table.
   *
   * @param process currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {
    val conf: JobConf = HadoopUtil.createJobConf(
        process.getConfigCopy,
        new JobConf(HBaseConfiguration.create()))
    val uriString: String = conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)
    val kijiUri: KijiURI = KijiURI.newBuilder(uriString).build()

    // Build the input context.
    withKijiTable(kijiUri, conf) { table: KijiTable =>
      val request = KijiScheme.buildRequest(timeRange, inputColumns.values)
      val reader = {
        val allCellSpecs: JMap[KijiColumnName, CellSpec] = new HashMap[KijiColumnName, CellSpec]()
        allCellSpecs.putAll(GenericCellSpecs(table))
        allCellSpecs.putAll(SpecificCellSpecs.buildCellSpecs(table.getLayout, inputColumns).asJava)
        table.getReaderFactory.openTableReader(allCellSpecs)
      }
      val scanner = reader.getScanner(request)
      val context = InputContext(reader, scanner, scanner.iterator.asScala)
      sourceCall.setContext(context)
    }
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *     <code>false</code> if there were no more rows to read.
   */
  override def source(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]): Boolean = {
    val itr = sourceCall.getContext.iterator
    if (itr.hasNext) {
      val tuple = KijiScheme.rowToTuple(inputColumns, getSourceFields, timestampField, itr.next())

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(tuple)
      process.increment(KijiScheme.counterGroupName, KijiScheme.counterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
    }
  }

  /**
   * Cleans up any resources used to read from a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourceCleanup(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {
    val context: InputContext = sourceCall.getContext
    context.scanner.close()
    context.reader.close()

    // Set the context to null so that we no longer hold any references to it.
    sourceCall.setContext(null)
  }

  def sinkConfInit(
      flowProcess: FlowProcess[Properties],
      tap: Tap[Properties, InputStream, OutputStream],
      conf: Properties): Unit = {
  }

  /**
   * Sets up any resources required to write to a Kiji table.
   *
   * @param flow Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {

    val conf: JobConf =
      HadoopUtil.createJobConf(flow.getConfigCopy, new JobConf(HBaseConfiguration.create()))

    val uri: KijiURI = KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build()

    doAndRelease(Kiji.Factory.open(uri, conf)) { kiji =>
      doAndRelease(kiji.openTable(uri.getTable)) { table: KijiTable =>
      // Set the sink context to an opened KijiTableWriter.
        sinkCall.setContext(
          DirectKijiSinkContext(
            EntityIdFactory.getFactory(table.getLayout),
            table.getWriterFactory.openBufferedWriter))
      }
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sink(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val DirectKijiSinkContext(eidFactory, writer) = sinkCall.getContext
    val tuple: TupleEntry = sinkCall.getOutgoingEntry

    // Get the entityId.
    val eid: JEntityId = tuple
        .getObject(KijiScheme.entityIdField.name)
        .asInstanceOf[EntityId]
        .toJavaEntityId(eidFactory)

    // Get a timestamp to write the values to, if it was specified by the user.
    val version: Long = timestampField
        .map(field => tuple.getLong(field.name))
        .getOrElse(HConstants.LATEST_TIMESTAMP)

    outputColumns.foreach { case (field, column) =>
      val value = tuple.getObject(field.name)

      val qualifier: String = column match {
        case qc: QualifiedColumnOutputSpec => qc.qualifier
        case cf: ColumnFamilyOutputSpec => tuple.getString(cf.qualifierSelector.name)
      }

      writer.put(eid, column.family, qualifier, version, value)
    }
  }

  /**
   * Cleans up any resources used to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }
}

/**
 * Encapsulates the state required to read from a Kiji table locally, for use in
 * [[org.kiji.express.flow.framework.LocalKijiScheme]].
 *
 * @param scanner that has an open connection to the desired Kiji table.
 * @param iterator that maintains the current row pointer.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[express] final case class InputContext(
    reader: KijiTableReader,
    scanner: KijiRowScanner,
    iterator: Iterator[KijiRowData])
