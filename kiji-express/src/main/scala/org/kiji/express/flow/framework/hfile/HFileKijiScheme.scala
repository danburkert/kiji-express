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

package org.kiji.express.flow.framework.hfile

import cascading.flow.FlowProcess
import cascading.scheme.NullScheme
import cascading.scheme.SinkCall
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.framework.KijiSourceContext
import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.mapreduce.framework.HFileKeyValue
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.impl.CellEncoderProvider
import org.kiji.schema.layout.impl.ColumnNameTranslator

/**
 * A Kiji-specific implementation of a Cascading `Scheme` which defines how to write data
 * to HFiles.
 *
 * HFileKijiScheme is responsible for converting rows from a Kiji table that are input to a
 * Cascading flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to an HFile capable of being bulk loaded into HBase
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * HFileKijiScheme must be used with [[org.kiji.express.flow.framework.hfile.HFileKijiTap]],
 * since it expects the Tap to have access to a Kiji table.
 * [[org.kiji.express.flow.framework.hfile.HFileKijiSource]] handles the creation of both
 * HFileKijiScheme and KijiTap in KijiExpress.
 *
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param loggingInterval to log skipped rows on. For example, if loggingInterval is 1000,
 *     then every 1000th skipped row will be logged.
 * @param columns mapping tuple field names to requests for Kiji columns.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class HFileKijiScheme(
  private[express] val timeRange: TimeRange,
  private[express] val timestampField: Option[Symbol],
  private[express] val loggingInterval: Long,
  @transient private[express] val columns: Map[Symbol, ColumnOutputSpec])
    extends HFileKijiScheme.HFileScheme {

  // ColumnOutputSpec objects cannot be correctly serialized via
  // java.io.Serializable.  Chiefly, Avro objects including Schema and all of the Generic types
  // are not Serializable.  By making the columns transient and wrapping
  // them in KijiLocker objects (which handle serialization correctly),
  // we can work around this limitation.  Thus, the following two lines should be the only to
  // reference `inputColumns` and `outputColumns`, because they will be null after serialization.
  // Everything else should instead use _inputColumns.get and _outputColumns.get.
  private val _columns = KijiLocker(columns)

  setSinkFields(KijiScheme.buildSinkFields(_columns.get, timestampField))

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val uri = flow.getConfigCopy.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val kijiURI = KijiURI.newBuilder(uri).build()
    val kiji = Kiji.Factory.open(kijiURI)

    doAndRelease(kiji.openTable(kijiURI.getTable)) { table: KijiTable =>
      // Set the sink context to an opened KijiTableWriter.
      val ctx = HFileKijiSinkContext(kiji, kijiURI,
        table.getLayout, new ColumnNameTranslator(table.getLayout))
      sinkCall.setContext(ctx)
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry

    val HFileKijiSinkContext(kiji, uri, layout, colTranslator) = sinkCall.getContext
    val eidFactory = EntityIdFactory.getFactory(layout)

    val encoderProvider = new CellEncoderProvider(uri, layout, kiji.getSchemaTable,
        DefaultKijiCellEncoderFactory.get())

    HFileKijiScheme.outputCells(output, timestampField, _columns.get) { key: HFileCell =>
      // Convert cell to an HFileKeyValue
      val kijiColumn = new KijiColumnName(key.colRequest.family, key.colRequest.qualifier)
      val hbaseColumn = colTranslator.toHBaseColumnName(kijiColumn)

      val encoder = encoderProvider.getEncoder(kijiColumn.getFamily, kijiColumn.getQualifier)
      val hFileKeyValue = new HFileKeyValue(
        key.entityId.toJavaEntityId(eidFactory).getHBaseRowKey,
        hbaseColumn.getFamily, hbaseColumn.getQualifier, key.timestamp,
        encoder.encode(key.datum))

      sinkCall.getOutput.collect(hFileKeyValue, NullWritable.get())
    }
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkCleanup(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val HFileKijiSinkContext(kiji, _, _, _) = sinkCall.getContext

    kiji.release()
    sinkCall.setContext(null)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: HFileKijiScheme => {
      _columns.get == other._columns.get &&
        timestampField == other.timestampField &&
        timeRange == other.timeRange
    }
    case _ => false
  }

  override def hashCode: Int = Objects.hashCode(_columns.get, timestampField, timeRange)
}

/**
 * Context housing information necessary for the scheme to interact
 * with the Kiji table.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
private[express] case class HFileKijiSinkContext (
  kiji: Kiji,
  kijiUri: KijiURI,
  layout: KijiTableLayout,
  columnTranslator: ColumnNameTranslator
)

/**
 * A cell from a Kiji table containing some datum, addressed by a family, qualifier,
 * and version timestamp.
 *
 * @param entityId of the Kiji table cell.
 * @param colRequest identifying the location of the Kiji table cell.
 * @param timestamp  of the Kiji table cell.
 * @param datum in the Kiji table cell.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
private[express] case class HFileCell private[express] (
  entityId: EntityId,
  colRequest: QualifiedColumnOutputSpec,
  timestamp: Long,
  datum: AnyRef)

object HFileKijiScheme {
  type HFileScheme = NullScheme[JobConf, RecordReader[_, _],
    OutputCollector[HFileKeyValue, NullWritable], KijiSourceContext, HFileKijiSinkContext]

  private[express] def outputCells(output: TupleEntry,
                                   timestampField: Option[Symbol],
                                   columns: Map[Symbol, ColumnOutputSpec])(
                                     cellHandler: HFileCell => Unit) {

    // Get a timestamp to write the values to, if it was specified by the user.
    val timestamp: Long = timestampField
        .map(field => output.getLong(field.name))
        .getOrElse(HConstants.LATEST_TIMESTAMP)

    val entityId: EntityId = output.getObject(KijiScheme.entityIdField.name).asInstanceOf[EntityId]

    columns.foreach { case (field, colSpec) =>
      val colValue = output.getObject(field.name)
      val newColRequest = colSpec match {
        case ColumnFamilyOutputSpec(family, qualField, _) =>
          QualifiedColumnOutputSpec(family, output.getString(qualField.name))
        case qc @ QualifiedColumnOutputSpec(_, _, _) => qc
      }
      cellHandler(HFileCell(entityId, newColRequest, timestamp, colValue))
    }
  }
}
