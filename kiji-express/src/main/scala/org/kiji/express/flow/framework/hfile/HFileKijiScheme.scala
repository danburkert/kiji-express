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
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector

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
import org.kiji.schema.layout.impl.CellEncoderProvider
import org.kiji.schema.layout.impl.ColumnNameTranslator
import org.kiji.schema.{EntityId => JEntityId}

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
 * @param ocolumns mapping tuple field names to requests for Kiji columns.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class HFileKijiScheme(
  private[express] val timeRange: TimeRange,
  private[express] val timestampField: Option[Symbol],
  ocolumns: Map[Symbol, ColumnOutputSpec])
extends Scheme[JobConf, Nothing, OutputCollector[HFileKeyValue, NullWritable],
    KijiSourceContext, HFileKijiSinkContext] {

  /** Serialization workaround. Do not access directly. */
  private[this] val _outputColumns = KijiLocker(ocolumns)

  private[express] def outputColumns = _outputColumns.get

  setSinkFields(KijiScheme.buildSinkFields(_outputColumns.get, timestampField))

  def sourceConfInit(
      flowProcess: FlowProcess[JobConf],
      tap: Tap[JobConf, Nothing, OutputCollector[HFileKeyValue, NullWritable]],
      conf: JobConf): Unit = throw new UnsupportedOperationException("Cannot read from HFiles")

  def sinkConfInit(
      flowProcess: FlowProcess[JobConf],
      tap: Tap[JobConf, Nothing, OutputCollector[HFileKeyValue, NullWritable]],
      conf: JobConf
  ): Unit = throw new UnsupportedOperationException("Cannot read from HFiles")

  def source(
      flowProcess: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, Nothing]
  ): Boolean = throw new UnsupportedOperationException("Cannot read from HFiles")

  /**
   * Sets up any resources required for the sink job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val conf = flow.getConfigCopy
    val uri: KijiURI = KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build()

    doAndRelease(Kiji.Factory.open(uri, conf)) { kiji =>
      doAndRelease(kiji.openTable(uri.getTable)) { table: KijiTable =>
        val layout = table.getLayout
        sinkCall.setContext(
          HFileKijiSinkContext(
            EntityIdFactory.getFactory(table.getLayout),
            new ColumnNameTranslator(layout),
            new CellEncoderProvider(uri, layout, kiji.getSchemaTable,
              DefaultKijiCellEncoderFactory.get())))
      }
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster, so it should be kept as light as possible.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val HFileKijiSinkContext(eidFactory, columnTranslator, encoderProvider) = sinkCall.getContext
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

      val kijiColumn = new KijiColumnName(column.family, qualifier)
      val hbaseColumn = columnTranslator.toHBaseColumnName(kijiColumn)
      val encoder = encoderProvider.getEncoder(kijiColumn.getFamily, kijiColumn.getQualifier)

      val hfileKV = new HFileKeyValue(
        eid.getHBaseRowKey,
        hbaseColumn.getFamily,
        hbaseColumn.getQualifier,
        version,
        encoder.encode(value))

      sinkCall.getOutput.collect(hfileKV, null)
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
    sinkCall.setContext(null)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: HFileKijiScheme => (
        outputColumns == other.outputColumns
        && timestampField == other.timestampField
        && timeRange == other.timeRange)
    case _ => false
  }

  override def hashCode: Int = Objects.hashCode(outputColumns, timestampField, timeRange)
}

/**
 * Context housing information necessary for the scheme to interact
 * with the Kiji table.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
private[express] case class HFileKijiSinkContext (
    eidFactory: EntityIdFactory,
    columnTranslator: ColumnNameTranslator,
    encoderProvider: CellEncoderProvider)
