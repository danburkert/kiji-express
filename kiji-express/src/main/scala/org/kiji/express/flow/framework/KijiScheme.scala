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

import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.{TupleEntry, Fields, Tuple}
import com.google.common.base.Objects
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.ColumnFamilyInputSpec
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.PagingSpec
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.TransientStream
import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.AvroUtil
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.express.flow.util.SpecificCellSpecs
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.ColumnVersionIterator
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiBufferedWriter
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.MapFamilyVersionIterator
import org.kiji.schema.{EntityId => JEntityId}

/**
 * A Kiji-specific implementation of a Cascading `Scheme`, which defines how to read and write the
 * data stored in a Kiji table.
 *
 * KijiScheme is responsible for converting rows from a Kiji table that are input to a Cascading
 * flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Kiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * KijiScheme must be used with [[org.kiji.express.flow.framework.KijiTap]],
 * since it expects the Tap to have access to a Kiji table.  [[org.kiji.express.flow.KijiSource]]
 * handles the creation of both KijiScheme and KijiTap in KijiExpress.
 *
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param icolumns mapping tuple field names to requests for Kiji columns.
 * @param ocolumns mapping tuple field names to specifications for Kiji columns to write out
 *     to.
 */
@ApiAudience.Framework
@ApiStability.Experimental
class KijiScheme(
    private[express] val timeRange: TimeRange,
    private[express] val timestampField: Option[Symbol],
    icolumns: Map[Symbol, ColumnInputSpec] = Map(),
    ocolumns: Map[Symbol, ColumnOutputSpec] = Map())
extends KijiScheme.HadoopScheme {
  import KijiScheme._

  /** Serialization workaround. Do not access directly. */
  private[this] val _inputColumns = KijiLocker(icolumns)
  private[this] val _outputColumns = KijiLocker(ocolumns)

  @transient private[express] lazy val inputColumns = _inputColumns.get
  @transient private[express] lazy val outputColumns = _outputColumns.get

  // Including output column keys here because we might need to read back outputs during test
  // TODO (EXP-250): Ideally we should include outputColumns.keys here only during tests.
  setSourceFields(buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(buildSinkFields(outputColumns, timestampField))

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our KijiDataRequest.
   */
  override def sourceConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], Nothing],
      conf: JobConf) {
    // Build a data request.
    val request: KijiDataRequest = buildRequest(timeRange, inputColumns.values)

    // Write all the required values to the job's configuration object.
    conf.setInputFormat(classOf[KijiInputFormat])
    conf.set(
        KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
    conf.set(SpecificCellSpecs.CELLSPEC_OVERRIDE_CONF_KEY,
        SpecificCellSpecs.serializeOverrides(inputColumns))
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]) {
    // Set the context used when reading data from the source.
    sourceCall.setContext(KijiSourceContext(sourceCall.getInput.createValue()))
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   * @return `true` if another row was read and it was converted to a tuple,
   *     `false` if there were no more rows to read.
   */
  override def source(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]): Boolean = {
    // Get the current key/value pair.
    val KijiSourceContext(value) = sourceCall.getContext

    // Get the next row.
    if (sourceCall.getInput.next(null, value)) {
      val row: KijiRowData = value.get()

      // Build a tuple from this row.
      val tuple: Tuple = rowToTuple(inputColumns, getSourceFields, timestampField, row)

      // If no fields were missing, set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(tuple)
      flow.increment(counterGroupName, counterSuccess, 1)

      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the RecordReader.
    }
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourceCleanup(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]) {
    sourceCall.setContext(null)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our KijiDataRequest.
   */
  override def sinkConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], Nothing],
      conf: JobConf) {
    // No-op since no configuration parameters need to be set to encode data for Kiji.
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectKijiSinkContext, Nothing]) {

    val conf = flow.getConfigCopy
    val uri: KijiURI = KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build()

    doAndRelease(Kiji.Factory.open(uri, conf)) { kiji =>
      doAndRelease(kiji.openTable(uri.getTable)) { table: KijiTable =>
      // Set the sink context to an opened KijiTableWriter.
        sinkCall.setContext(
          DirectKijiSinkContext(
            outputColumns,
            timestampField,
            EntityIdFactory.getFactory(table.getLayout),
            table.getWriterFactory.openBufferedWriter(),
            KijiScheme.sinkDirect))
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
      sinkCall: SinkCall[DirectKijiSinkContext, Nothing]) {
    KijiScheme.kijiSink(sinkCall)
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
      sinkCall: SinkCall[DirectKijiSinkContext, Nothing]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }

  override def equals(other: Any): Boolean = other match {
    case scheme: KijiScheme => (
        inputColumns == scheme.inputColumns
        && outputColumns == scheme.outputColumns
        && timestampField == scheme.timestampField
        && timeRange == scheme.timeRange
      )
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(
      inputColumns,
      outputColumns,
      timestampField,
      timeRange)
}


/**
 * Companion object for KijiScheme.
 *
 * Contains constants and helper methods for converting between Kiji rows and Cascading tuples,
 * building Kiji data requests, and some utility methods for handling Cascading fields.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object KijiScheme {

  type HadoopScheme = Scheme[JobConf, RecordReader[KijiKey, KijiValue],
      Nothing, KijiSourceContext, DirectKijiSinkContext]

  private val logger: Logger = LoggerFactory.getLogger(classOf[KijiScheme])

  /** Hadoop mapred counter group for KijiExpress. */
  private[express] val counterGroupName = "kiji-express"
  /** Counter name for the number of rows successfully read. */
  private[express] val counterSuccess = "ROWS_SUCCESSFULLY_READ"
  /** Field name containing a row's [[org.kiji.schema.EntityId]]. */
  val entityIdField: Symbol = 'entityId
  /** Default number of qualifiers to retrieve when paging in a map type family.*/
  private val qualifierPageSize: Int = 1000

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * If there is no data in a column in this row, the value of that field in the tuple will be an
   * empty iterable of cells.
   *
   * This is used in the `source` method of KijiScheme, for reading rows from Kiji into
   * KijiExpress tuples.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of desired tuple elements.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @param row to convert to a tuple.
   * @return a tuple containing the values contained in the specified row.
   */
  private[express] def rowToTuple(
      columns: Map[Symbol, ColumnInputSpec],
      fields: Fields,
      timestampField: Option[Symbol],
      row: KijiRowData
  ): Tuple = {
    val result: Tuple = new Tuple()

    // Add the row's EntityId to the tuple.
    val entityId: EntityId = EntityId.fromJavaEntityId(row.getEntityId)
    result.add(entityId)

    def rowToTupleColumnFamily(cf: ColumnFamilyInputSpec): Unit = {
      if (row.containsColumn(cf.family)) {
        cf.paging match {
          case PagingSpec.Off => {
            val stream: Seq[FlowCell[_]] = row
                .iterator(cf.family)
                .asScala
                .toList
                .map { kijiCell: KijiCell[_] => FlowCell(kijiCell) }
            result.add(stream)
          }
          case PagingSpec.Cells(pageSize) => {
            def genItr(): Iterator[FlowCell[_]] = {
              new MapFamilyVersionIterator(row, cf.family, qualifierPageSize, pageSize)
                  .asScala
                  .map { entry: MapFamilyVersionIterator.Entry[_] =>
                    FlowCell(
                        cf.family,
                        entry.getQualifier,
                        entry.getTimestamp,
                        AvroUtil.avroToScala(entry.getValue))
              }
            }
            result.add(new TransientStream(genItr))
          }
        }
      } else {
        result.add(Seq())
      }
    }

    def rowToTupleQualifiedColumn(qc: QualifiedColumnInputSpec): Unit = {
      if (row.containsColumn(qc.family, qc.qualifier)) {
        qc.paging match {
          case PagingSpec.Off => {
            val stream: Seq[FlowCell[_]] = row
                .iterator(qc.family, qc.qualifier)
                .asScala
                .toList
                .map { kijiCell: KijiCell[_] => FlowCell(kijiCell) }
            result.add(stream)
          }
          case PagingSpec.Cells(pageSize) => {
            def genItr(): Iterator[FlowCell[_]] = {
              new ColumnVersionIterator(row, qc.family, qc.qualifier, pageSize)
                  .asScala
                  .map { entry: java.util.Map.Entry[java.lang.Long,_] =>
                    FlowCell(
                      qc.family,
                      qc.qualifier,
                      entry.getKey,
                      AvroUtil.avroToScala(entry.getValue)
                    )
                  }
            }
            result.add(new TransientStream(genItr))
          }
        }
      } else {
        result.add(Seq())
      }
    }

    // Get rid of the entity id and timestamp fields, then map over each field to add a column
    // to the tuple.
    fields
        .iterator()
        .asScala
        .map(f => Symbol(f.toString))
        .filter(field => field != entityIdField)
        .filter(field => timestampField.map(_ != field).getOrElse(true))
        .map(columns)
        // Build the tuple by adding each requested value into result.
        .foreach {
          case cf: ColumnFamilyInputSpec => rowToTupleColumnFamily(cf)
          case qc: QualifiedColumnInputSpec => rowToTupleQualifiedColumn(qc)
        }

    result
  }

  /**
   * Extracts a KijiCell from the output tuple, and passes the componenets to `sinkCell`.
   * @tparam Context type of context object
   * @tparam Output type of sink output
   * @param sinkCall context of sink
   */
  def kijiSink[Context <: KijiSinkContext, Output](sinkCall: SinkCall[Context, Output]) = {
    val context = sinkCall.getContext
    val tuple: TupleEntry = sinkCall.getOutgoingEntry

    // Get the entityId.
    val entityId: JEntityId = tuple
        .getObject(KijiScheme.entityIdField.name)
        .asInstanceOf[EntityId]
        .toJavaEntityId(context.eidFactory)

    // Get a timestamp to write the values to, if it was specified by the user.
    val timestamp: Long = context.timestampField
        .map(field => tuple.getLong(field.name))
        .getOrElse(HConstants.LATEST_TIMESTAMP)

    context.columns.foreach { case (field, column) =>
      val value = tuple.getObject(field.name)

      val qualifier: String = column match {
        case qc: QualifiedColumnOutputSpec => qc.qualifier
        case cf: ColumnFamilyOutputSpec => tuple.getString(cf.qualifierSelector.name)
      }

      context.cellSink(context, entityId, column.family, qualifier, timestamp, column.encode(value))
    }
  }

  /**
   * A CellSink implementation for writing cells directly to a KijiTable.
   * @param sinkCall context holder
   * @param eid entity id of cell
   * @param family column family of cell
   * @param qualifier column qualifier of cell
   * @param version of cell
   * @param value of cell
   */
  def sinkDirect(
      sinkCall: SinkCall[DirectKijiSinkContext, Nothing],
      eid: JEntityId,
      family: String,
      qualifier: String,
      version: Long,
      value: Any): Unit = {
    sinkCall.getContext.writer.put(eid, family, qualifier, version, value)
  }

  /**
   * Builds a data request out of the timerange and list of column requests.
   *
   * @param timeRange of cells to retrieve.
   * @param columns to retrieve.
   * @return data request configured with timeRange and columns.
   */
  private[express] def buildRequest(
      timeRange: TimeRange,
      columns: Iterable[ColumnInputSpec]
  ): KijiDataRequest = {
    def addColumn(
        builder: KijiDataRequestBuilder,
        column: ColumnInputSpec
    ): KijiDataRequestBuilder.ColumnsDef = {
      builder.newColumnsDef()
          .withMaxVersions(column.maxVersions)
          .withFilter(column.filter.map(_.toKijiColumnFilter).getOrElse(null))
          .withPageSize(column.paging.cellsPerPage.getOrElse(0))
          .add(column.columnName)
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columns.foreach(column => addColumn(requestBuilder, column))
    requestBuilder.build()
  }

  /**
   * Transforms a list of field names into a Cascading [[cascading.tuple.Fields]] object.
   *
   * @param fieldNames is a list of field names.
   * @return a Fields object containing the names.
   */
  private def toField(fieldNames: Iterable[Symbol]): Fields = {
    new Fields(fieldNames.map(_.name).toArray[Comparable[String]]:_*)
  }

  /**
   * Builds the list of tuple fields being read by a scheme. The special field name
   * "entityId" will be included to hold entity ids from the rows of Kiji tables.
   *
   * @param fieldNames is a list of field names that a scheme should read.
   * @return is a collection of fields created from the names.
   */
  private[express] def buildSourceFields(fieldNames: Iterable[Symbol]): Fields = {
    toField(Set(entityIdField) ++ fieldNames)
  }

  /**
   * Builds the list of tuple fields being written by a scheme. The special field name "entityId"
   * will be included to hold entity ids that values should be written to. Any fields that are
   * specified as qualifiers for a map-type column family will also be included. A timestamp field
   * can also be included, identifying a timestamp that all values will be written to.
   *
   * @param columns is the column requests for this Scheme, with the names of each of the
   *     fields that contain data to write to Kiji.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @return a collection of fields created from the parameters.
   */
  private[express] def buildSinkFields(
      columns: Map[Symbol, ColumnOutputSpec],
      timestampField: Option[Symbol]
  ): Fields = {
    toField(Set(entityIdField)
        ++ columns.keys
        ++ columns.values.collect { case x: ColumnFamilyOutputSpec => x.qualifierSelector }
        ++ timestampField)
  }
}

/**
 * A SinkContext for writing to a Kiji table.
 */
abstract class KijiSinkContext {
  def columns: Map[Symbol, ColumnOutputSpec]
  def timestampField: Option[Symbol]
  def eidFactory: EntityIdFactory
  def cellSink: (this.type, JEntityId, String, String, Long, Any) => Any
}

/**
 * Container for the table writer and Kiji table layout required during a sink
 * operation to write the output of a map reduce task back to a Kiji table.
 * This is configured during the sink prepare operation.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
case class DirectKijiSinkContext(
    columns: Map[Symbol, ColumnOutputSpec],
    timestampField: Option[Symbol],
    eidFactory: EntityIdFactory,
    writer: KijiBufferedWriter,
    cellSink: (SinkCall[this.type, Nothing], JEntityId, String, String, Long, Any) => Any)
    extends KijiSinkContext
