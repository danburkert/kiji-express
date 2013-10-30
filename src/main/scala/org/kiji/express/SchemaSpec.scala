package org.kiji.express

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

/**
 * A specification of how to read or write values to a Kiji column.
 */
sealed trait SchemaSpec extends java.io.Serializable {
  /**
   * Retrieve the Avro [[org.apache.avro.Schema]] object associated with this SchemaSpec,
   * if possible.
   */
  private[express] def schema: Option[Schema]
}

/**
 * Module to provide SchemaSpec implementations.
 */
object SchemaSpec {
  /**
   * Specifies reading or writing with the supplied [[org.apache.avro.Schema]].
   *
   * Note that for serialization reasons this class takes the json form of the schema;
   * use the factory function in the companion class to construct directly with a
   * [[org.apache.avro.Schema]].
   * @param genericSchema of data
   */
  case class Generic(genericSchema: Schema) extends SchemaSpec {
    override lazy val schema = Some(genericSchema)
  }

  /**
   * A specification for reading or writing as an instance of the supplied Avro specific record.
   * @param klass of the specific record.
   */
  case class Specific(klass: Class[_ <: SpecificRecord]) extends SchemaSpec {
    @transient override lazy val schema = Some(klass.newInstance.getSchema)
  }

  /**
   * Use the writer schema associated with a value to read or write.
   *
   * In the case of reading a value, the writer schema used to serialize the value will be used.
   * In the case of writing a value, the schema attached to or inferred from the value will be used.
   */
  case object Writer extends SchemaSpec {
    override val schema = None
  }

  /**
   * Use the default reader schema of the column to read or write the values to the column.
   */
  case object DefaultReader extends SchemaSpec {
    override val schema = None
  }
}

