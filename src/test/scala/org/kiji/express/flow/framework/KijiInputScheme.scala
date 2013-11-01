package org.kiji.express.flow.framework

import org.kiji.express.flow.{InputColumnSpec, TimeRange}
import com.twitter.scalding.Field

/**
 * A Kiji-specific implementation of a Cascading [[cascading.scheme.Scheme]],
 * which defines how to read data from a Kiji table.
 *
 * [[org.kiji.express.flow.framework.KijiInputScheme]] is responsible for converting rows from a
 * Kiji table into Cascading [[cascading.tuple.Tuple]]s, which are fed into the Cascading
 * [[cascading.flow.Flow]].
 *
 * @param timeRange from which to collect cells from the table.
 * @param loggingInterval to log skipped rows.  For example, if loggingInterval is 1000,
 *                        then every 1000th skipped row will be logged.
 * @param columns mapping of [[cascading.tuple.Tuple]] field names to Kiji column input spec.
 */
private[express] class KijiInputScheme(
    timeRange: TimeRange,
    loggingInterval: Long,
    columns: Map[Field[_], InputColumnSpec]
) {

}
