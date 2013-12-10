package org.kiji.express

import com.twitter.scalding.Job
import com.twitter.scalding.Tool
import org.apache.hadoop.fs.Path

import org.kiji.mapreduce.HFileLoader
import org.kiji.schema.KijiTable
import org.apache.hadoop.conf.Configuration

/**
 * Provides helper functions for writing integration tests against scalding.
 */
object IntegrationUtil {

  /**
   * Run a job with the given arguments.
   * @param jobClass class of job
   * @param args to supply to job
   */
  def runJob(jobClass: Class[_ <: Job], args: String*): Unit =
    Tool.main(jobClass.getName +: args.toArray)

  /**
   * Bulk load HFiles from the given path into the table specified with the uri and conf.
   *
   * @param path to HFiles
   * @param table to bulk-load into
   */
  def bulkLoadHFiles(path: String, conf: Configuration, table: KijiTable) =
    HFileLoader.create(conf).load(new Path(path), table)
}
