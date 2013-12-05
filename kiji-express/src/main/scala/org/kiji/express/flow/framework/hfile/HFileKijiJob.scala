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

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

import cascading.flow.Flow
import com.twitter.scalding.Args
import com.twitter.scalding.Mode
import org.apache.hadoop.mapred.JobConf

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.KijiJob
import cascading.pipe.{Checkpoint, Pipe}
import cascading.tap.Tap

/**
 * HFileKijiJob is an extension of KijiJob and users should extend it instead of KijiJob when
 * writing jobs in KijiExpress that need to output HFiles that can eventually be bulk-loaded into
 * HBase.
 *
 * In your HFileKijiJob, you need to write to a source constructed with
 * [[org.kiji.express.flow.framework.hfile.HFileKijiOutput]].
 *
 * You can extend HFileKijiJob like this:
 *
 * {{{
 * class MyKijiExpressClass(args) extends HFileKijiJob(args) {
 *   // Your code here.
 *   .write(HFileKijiOutput(tableUri = "kiji://localhost:2181/default/mytable",
 *       hFileOutput = "my_hfiles",
 *       timestampField = 'timestamps,
 *       'column1 -> "info:column1",
 *       'column2 -> "info:column2"))
 * }
 * }}}
 *
 *     NOTE: To properly work with dumping to HFiles, the argument --hfile-output must be provided.
 *     This argument specifies the location where the HFiles will be written upon job completion.
 *     Also required is the --output flag. This argument specifies the Kiji table to use to obtain
 *     layout information to properly format the HFiles for bulk loading.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
class HFileKijiJob(args: Args) extends KijiJob(args) {

  override def buildFlow(implicit mode: Mode): Flow[_] = {
    modifyFlowDef()
    val flow = super.buildFlow
    // Here we set the strategy to change the sink steps since we are dumping to HFiles.
    flow.setFlowStepStrategy(HFileFlowStepStrategy)
    flow.writeDOT("post-flow.dot")
    flow.writeStepsDOT("post-steps.dot")
    flow
  }

  def modifyFlowDef(): Unit = {
    val sinks: java.util.Map[String, Tap[_, _, _]] = flowDef.getSinks
    val tails: java.util.List[Pipe] = flowDef.getTails

    val hfileSinks = sinks.asScala.collectFirst { case (name, _: HFileKijiTap) => name }.toSet

    if (!hfileSinks.isEmpty) {
      val tailsMap = flowDef.getTails.asScala.map((p: Pipe) => p.getName -> p).toMap
      val flow: Flow[JobConf] = super.buildFlow.asInstanceOf[Flow[JobConf]]
      flow.writeDOT("pre-flow.dot")
      flow.writeStepsDOT("pre-steps.dot")

      for {
        flowStep <- flow.getFlowSteps.asScala
        sink <- flowStep.getSinks.asScala
        name <- flowStep.getSinkName(sink).asScala if hfileSinks(name)
      } {
        val tail = tailsMap(name)
        if (flowStep.getConfig.getNumReduceTasks > 0) {
          tails.remove(tail)
          flowDef.addTail(new Pipe(name, new Checkpoint(tail.getPrevious.head)))
        }
      }
    }
  }
}
