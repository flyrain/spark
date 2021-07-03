/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.config.UI._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "executors") {

  init()

  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)

    val ajaxEnabled = parent.conf.getBoolean("spark.ui.ajax.enabled", true)
    attachPage(new ExecutorsPage(this, threadDumpEnabled, ajaxEnabled, store))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
  }

}

private[ui] class ExecutorsPage(
    parent: SparkUITab,
    threadDumpEnabled: Boolean,
    ajaxEnabled: Boolean,
    store: AppStatusStore)
  extends WebUIPage("") {

  def executorList(): Seq[ExecutorSummary] = {
    store.executorList(false).map { summary =>
      val escaped = summary.removeReason.map { r =>
        // JSON doesn't allow multiline strings.
        r.replaceAll("\n", "\\\\n")
      }
      new ExecutorSummary(
        summary.id,
        summary.hostPort,
        summary.isActive,
        summary.rddBlocks,
        summary.memoryUsed,
        summary.diskUsed,
        summary.totalCores,
        summary.maxTasks,
        summary.activeTasks,
        summary.failedTasks,
        summary.completedTasks,
        summary.totalTasks,
        summary.totalDuration,
        summary.totalGCTime,
        summary.totalInputBytes,
        summary.totalShuffleRead,
        summary.totalShuffleWrite,
        summary.isExcluded,
        summary.maxMemory,
        summary.addTime,
        summary.removeTime,
        escaped,
        summary.executorLogs,
        summary.memoryMetrics,
        summary.blacklistedInStages,
        summary.peakMemoryMetrics,
        summary.attributes,
        summary.resources,
        summary.resourceProfileId,
        summary.isExcluded,
        summary.excludedInStages)
    }
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    def allExecutorsDataScript: Seq[Node] = {
      <script>
        {Unparsed {
        "var allExecutorsData='" + mapper.writeValueAsString(executorList) + "';"
      }}
      </script>
    }
    val content =
      {
        <div id="active-executors"></div> ++
        <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
        <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
        <script src={UIUtils.prependBaseUri(request,
            "/static/executorspage-template.js")}></script> ++
        {if (!ajaxEnabled) allExecutorsDataScript else Seq.empty} ++
        <script>setAjaxEnabled({ajaxEnabled})</script>
        <script>setThreadDumpEnabled({threadDumpEnabled})</script>
      }

    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
}
