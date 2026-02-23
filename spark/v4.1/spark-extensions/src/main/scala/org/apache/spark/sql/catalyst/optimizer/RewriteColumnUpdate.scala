/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.catalyst.optimizer

import org.apache.iceberg.MetadataColumns
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.ColumnUpdateContext
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

// TODO gaborkaszab: apparently I can't change the output schema of the plan here because the plan
// is already resolved and changing it will 'unresolve' the plan. Instead I gather the columns that
// were updated by the UPDATE command.

// Warning: AI-generated code

/**
 * Detects column-update mode for UPDATE operations and extracts the updated column names.
 * The actual schema filtering happens in SparkWriteBuilder using the ColumnUpdateContext.
 *
 * This rule does NOT modify the plan structure - it only extracts information about
 * which columns are being updated and stores it for use in the write path.
 */
object RewriteColumnUpdate extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreachUp {
      case ReplaceData(table, _, query, originalTable, _, _, _) =>
        // Check if we're in column-update mode
        if (isColumnUpdateMode(table) || isColumnUpdateMode(originalTable)) {
          logDebug("Column update mode detected, extracting updated columns")
          // Extract updated column names from the query and store in context
          val updatedColumns = findUpdatedColumnNames(query)
          if (updatedColumns.nonEmpty) {
            logDebug(s"Updated columns: $updatedColumns")
            ColumnUpdateContext.setUpdatedColumns(updatedColumns)
          }
        }
      case _ => // ignore other plans
    }
    // Return the plan unchanged
    plan
  }

  /**
   * Check if the table is configured for column-update mode.
   */
  private def isColumnUpdateMode(table: NamedRelation): Boolean = {
    table match {
      case DataSourceV2Relation(t, _, _, _, _, _) =>
        t match {
          case opTable: RowLevelOperationTable =>
            getIcebergTableProperty(opTable.table)
          case sparkTable: org.apache.iceberg.spark.source.SparkTable =>
            checkColumnUpdateMode(sparkTable.table())
          case _ => false
        }
      case _ => false
    }
  }

  private def getIcebergTableProperty(table: Any): Boolean = {
    table match {
      case icebergTable: org.apache.iceberg.Table =>
        checkColumnUpdateMode(icebergTable)
      case sparkTable: org.apache.iceberg.spark.source.SparkTable =>
        checkColumnUpdateMode(sparkTable.table())
      case _ => false
    }
  }

  private def checkColumnUpdateMode(icebergTable: org.apache.iceberg.Table): Boolean = {
    val modeName = icebergTable
      .properties()
      .getOrDefault("write.update.mode", "copy-on-write")
    modeName.equalsIgnoreCase("column-update")
  }

  /**
   * Find the names of columns that are being updated in the query.
   * Returns a Set of column names that have been modified.
   */
  private def findUpdatedColumnNames(query: LogicalPlan): Set[String] = {
    val projectOpt = findInnermostProject(query)

    projectOpt match {
      case Some(Project(projectList, _)) =>
        projectList.flatMap {
          // Skip metadata columns
          case e if MetadataColumns.isMetadataColumn(e.name) =>
            None

          // Case 1: Alias with If expression (conditional update)
          case Alias(If(_, trueValue, falseValue: AttributeReference), name) =>
            if (!trueValue.isInstanceOf[AttributeReference] ||
              trueValue.asInstanceOf[AttributeReference].name != falseValue.name) {
              Some(name)
            } else {
              None
            }

          // Case 2: Alias with Literal (unconditional update to constant)
          case Alias(_: Literal, name) =>
            Some(name)

          // Case 3: Alias where child is NOT a simple pass-through
          case Alias(child, name) if !isPassThrough(child, name) =>
            Some(name)

          // Case 4: Simple pass-through - NOT an update
          case _: AttributeReference =>
            None

          case _ =>
            None
        }.toSet

      case _ =>
        Set.empty
    }
  }

  private def isPassThrough(
      expr: org.apache.spark.sql.catalyst.expressions.Expression,
      name: String): Boolean = {
    expr match {
      case attr: AttributeReference => attr.name == name
      case _ => false
    }
  }

  private def findInnermostProject(plan: LogicalPlan): Option[Project] = {
    plan match {
      case p: Project =>
        p.child match {
          case childProject: Project => findInnermostProject(childProject)
          case _ => Some(p)
        }
      case _ =>
        plan.children.flatMap(findInnermostProject).headOption
    }
  }
}
