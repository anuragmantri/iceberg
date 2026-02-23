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
package org.apache.spark.sql.catalyst.analysis

/**
 * Thread-local context to pass updated column names from optimizer to write builder.
 * Used for column-update mode to track which columns are being updated.
 */
object ColumnUpdateContext {
  private val updatedColumns = new ThreadLocal[Set[String]]()

  def setUpdatedColumns(columns: Set[String]): Unit = {
    updatedColumns.set(columns)
  }

  def getUpdatedColumns: Set[String] = {
    Option(updatedColumns.get()).getOrElse(Set.empty)
  }

  def clear(): Unit = {
    updatedColumns.remove()
  }
}
