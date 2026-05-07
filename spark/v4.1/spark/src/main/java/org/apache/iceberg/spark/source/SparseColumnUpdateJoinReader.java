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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.types.Types;

/**
 * Constants for sparse column update files.
 *
 * <p>The {@code _pos} column in update files uses a regular field ID (not a metadata column ID) so
 * the Parquet reader reads the actual stored values instead of generating synthetic positions.
 */
public class SparseColumnUpdateJoinReader {

  public static final int STORED_POS_FIELD_ID =
      2147483545; // Integer.MAX_VALUE - 102, avoids metadata range
  public static final Types.NestedField STORED_POS_FIELD =
      Types.NestedField.required(STORED_POS_FIELD_ID, "_pos", Types.LongType.get());

  private SparseColumnUpdateJoinReader() {}
}
