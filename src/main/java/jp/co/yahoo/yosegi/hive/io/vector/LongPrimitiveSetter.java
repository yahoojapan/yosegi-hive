/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.hive.io.vector;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;

import java.io.IOException;

public final class LongPrimitiveSetter implements INumberPrimitiveSetter {

  private static final LongPrimitiveSetter SETTER = new LongPrimitiveSetter();

  private LongPrimitiveSetter() {}

  @Override
  public void set(
      final PrimitiveObject primitiveObject ,
      final LongColumnVector columnVector ,
      final int index ) throws IOException {
    try {
      columnVector.vector[index] = primitiveObject.getLong();
    } catch ( NumberFormatException | NullPointerException ex ) {
      VectorizedBatchUtil.setNullColIsNullValue( columnVector , index );
    }
  }

  public static LongPrimitiveSetter getInstance() {
    return SETTER;
  }

}
