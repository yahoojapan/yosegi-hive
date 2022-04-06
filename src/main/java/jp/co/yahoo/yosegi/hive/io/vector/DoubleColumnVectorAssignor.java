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
import jp.co.yahoo.yosegi.spread.column.IColumn;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;

import java.io.IOException;

public class DoubleColumnVectorAssignor implements IColumnVectorAssignor {

  private final IDecimalPrimitiveSetter setter;
  private IColumn column;

  public DoubleColumnVectorAssignor( final IDecimalPrimitiveSetter setter ) {
    this.setter = setter;
  }

  @Override
  public void setColumn( final int spreadSize , final IColumn column ) throws IOException {
    this.column = column;
  }

  @Override
  public void setColumnVector(
      final ColumnVector vector ,
      final int start ,
      final int length ) throws IOException {
    DoubleColumnVector columnVector = (DoubleColumnVector)vector;
    for ( int i = 0 ; i < length ; i++ ) {
      PrimitiveObject primitiveObject = ColumnUtil.getPrimitiveObject( column , i + start );
      if ( primitiveObject == null ) {
        VectorizedBatchUtil.setNullColIsNullValue( columnVector , i );
      } else {
        setter.set( primitiveObject , columnVector , i );
      }
    }
  }

}
