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
package jp.co.yahoo.yosegi.hive.blackbox.line;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.hive.blackbox.ColumnBinaryTestCase;
import jp.co.yahoo.yosegi.hive.blackbox.YosegiFileTestCase;
import jp.co.yahoo.yosegi.hive.YosegiObjectInspectorFactory;
import jp.co.yahoo.yosegi.hive.io.ColumnAndIndex;
import jp.co.yahoo.yosegi.hive.io.YosegiHiveLineReader;
import jp.co.yahoo.yosegi.hive.io.vector.DoubleColumnVectorAssignor;
import jp.co.yahoo.yosegi.hive.io.vector.FloatPrimitiveSetter;
import jp.co.yahoo.yosegi.hive.io.vector.IColumnVectorAssignor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public final class TestFloatVector {

  public static void createTestCase(List<Arguments> args, float[] data, boolean[] isNullArray) throws IOException {
    String[] testClassArray = ColumnBinaryTestCase.floatClassNames();
    for (int i = 0; i < testClassArray.length; i++) {
      args.add(arguments(testClassArray[i], data, isNullArray, isNullArray.length));
    }
  }

  public static Stream<Arguments> data() throws IOException {
    // 通常のデータ
    List<Arguments> args = new ArrayList<Arguments>();
    createTestCase(args, new float[]{(float) 10, (float) 20, (float) 30, (float) 40, (float) 50, (float) 10, (float) 10, (float) 20, (float) 20, (float) 30}, new boolean[]{false, false, false, false, false, false, false, false, false, false});
    createTestCase(args, new float[]{(float) 0, (float) 20, (float) 0, (float) 40, (float) 0, (float) 10, (float) 0, (float) 20, (float) 0, (float) 30}, new boolean[]{true, false, true, false, true, false, true, false, true, false});
    createTestCase(args, new float[]{(float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 10, (float) 10, (float) 20, (float) 20, (float) 30}, new boolean[]{true, true, true, true, true, false, false, false, false, false});
    createTestCase(args, new float[]{(float) 10, (float) 10, (float) 20, (float) 20, (float) 30, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0}, new boolean[]{false, false, false, false, false, true, true, true, true, true});
    createTestCase(args, new float[]{(float) 10, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 10}, new boolean[]{false, true, true, true, true, true, true, true, true, false});

    // 固定の値のデータ
    createTestCase(args, new float[]{(float) 10, (float) 10, (float) 10, (float) 10, (float) 10, (float) 10, (float) 10, (float) 10, (float) 10, (float) 10}, new boolean[]{false, false, false, false, false, false, false, false, false, false});
    createTestCase(args, new float[]{(float) 0, (float) 10, (float) 0, (float) 10, (float) 0, (float) 10, (float) 0, (float) 10, (float) 0, (float) 10}, new boolean[]{true, false, true, false, true, false, true, false, true, false});
    createTestCase(args, new float[]{(float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 10, (float) 10, (float) 10, (float) 10, (float) 10}, new boolean[]{true, true, true, true, true, false, false, false, false, false});
    createTestCase(args, new float[]{(float) 10, (float) 10, (float) 10, (float) 10, (float) 10, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0}, new boolean[]{false, false, false, false, false, true, true, true, true, true});
    createTestCase(args, new float[]{(float) 10, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 10}, new boolean[]{false, true, true, true, true, true, true, true, true, false});
    createTestCase(args, new float[]{(float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 10}, new boolean[]{true, true, true, true, true, true, true, true, true, false});
    createTestCase(args, new float[]{(float) 10, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0, (float) 0}, new boolean[]{false, true, true, true, true, true, true, true, true, true});
    return args.stream();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue( String targetClassName , float[] data , boolean[] isNullArray ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createFloatColumnBinaryFromFloat(targetClassName, data, isNullArray, "col");
    IColumn column = ColumnBinaryTestCase.toColumn( columnBinary , 10 );
    DoubleColumnVector vector = new DoubleColumnVector( column.size() );
    IColumnVectorAssignor assignor = new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );

    assignor.setColumn( column.size() , column );
    int offset = column.size() / 2;
    assignor.setColumnVector( vector , 0 , offset );
    for( int i = 0 ; i < offset ; i++ ) {
      if( vector.isNull[i] ) {
        assertTrue(isNullArray[i]);
      } else {
        assertEquals( (float)(vector.vector[i]) , data[i] );
      }
    }
    vector.reset();
    assignor.setColumnVector( vector , offset , (column.size() - offset) );
    for( int i = 0 ; i < column.size() - offset ; i++ ){
      if( vector.isNull[i] ) {
        assertTrue(isNullArray[i + offset]);
      } else {
        assertEquals( (float)(vector.vector[i]) , data[i + offset] );
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withAllValueIndex( String targetClassName , float[] data , boolean[] isNullArray ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createFloatColumnBinaryFromFloat(targetClassName, data, isNullArray, "col");
    columnBinary.setRepetitions(new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 10);
    IColumn column = ColumnBinaryTestCase.toColumn( columnBinary , 10 );
    DoubleColumnVector vector = new DoubleColumnVector( column.size() );
    IColumnVectorAssignor assignor = new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );

    assignor.setColumn( column.size() , column );
    int offset = column.size() / 2;
    assignor.setColumnVector( vector , 0 , offset );
    for( int i = 0 ; i < offset ; i++ ) {
      if( vector.isNull[i] ){
        assertTrue(isNullArray[i]);
      } else {
        assertEquals( (float)(vector.vector[i]) , data[i] );
      }
    }
    vector.reset();
    assignor.setColumnVector( vector , offset , (column.size() - offset) );
    for( int i = 0 ; i < column.size() - offset ; i++ ) {
      if( vector.isNull[i] ) {
        assertTrue(isNullArray[i + offset]);
      } else {
        assertEquals( (float)(vector.vector[i]) , data[i + offset] );
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLargeLoadIndex( String targetClassName , float[] data , boolean[] isNullArray ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createFloatColumnBinaryFromFloat(targetClassName, data, isNullArray, "col");
    columnBinary.setRepetitions(new int[]{1, 1, 1, 1, 1}, 5);
    IColumn column = ColumnBinaryTestCase.toColumn( columnBinary , 5 );
    DoubleColumnVector vector = new DoubleColumnVector( 5 );
    IColumnVectorAssignor assignor = new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );

    assignor.setColumn( 5 , column );
    int offset = 5 / 2;
    assignor.setColumnVector( vector , 0 , offset );
    for( int i = 0 ; i < offset ; i++ ){
      if( ! vector.isNull[i] ) {
        assertEquals( (float)(vector.vector[i]) , data[i] );
      }
    }
    vector.reset();
    assignor.setColumnVector( vector , offset , (5 - offset) );
    for( int i = 0 ; i < 5 - offset ; i++ ) {
      if( ! vector.isNull[i] ) {
        assertEquals( (float)(vector.vector[i]) , data[i + offset] );
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLoadIndexIsTail5( String targetClassName , float[] data , boolean[] isNullArray ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createFloatColumnBinaryFromFloat(targetClassName, data, isNullArray, "col");
    columnBinary.setRepetitions(new int[]{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, 5);
    IColumn column = ColumnBinaryTestCase.toColumn( columnBinary , 5 );
    DoubleColumnVector vector = new DoubleColumnVector( 5 );
    IColumnVectorAssignor assignor = new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );

    assignor.setColumn( 5 , column );
    int offset = 5 / 2;
    assignor.setColumnVector( vector , 0 , offset );
    for( int i = 0 ; i < offset ; i++ ) {
      if( ! vector.isNull[i] ) {
        assertEquals( (float)(vector.vector[i]) , data[i + 5] );
      }
    }
    vector.reset();
    assignor.setColumnVector( vector , offset , (5 - offset) );
    for( int i = 0 ; i < 5 - offset ; i++ ) {
      if( ! vector.isNull[i] ) {
        assertEquals( (float)(vector.vector[i]) , data[i + offset + 5] );
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withAllValueIndexAndExpand( String targetClassName , float[] data , boolean[] isNullArray ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createFloatColumnBinaryFromFloat(targetClassName, data, isNullArray, "col");
    int[] loadIndex = new int[]{2, 1, 2, 1, 2, 1, 2, 1, 2, 1};
    columnBinary.setRepetitions(loadIndex, 15);
    IColumn column = ColumnBinaryTestCase.toColumn( columnBinary , 15 );
    DoubleColumnVector vector = new DoubleColumnVector( 15 );
    IColumnVectorAssignor assignor = new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );

    assignor.setColumn( 15 , column );
    assignor.setColumnVector( vector , 0 , 15 );
    int index = 0;
    for ( int i = 0 ; i < loadIndex.length ; i++ ) {
      for ( int n = index ; n < index + loadIndex[i] ; n++ ) {
        if (isNullArray[i]) {
          assertTrue( vector.isNull[index] );
        } else {
          assertFalse( vector.isNull[index] );
          assertEquals( data[i] , (float)(vector.vector[n]) );
        }
      }
      index += loadIndex[i];
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLargeLoadIndexAndExpand( String targetClassName , float[] data , boolean[] isNullArray ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createFloatColumnBinaryFromFloat(targetClassName, data, isNullArray, "col");
    int[] loadIndex = new int[]{2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
    columnBinary.setRepetitions(loadIndex, 20);
    IColumn column = ColumnBinaryTestCase.toColumn( columnBinary , 20 );
    DoubleColumnVector vector = new DoubleColumnVector( 20 );
    IColumnVectorAssignor assignor = new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );

    assignor.setColumn( 20 , column );
    assignor.setColumnVector( vector , 0 , 20 );
    int index = 0;
    for ( int i = 0 ; i < loadIndex.length ; i++ ) {
      for ( int n = index ; n < index + loadIndex[i] ; n++ ) {
        if ( i < isNullArray.length ) {
          if (isNullArray[i]) {
            assertTrue( vector.isNull[index] );
          } else {
            assertFalse( vector.isNull[index] );
            assertEquals( data[i] , (float)(vector.vector[n]) );
          }
        } else {
          assertTrue( vector.isNull[index] );
        }
      }
      index += loadIndex[i];
    }
  }
}
