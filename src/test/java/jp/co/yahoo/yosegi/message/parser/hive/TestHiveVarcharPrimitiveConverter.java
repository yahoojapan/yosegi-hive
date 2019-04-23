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
package jp.co.yahoo.yosegi.message.parser.hive;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import java.io.IOException;

public class TestHiveVarcharPrimitiveConverter {

  @Test
  public void T_1() throws IOException{
    HiveVarcharPrimitiveConverter converter = new HiveVarcharPrimitiveConverter(
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector );

    HiveVarcharWritable charObj = new HiveVarcharWritable();
    charObj.set( "This is test string." );
    PrimitiveObject tObj = converter.get( charObj );
    assertEquals( "This is test string." , tObj.getString() );
  }

  @Test
  public void T_2() throws IOException{
    HiveVarcharPrimitiveConverter converter = new HiveVarcharPrimitiveConverter(
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector );

    PrimitiveObject tObj = converter.get( null );
    assertTrue( ( tObj instanceof NullObj ) );
  }

}
