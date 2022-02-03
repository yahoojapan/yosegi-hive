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
package jp.co.yahoo.yosegi.hive.blackbox;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.hive.io.DummyJobReporter;
import jp.co.yahoo.yosegi.hive.io.HiveReaderSetting;
import jp.co.yahoo.yosegi.hive.io.SpreadCounter;
import jp.co.yahoo.yosegi.hive.io.YosegiHiveLineReader;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.writer.YosegiWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public final class YosegiFileTestCase {

  public static byte[] createYosegiFileBinary(
      final List<List<ColumnBinary>> columnBinaryLists ,
      final List<Integer> spreadSizeList ) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );
    for ( int i = 0 ; i < columnBinaryLists.size() ; i++ ) {
      writer.appendRow( columnBinaryLists.get(i) , spreadSizeList.get(i) );
    }
    writer.close();
    return out.toByteArray();
  }

  public static YosegiHiveLineReader createYosegiHiveLineReader(
      final byte[] yosegiFile ,
      final Configuration config ,
      final IExpressionNode node ) throws IOException {
    HiveReaderSetting readerSetting = new HiveReaderSetting(
        config , node , false , false , false );
    return new YosegiHiveLineReader(
        new ByteArrayInputStream( yosegiFile ) ,
        yosegiFile.length ,
        0 ,
        yosegiFile.length ,
        readerSetting , 
        new DummyJobReporter() ,
        new SpreadCounter() );
  }

  public static YosegiHiveLineReader createYosegiFileBinaryAndCreateLineReader(
      final List<List<ColumnBinary>> columnBinaryLists ,
      final List<Integer> spreadSizeList ,
      final Configuration config ,
      final IExpressionNode node ) throws IOException {
    return createYosegiHiveLineReader(
        createYosegiFileBinary( columnBinaryLists , spreadSizeList ) ,
        config ,
        null );
  }
}
