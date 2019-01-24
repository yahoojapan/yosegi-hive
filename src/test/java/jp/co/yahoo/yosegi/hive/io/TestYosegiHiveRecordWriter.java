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
package jp.co.yahoo.yosegi.hive.io;

import java.io.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.expression.AllExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.*;
import jp.co.yahoo.yosegi.message.parser.json.*;
import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.*;

public class TestYosegiHiveRecordWriter{

  @Test
  public void T_1() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiHiveRecordWriter writer = new YosegiHiveRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "io/TestYosegiHiveRecordWriter.json" ).openStream() ) );
    String line = in.readLine();
    ParserWritable writable = new ParserWritable();
    while( line != null ){
      writable.set( messageReader.create( line ) );
      writer.write( writable );
      line = in.readLine();
    }
    writer.close( false );

    YosegiReader reader = new YosegiReader();
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while( reader.hasNext() ){
      Spread spread = reader.next();
      IColumn key1Column = spread.getColumn( "key1" );
      IExpressionIndex indexList = new AllExpressionIndex( spread.size() );
      PrimitiveObject[] primitiveArray = key1Column.getPrimitiveObjectArray( indexList , 0 , spread.size() );
      assertEquals( 7 , primitiveArray.length );
      assertEquals( "a" , primitiveArray[0].getString() );
      assertEquals( "b" , primitiveArray[1].getString() );
      assertEquals( "a" , primitiveArray[2].getString() );
      assertEquals( "b" , primitiveArray[3].getString() );
      assertEquals( "a" , primitiveArray[4].getString() );
      assertEquals( "b" , primitiveArray[5].getString() );
      assertEquals( "a" , primitiveArray[6].getString() );
    }
  }

}
