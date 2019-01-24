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

import java.util.Map;
import java.util.HashMap;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.PerfectMatchStringFilter;
import jp.co.yahoo.yosegi.spread.expression.ExecuterNode;
import jp.co.yahoo.yosegi.spread.expression.OrExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.StringExtractNode;

import org.apache.hadoop.io.NullWritable;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.*;

public class TestYosegiHiveLineReader{

  private void createFile( final String path )throws IOException{
    OutputStream out = new FileOutputStream( path );
    Configuration config = new jp.co.yahoo.yosegi.config.Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    Map<String,Object> dataContainer = new HashMap<String,Object>();

    for( int i = 0 ; i < 3000 ; i++ ){
      dataContainer.put( "str" , new StringObj( "a-" + i ) );
      dataContainer.put( "num" , new IntegerObj( i ) );
      dataContainer.put( "num2" , new IntegerObj( i * 2 ) );
      writer.addRow( dataContainer );
    }
    writer.close();
  }

  private void createFile2( final String path )throws IOException{
    OutputStream out = new FileOutputStream( path );
    Configuration config = new jp.co.yahoo.yosegi.config.Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Map<String,Object> dataContainer = new HashMap<String,Object>();

    Spread s = new Spread();
    for( int i = 0 ; i < 3000 ; i++ ){
      dataContainer.put( "str" , new StringObj( "a-" + i ) );
      dataContainer.put( "num" , new IntegerObj( i ) );
      dataContainer.put( "num2" , new IntegerObj( i * 2 ) );
      s.addRow( dataContainer );
      if( ( i % 500 ) == 499 ){
        writer.append( s );
        s = new Spread();
      }
    }
    writer.close();
  }


  private HiveReaderSetting getHiveReaderSetting(){
    return new HiveReaderSetting( new Configuration() , new OrExpressionNode() , false , false , false );
  }

  private HiveReaderSetting getHiveReaderSetting2(){
    OrExpressionNode or = new OrExpressionNode();
    or.addChildNode( new ExecuterNode( new StringExtractNode( "str" ) , new PerfectMatchStringFilter( "a-0" ) ) );
    return new HiveReaderSetting( new Configuration() , or , false , false , false );
  }

  @Test
  public void T_allTest_1() throws IOException{
    String dirName = this.getClass().getClassLoader().getResource( "io/out" ).getPath();
    String outPath = String.format( "%s/TestYosegiHiveLineReader_T_allTest_1.yosegi" , dirName );
    createFile( outPath );

    HiveReaderSetting setting = getHiveReaderSetting();
    File inFile = new File( outPath );
    YosegiHiveLineReader reader = new YosegiHiveLineReader( new FileInputStream( inFile ) , inFile.length() , 0 , inFile.length() , setting , new DummyJobReporter() , new SpreadCounter() );
    NullWritable key = reader.createKey();
    ColumnAndIndex value = reader.createValue();
    int colCount = 0;
    while( reader.next( key , value ) ){
      IColumn spreadColumn = value.column;
      int index = value.index;
      int columnIndex = value.columnIndex;
      IColumn strColumn = spreadColumn.getColumn( "str" );
      PrimitiveObject strObj = (PrimitiveObject)( strColumn.get( index ).getRow() );
      assertEquals( "a-" + colCount , strObj.getString() );
      colCount++;
    }
    reader.getPos();
    reader.getProgress();
    reader.close();
  }

  @Test
  public void T_allTest_2() throws IOException{
    String dirName = this.getClass().getClassLoader().getResource( "io/out" ).getPath();
    String outPath = String.format( "%s/TestYosegiHiveLineReader_T_allTest_2.yosegi" , dirName );
    createFile2( outPath );

    HiveReaderSetting setting = getHiveReaderSetting2();
    File inFile = new File( outPath );
    YosegiHiveLineReader reader = new YosegiHiveLineReader( new FileInputStream( inFile ) , inFile.length() , 0 , inFile.length() , setting , new DummyJobReporter() , new SpreadCounter() );
    NullWritable key = reader.createKey();
    ColumnAndIndex value = reader.createValue();
    int colCount = 0;
    while( reader.next( key , value ) ){
      IColumn spreadColumn = value.column;
      int index = value.index;
      int columnIndex = value.columnIndex;
      IColumn strColumn = spreadColumn.getColumn( "str" );
      PrimitiveObject strObj = (PrimitiveObject)( strColumn.get( index ).getRow() );
      assertEquals( "a-" + colCount , strObj.getString() );
      colCount++;
    }
    reader.getPos();
    reader.getProgress();
    reader.close();
  }

  @Test
  public void T_allTest_3() throws IOException{
    String dirName = this.getClass().getClassLoader().getResource( "io/out" ).getPath();
    String outPath = String.format( "%s/TestYosegiHiveLineReader_T_allTest_3.yosegi" , dirName );
    createFile2( outPath );

    HiveReaderSetting setting = getHiveReaderSetting();
    File inFile = new File( outPath );
    YosegiHiveLineReader reader = new YosegiHiveLineReader( new FileInputStream( inFile ) , inFile.length() , 0 , inFile.length() , setting , new DummyJobReporter() , new SpreadCounter() );
    NullWritable key = reader.createKey();
    ColumnAndIndex value = reader.createValue();
    int colCount = 0;
    while( reader.next( key , value ) ){
      IColumn spreadColumn = value.column;
      int index = value.index;
      int columnIndex = value.columnIndex;
      IColumn strColumn = spreadColumn.getColumn( "str" );
      PrimitiveObject strObj = (PrimitiveObject)( strColumn.get( index ).getRow() );
      assertEquals( "a-" + colCount , strObj.getString() );
      colCount++;
    }
    reader.getPos();
    reader.getProgress();
    reader.close();
  }

}
