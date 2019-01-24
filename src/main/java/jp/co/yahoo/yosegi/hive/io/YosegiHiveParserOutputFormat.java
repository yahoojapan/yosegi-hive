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

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class YosegiHiveParserOutputFormat extends FileOutputFormat<NullWritable,ParserWritable>
    implements HiveOutputFormat<NullWritable,ParserWritable> {

  @Override
  public RecordWriter<NullWritable, ParserWritable> getRecordWriter(
      final FileSystem ignored ,
      final JobConf job ,
      final String name ,
      final Progressable progress ) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      final JobConf job ,
      final Path outputPath ,
      final Class<? extends Writable> valueClass ,
      final boolean isCompressed ,
      final Properties tableProperties ,
      final Progressable progress ) throws IOException {
    Configuration config = new Configuration();
    if ( tableProperties.containsKey( "yosegi.spread.size" ) ) {
      String spreadSizeStr = tableProperties.getProperty( "yosegi.spread.size" );
      try {
        int spreadSize = Integer.valueOf( spreadSizeStr );
        config.set( "spread.size" , spreadSizeStr );
      } catch ( Exception ex ) {
        throw new IOException( ex );
      }
    }
    if ( tableProperties.containsKey( "yosegi.record.writer.max.rows" ) ) {
      String spreadMaxRowsStr = tableProperties.getProperty( "yosegi.record.writer.max.rows" );
      try {
        int spreadMaxRows = Integer.valueOf( spreadMaxRowsStr );
        config.set( "record.writer.max.rows" , spreadMaxRowsStr );
      } catch ( Exception ex ) {
        throw new IOException( ex );
      }
    }
    if ( tableProperties.containsKey( "yosegi.compression.class" ) ) {
      String compressionClass = tableProperties.getProperty( "yosegi.compression.class" );
      config.set( "spread.column.maker.default.compress.class" , compressionClass );
    }
    FileSystem fs = outputPath.getFileSystem( job );
    long dfsBlockSize = Math.max( fs.getDefaultBlockSize( outputPath ) , 1024 * 1024 * 256 );
    OutputStream out = fs.create(
        outputPath , true , 4096 , fs.getDefaultReplication( outputPath ) , dfsBlockSize );
    return new YosegiHiveRecordWriter( out , config );
  }

}
