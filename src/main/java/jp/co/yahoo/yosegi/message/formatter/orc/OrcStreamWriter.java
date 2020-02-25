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

package jp.co.yahoo.yosegi.message.formatter.orc;

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.hive.HiveSchemaFactory;
import jp.co.yahoo.yosegi.message.formatter.IStreamWriter;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OrcStreamWriter implements IStreamWriter {

  private static final long HIVE_ORC_DEFAULT_STRIPE_SIZE = 1024 * 1024 * 128;
  private static final int HIVE_ORC_DEFAULT_BUFFER_SIZE = 1024 * 256;
  private static final int HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE = 10000;

  private final Writer writer;
  private final IOrcFormatter formatter;

  /**
   * Create new instance.
   */
  public OrcStreamWriter(
      final Configuration config,
      final Path path,
      final String schema ) throws IOException {
    FileSystem fs = FileSystem.get(config);
    long stripeSize = HIVE_ORC_DEFAULT_STRIPE_SIZE;
    CompressionKind compress = CompressionKind.ZLIB;
    int bufferSize = HIVE_ORC_DEFAULT_BUFFER_SIZE;
    int rowIndexStride =  HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE;

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString( schema );
    ObjectInspector inspector = TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo( typeInfo );
    writer = OrcFile.createWriter(
        fs, path, config, inspector, stripeSize, compress, bufferSize, rowIndexStride );
    formatter = OrcFormatterFactory.get( typeInfo );
  }

  /**
   * Create new instance.
   */
  public OrcStreamWriter(
      final Configuration config,
      final Path path,
      final TypeInfo typeInfo ) throws IOException {
    FileSystem fs = FileSystem.get(config);
    long stripeSize = HIVE_ORC_DEFAULT_STRIPE_SIZE;
    CompressionKind compress = CompressionKind.ZLIB;
    int bufferSize = HIVE_ORC_DEFAULT_BUFFER_SIZE;
    int rowIndexStride = HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE;

    ObjectInspector inspector = TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo( typeInfo );
    writer = OrcFile.createWriter(
        fs, path, config, inspector, stripeSize, compress, bufferSize, rowIndexStride );
    formatter = OrcFormatterFactory.get( typeInfo );
  }

  /**
   * Create new instance.
   */
  public OrcStreamWriter(
      final Configuration config,
      final Path path,
      final IField schema ) throws IOException {
    FileSystem fs = FileSystem.get(config);
    long stripeSize = HIVE_ORC_DEFAULT_STRIPE_SIZE;
    CompressionKind compress = CompressionKind.ZLIB;
    int bufferSize = HIVE_ORC_DEFAULT_BUFFER_SIZE;
    int rowIndexStride =  HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE;
    TypeInfo typeInfo = HiveSchemaFactory.getHiveSchema( schema ); 

    ObjectInspector inspector =
        TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo );

    writer = OrcFile.createWriter(
        fs, path, config, inspector, stripeSize, compress, bufferSize, rowIndexStride );
    formatter = OrcFormatterFactory.get( typeInfo );
  }

  @Override
  public void write( final PrimitiveObject obj ) throws IOException {
    writer.addRow( formatter.write( obj ) );
  }

  @Override
  public void write( final List<Object> array ) throws IOException {
    writer.addRow( formatter.write( array ) );
  }

  @Override
  public void write( final Map<Object,Object> map ) throws IOException {
    writer.addRow( formatter.write( map ) );
  }

  @Override
  public void write( final IParser parser ) throws IOException {
    writer.addRow( formatter.writeFromParser( null , parser ) );
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

}
