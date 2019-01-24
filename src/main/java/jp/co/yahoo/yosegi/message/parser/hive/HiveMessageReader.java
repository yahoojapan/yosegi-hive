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

import jp.co.yahoo.yosegi.message.parser.IMessageReader;
import jp.co.yahoo.yosegi.message.parser.IParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;

public class HiveMessageReader implements IMessageReader {

  public IHiveParser create( final ObjectInspector inspector ) throws IOException {
    return HiveParserFactory.get( inspector );
  }

  /**
   * Create IParser from ObjectInspector.
   */
  public IParser create( final ObjectInspector inspector , final Object obj ) throws IOException {
    IHiveParser parser = HiveParserFactory.get( inspector );
    parser.setObject( obj );
    return parser;
  }

  @Override
  public IParser create( final byte[] message ) throws IOException {
    throw new UnsupportedOperationException( "Unsupport create( byte[] message )" );
  }

  @Override
  public IParser create(
      final byte[] message , final int start , final int length ) throws IOException {
    throw new UnsupportedOperationException(
        "Unsupport create( byte[] message , int start , int length )" );
  }

}
