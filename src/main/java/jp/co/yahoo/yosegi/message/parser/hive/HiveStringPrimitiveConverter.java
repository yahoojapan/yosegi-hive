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

import jp.co.yahoo.yosegi.message.objects.BytesStringObj;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.Charset;

public class HiveStringPrimitiveConverter implements IHivePrimitiveConverter {

  private final StringObjectInspector inspector;

  public HiveStringPrimitiveConverter( final StringObjectInspector inspector ) {
    this.inspector = inspector;
  }

  @Override
  public PrimitiveObject get(final Object target ) throws IOException {
    if ( target == null ) {
      return NullObj.getInstance();
    }
    Text text = inspector.getPrimitiveWritableObject( target );
    if ( text.getLength() == text.getBytes().length ) {
      return new BytesStringObj( text.getBytes() );
    } else {
      return new BytesStringObj( text.getBytes() , 0 , text.getLength() );
    }
  }

}
