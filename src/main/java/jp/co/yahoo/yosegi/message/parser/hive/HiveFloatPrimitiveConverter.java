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

import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;

import java.io.IOException;

public class HiveFloatPrimitiveConverter implements IHivePrimitiveConverter {

  private final FloatObjectInspector inspector;

  public HiveFloatPrimitiveConverter( final FloatObjectInspector inspector ) {
    this.inspector = inspector;
  }

  @Override
  public PrimitiveObject get(final Object target ) throws IOException {
    if ( target == null ) {
      return NullObj.getInstance();
    }
    return new FloatObj( inspector.get( target ) );
  }

}
