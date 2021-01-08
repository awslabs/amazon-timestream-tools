/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.timestream;

import com.amazonaws.services.timestream.TimestreamPoint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class TimestreamPointSchema implements DeserializationSchema<TimestreamPoint> {

  @Override
  public TimestreamPoint deserialize(byte[] bytes) {
      
    JsonReader jsonReader =  new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
    JsonElement jsonElement = Streams.parse(jsonReader);
    
    HashMap<String,String> map = new Gson().fromJson(jsonElement,
                new TypeToken<HashMap<String, String>>(){}.getType());
    TimestreamPoint dataPoint = new TimestreamPoint();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // assuming these fields are present in every JSON record
            switch (key.toLowerCase()) {
                case "time":
                    dataPoint.setTime(Long.parseLong(value));
                    break;
                case "timeunit":
                    dataPoint.setTimeUnit(value);
                    break;
                case "measurename":
                    dataPoint.setMeasureName(value);
                    break;
                case "measurevalue":
                    dataPoint.setMeasureValue(value);
                    break;
                case "measurevaluetype":
                    dataPoint.setMeasureValueType(value);
                    break;
                default:
                    dataPoint.addDimension(key, value);
            }
        }

    return dataPoint;
  }

  @Override
  public boolean isEndOfStream(TimestreamPoint point) {
    return false;
  }

  @Override
  public TypeInformation<TimestreamPoint> getProducedType() {
    return TypeExtractor.getForClass(TimestreamPoint.class);
  }

}