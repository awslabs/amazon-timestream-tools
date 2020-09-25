package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.timestream.TimestreamPoint;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import java.util.HashMap;
import java.util.Map;

public class JsonToTimestreamPayloadFn extends RichMapFunction<String, TimestreamPoint> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public TimestreamPoint map(String jsonString) throws Exception {
        HashMap<String,String> map = new Gson().fromJson(jsonString,
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
}
