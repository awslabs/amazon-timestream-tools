package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;
import java.util.TimeZone;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TimestreamPointToAverage implements WindowFunction<TimestreamPoint, TimestreamPoint, Integer, TimeWindow> {
  @Override
  public void apply(Integer key, TimeWindow timeWindow, Iterable<TimestreamPoint> iterable, Collector<TimestreamPoint> collector) {

    //calculate the average
    double sumPoint = StreamSupport
        .stream(iterable.spliterator(), false)
        .mapToDouble(point -> Double.parseDouble(point.getMeasureValue()))
        .sum();

    double avgMeasureValue = sumPoint / Iterables.size(iterable);

    //create a new point to store the averaged point
    TimestreamPoint dataPoint = new TimestreamPoint();
    
    //get the maximum timestamp from the time window, set as the time for the averaged point
    long maxTime = timeWindow.getEnd();
    dataPoint.setTime(maxTime);
    dataPoint.setTimeUnit("MILLISECONDS");
    
    //get and set the measure name and value type
    String measureName = Iterables.get(iterable, 0).getMeasureName();
    dataPoint.setMeasureName("avg_" + measureName);
    dataPoint.setMeasureValue(String.valueOf(avgMeasureValue));
    dataPoint.setMeasureValueType("DOUBLE");
    
    //get and set all dimensions for the point
    Map<String, String> dimensions = Iterables.get(iterable, 0).getDimensions();
    dataPoint.setDimensions(dimensions);

    //set window start and window end dimensions
    long minTime = timeWindow.getStart();
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
    
    dataPoint.addDimension("window_start", format.format(minTime));
    dataPoint.addDimension("window_end", format.format(maxTime));
    
    collector.collect(dataPoint);
  }
}