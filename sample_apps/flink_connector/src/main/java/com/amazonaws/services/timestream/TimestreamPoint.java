package com.amazonaws.services.timestream;

import com.amazonaws.services.timestreamwrite.model.MeasureValueType;

import java.util.HashMap;
import java.util.Map;

public class TimestreamPoint
{
    private String measureName;
    private MeasureValueType measureValueType;
    private String measureValue;
    private long time;
    private String timeUnit;
    private Map<String, String> dimensions;

    public TimestreamPoint() {
        this.dimensions = new HashMap<>();
    }

    public TimestreamPoint(long time, Map<String, String> dimensions,
                           String measureName, String measureValueType, String measureValue)
    {
        this.time = time;
        this.dimensions = dimensions;
        this.measureName = measureName;
        this.measureValueType = MeasureValueType.fromValue(measureValueType.toUpperCase());
        this.measureValue = measureValue;
    }

    public String getMeasureName()
    {
        return measureName;
    }

    public void setMeasureName(String measureValue)
    {
        this.measureName = measureValue;
    }

    public String getMeasureValue()
    {
        return measureValue;
    }

    public void setMeasureValue(String measureValue)
    {
        this.measureValue = measureValue;
    }

    public MeasureValueType getMeasureValueType()
    {
        return measureValueType;
    }

    public void setMeasureValueType(MeasureValueType measureValueType)
    {
        this.measureValueType = measureValueType;
    }

    public void setMeasureValueType(String measureValueType) {
        this.measureValueType = MeasureValueType.fromValue(measureValueType.toUpperCase());
    }

    public long getTime()
    {
        return time;
    }

    public void setTime(long time)
    {
        this.time = time;
    }

    public String getTimeUnit()
    {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit)
    {
        this.timeUnit = timeUnit;
    }

    public Map<String, String> getDimensions()
    {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dims)
    {
        this.dimensions = dims;
    }

    public void addDimension(String dimensionName, String dimensionValue) {
        dimensions.put(dimensionName, dimensionValue);
    }
}
