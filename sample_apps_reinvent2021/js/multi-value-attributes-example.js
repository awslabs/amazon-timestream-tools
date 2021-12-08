const constants = require('./constants');

async function writeRecordsWithMultiMeasureValueSingleRecord() {
    console.log("Writing records with Multi Measure Value single record");
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const dimensions = [
        {'Name': 'region', 'Value': 'us-east-1'},
        {'Name': 'az', 'Value': 'az1'},
        {'Name': 'hostname', 'Value': 'host1'}
    ];

    const commonAttributes = {
        'Dimensions': dimensions,
        'Time': currentTime.toString()
    };

    const cpuUtilization = {
        'Name': 'cpu_utilization',
        'Value': '13.5',
        'Type': 'DOUBLE',
    };

    const memoryUtilization = {
        'Name': 'memory_utilization',
        'Value': '40',
        'Type': 'DOUBLE',
    };

    const computationalRecord = {
        'MeasureName': 'cpu_memory',
        'MeasureValues': [cpuUtilization, memoryUtilization],
        'MeasureValueType': 'MULTI',
    }

    const records = [computationalRecord];

    const params = {
        DatabaseName: constants.MEASURE_VALUE_SAMPLE_DB,
        TableName: constants.MEASURE_VALUE_SAMPLE_TABLE,
        Records: records,
        CommonAttributes: commonAttributes
    };

    const request = writeClient.writeRecords(params);

    await request.promise().then(
        (data) => {
            console.log("Write records successful");
        },
        (err) => {
            console.log("Error writing records:", err);
        }
    );
}

async function writeRecordsWithMultiMeasureValueMultipleRecordsMixture() {
    console.log("Writing records with Multi Measure Value with multiple records");
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const dimensions = [
        {'Name': 'region', 'Value': 'us-east-1'},
        {'Name': 'az', 'Value': 'az1'},
        {'Name': 'hostname', 'Value': 'host1'}
    ];

    const commonAttributes = {
        'Dimensions': dimensions,
        'Time': currentTime.toString()
    };

    const cpuUtilization = {
        'Name': 'cpu_utilization',
        'Value': '15',
        'Type': 'DOUBLE',
    };

    const memoryUtilization = {
        'Name': 'memory_utilization',
        'Value': '60',
        'Type': 'DOUBLE',
    };

    const activeCores = {
        'Name': 'active_cores',
        'Value': '4',
        'Type': 'BIGINT',
    }

    const computationalRecord = {
        'MeasureName': 'computation_utilization',
        'MeasureValues': [cpuUtilization, memoryUtilization, activeCores],
        'MeasureValueType': 'MULTI',
    }

    const records = [computationalRecord];

    const params = {
        DatabaseName: constants.MEASURE_VALUE_SAMPLE_DB,
        TableName: constants.MEASURE_VALUE_SAMPLE_TABLE,
        Records: records,
        CommonAttributes: commonAttributes
    };

    const request = writeClient.writeRecords(params);

    await request.promise().then(
        (data) => {
            console.log("Write records successful");
        },
        (err) => {
            console.log("Error writing records:", err);
        }
    );
}

module.exports = {
    writeRecordsWithMultiMeasureValueSingleRecord,
    writeRecordsWithMultiMeasureValueMultipleRecordsMixture,
}