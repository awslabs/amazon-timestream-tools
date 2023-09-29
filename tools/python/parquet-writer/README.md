# Loading Apache Parquet files into Amazon Timestream

This example illustrates how to load Apache Parquet files and write the data to Amazon Timestream. Some of the characteristics are:

* Loading multiple files in a folder
* For higher ingestion speed multiple threads can be configured. The parameters are preconfigured for 4 threads

## Getting started

This example contains a sample parquet file with the following data structure:

| Field   | example content | mapping to Timestream attribute |
|---------|-----------------|---------------------------------|
| signal  | data channel | Dimension |
| source  | device indentifier | Dimension |
| time    | Timestamp | time |
| value   | example measure value | Multi Measure column |

The python code can be modified and is able to process more than one measure in multi-measure format. 
For modifying the code to your data, please change the following functions:

## Parquet data extraction:

### Function `load_parquet`

This function extracts the data needed for a record and simple transformation can be done here:

```python
def load_parquet(max_threads, folder_name):

...
        for df_record in df_records:
            buffer_index = record_count % max_threads
            time = unix_time_millis(df_record['time'])
            signal = df_record['signal']
            value = df_record['value']
            source = df_record['source']
            # print(df_record)
            row = {
                'time': time,
                'signal': signal,
                'value': value,
                'source': source
            }
            buffer[buffer_index].append(row)
            record_count += 1

    return buffer
```

### Function `create_record:`

## Parquet data to Timestream record mapping:


### Function `create_record:`

This function takes the record above and maps to Amazon Timestream attributes

```python
    def create_record(self, item):
        current_time = str(item['time'])
        source = item['source']
        value = item['value']
        signal = item['signal']

        record = self.prepare_record(current_time)

        record['Dimensions'].append(self.prepare_dimension('source', source))
        record['Dimensions'].append(self.prepare_dimension('signal', signal))
        # add more Dimensions from item as needed

        record['MeasureValues'].append(self.prepare_measure('value', value))
        # append more MeasureValues as measure columns as needed

        return record
```
Please append more dimensions or more measure values as needed

## Parameters to run:

The main function allows the following parameters:

| Parameter | Usage|
|-----------|------|
| threads   | Number of threads to run, should be at least 1 |
| region    | AWS Region of Timestream database |
| database | Database containing the target table |
| table | Target table |
| folder | folder that contains parquet files. A sample file is included in this example |

```json
    {
        'threads': 4,
        'region': 'us-east-1',
        'database': 'tools-sandbox',
        'table': 'ingestion-parquet',
        'folder': './'
    }
```