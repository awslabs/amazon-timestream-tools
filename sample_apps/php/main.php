<?php
    require '/path/to/vendor/autoload.php';

    const REGION = 'us-east-1';

    const CLIENT_KEY = 'CLIENT_KEY_OVERRIDE';
    const CLIENT_SECRET = 'CLIENT_SECRET_OVERRIDE';

    const DB_NAME = 'TEST_PHP';
    const DB_TABLE_NAME = 'TEST_TABLE_PHP';
    const MAGNETIC_STORE_RETENTION_PERIOD_IN_DAYS = 1;
    const MEMORY_STORE_RETENTION_PERIOD_IN_HOURS = 1;

    /*
     * This function create Database for Timestream DB
     * as parameters it uses aws client and database Name.
     */
    function createDatabase($client, $dbName) {
        return $client->createDatabase([
                   'DatabaseName' => $dbName
               ]);
    }

    /*
     * This function delete Database for Timestream DB
     * as parameters it uses aws client and database Name.
     */
    function deleteDatabase($client, $dbName) {
        return $client->deleteDatabase([
                   'DatabaseName' => $dbName,
               ]);
    }

    /*
     * This function create Table for Timestream DB
     * as parameters it uses aws client and database Name and table Name.
     */
    function createTable($client, $dbName, $dbTableName) {
        return $client->createTable([
                    'DatabaseName' => $dbName,
                    'RetentionProperties' => [
                       'MagneticStoreRetentionPeriodInDays' => MAGNETIC_STORE_RETENTION_PERIOD_IN_DAYS,
                       'MemoryStoreRetentionPeriodInHours' => MEMORY_STORE_RETENTION_PERIOD_IN_HOURS,
                    ],
                    'TableName' => $dbTableName
                ]);
    }

    /*
     * This function delete Table on Timestream DB
     * as parameters it uses aws client and database Name and table Name.
     */
    function deleteTable($client, $dbName, $dbTableName) {
        return $client->deleteTable([
                   'DatabaseName' => $dbName,
                   'TableName' => $dbTableName,
               ]);
    }

    /*
     * This function write records to Timestream DB Table
     * as parameters it uses aws client and database Name and table Name,
     * records that has Dimensions and Time and Measure information.
     */
    function writeRecords($client, $dbName, $dbTableName, $records) {
        $response = $client->writeRecords([
                   'DatabaseName' => $dbName,
                   'Records' => $records,
                   'TableName' => $dbTableName,
                ]);
        if (isset($response['RejectedRecords']) && !empty($response['RejectedRecords'])) {
            echo "Some records were rejected:\n";
            foreach ($response['RejectedRecords'] as $rejectedRecord) {
                echo "Reason: " . $rejectedRecord['Reason'] . "\n";
                echo "ExistingRecord: " . json_encode($rejectedRecord['ExistingRecord']) . "\n";
            }
        } else {
            echo "Data inserted successfully!\n";
        }

        return $response;
    }

    /*
     * This function generate sample records
     */
    function generateRecords() {
        $records = [];
        $arr = range(0, 5);
        foreach ($arr as &$value) {
            $record = [
                         'Dimensions' => [
                             [
                                 'DimensionValueType' => 'VARCHAR',
                                 'Name' => 'Computer',
                                 'Value' => 'Number'.$value,
                             ],
                          ],
                          'MeasureName' => 'CPU',
                          'MeasureValue' => strVal(mt_rand() / mt_getrandmax()),
                          'MeasureValueType' => 'DOUBLE',
                          'Time' => strVal(time()),
                          'TimeUnit' => 'SECONDS',
                      ];
            array_push($records, $record);
        }

        return $records;
    }

    /*
     * This function returns AWS Timestream write client
     * To get all list of available methods for client please look official documentation.
     */
    function createClient() {
        return new \Aws\TimestreamWrite\TimestreamWriteClient([
                   'version' => 'latest',
                   'region' => REGION,
                   'credentials' => new \Aws\Credentials\Credentials(CLIENT_KEY, CLIENT_SECRET)
               ]);
    }

    /*
     * This function returns AWS Timestream read client
     * To get all list of available methods for client please look official documentation.
     */
    function createReader() {
        return new \Aws\TimestreamQuery\TimestreamQueryClient([
                   'version' => 'latest',
                   'region' => REGION,
                   'credentials' => new \Aws\Credentials\Credentials(CLIENT_KEY, CLIENT_SECRET)
               ]);
    }

    function executeQuery($reader, $query) {
        return $reader->query([
                   'MaxRows' => 10,
                   'QueryString' => $query,
               ]);
    }

    $client = createClient();
    $reader = createReader();
    createDatabase($client, DB_NAME);
    createTable($client, DB_NAME, DB_TABLE_NAME);
    $records = generateRecords();
    writeRecords($client, DB_NAME, DB_TABLE_NAME, $records);
    $results = executeQuery($reader, 'SELECT * FROM '.DB_NAME.'.'.DB_TABLE_NAME);
    echo $results;
    deleteTable($client, DB_NAME, DB_TABLE_NAME);
    deleteDatabase($client, DB_NAME);
?>
