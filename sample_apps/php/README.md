# Getting started with Amazon Timestream with PHP

The example PHP application gives developers a basic understanding of how to begin using Amazon Timestream with PHP. The application exemplifies the beginning phases of database creation with some sample data population and how to execute queries against a database.

-----
## How to use
### Installation
#### PHP Installation
Navigate to [PHP official website](https://www.php.net/downloads.php) and install the stable version of PHP

#### Install the AWS SDK for PHP Version 3
1. Follow the [AWS official instruction](https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/getting-started_installation.html) for installation AWS SDK 
2. Update the code in main.php file:
`require '/path/to/vendor/autoload.php';` with the path that you will get from previous step

### Configuration updates
####
Specify aws clientId and secret in the main.php:

`const CLIENT_KEY = 'CLIENT_KEY_OVERRIDE';`

`const CLIENT_SECRET = 'CLIENT_SECRET_OVERRIDE';`

### Run sample
#### To execute sample php application run the following command:
```shell
php main.php
```

