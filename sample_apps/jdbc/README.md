# TimestreamCustomerSampleJDBC

Sample JDBC application

----
## How to use it

Ensure your Java SDK and runtime are 1.8 or higher.

1. Install maven: https://maven.apache.org/install.html

2. You can compile and run your source code with the below command:
    ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.timestream.Main" -Dexec.args="-d <database with DevOps table> -h <host name from hostname dimension>"
    ``` 
   NOTE: You might need to change the version of timestreamwrite and timestreamquery dependencies in `pom.xml` file based on the version of SDK jar you are using.
