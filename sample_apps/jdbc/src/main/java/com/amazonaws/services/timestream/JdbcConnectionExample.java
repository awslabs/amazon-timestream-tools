package com.amazonaws.services.timestream;

import software.amazon.timestream.jdbc.TimestreamDataSource;

import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.StringJoiner;

/**
 * This class showcases 7 different connection methods:
 * <ul>
 * <li>Connect using the default credential provider chain.
 * <li>Connect using the PropertiesFileCredentialsProvider as the authentication method.
 * <li>Connect using the InstanceProfileCredentialsProvider as the authentication method.
 * <li>Connect using the Okta credentials as the authentication method.
 * <li>Connect using the Azure AD credentials as the authentication method.
 * <li>Connect using specific connection properties.
 * <li>Connect using the URL.
 * <li>Connect with a specific endpoint.
 * <li>Connect using {@link TimestreamDataSource}.
 * <li>Connect using a pooled connection retrieved by {@link TimestreamDataSource}.
 * </ul><p>
 * Connection properties are stored in: db.properties: sample\src\main\resources\db.properties.
 * Connection properties for PropertiesFileCredentialsProvider are stored in {@code cred.properties:
 * sample\src\main\resources\cred.properties}.<p>The sample by default uses the first connection
 * method: using the default credential provider chain.<p> Feel free to use a different connection
 * method and modify connection properties in the db.properties files accordingly.
 */
public class JdbcConnectionExample {
  private static final ResourceBundle RESOURCE = ResourceBundle.getBundle("db");
  private static final ResourceBundle OKTA_RESOURCE = ResourceBundle.getBundle("OktaResources");
  private static final ResourceBundle AAD_RESOURCE = ResourceBundle.getBundle("AzureADResources");
  private static final String URL = "jdbc:timestream";
  private static final String URL_BRIDGE = "://";
  private static final String ACCESS_KEY_ID = RESOURCE.getString("AccessKeyId");
  private static final String SECRET_ACCESS_KEY = RESOURCE.getString("SecretAccessKey");
  private static final String SESSION_TOKEN = RESOURCE.getString("SessionToken");
  private static final String ENDPOINT = RESOURCE.getString("Endpoint");
  private static final String REGION = RESOURCE.getString("Region");
  private static final String OKTA_IDP_HOST = OKTA_RESOURCE.getString("IdpHost");
  private static final String OKTA_IDP_USERNAME = OKTA_RESOURCE.getString("IdpUserName");
  private static final String OKTA_IDP_PASSWORD = OKTA_RESOURCE.getString("IdpPassword");
  private static final String OKTA_AWS_APP_ID = OKTA_RESOURCE.getString("OktaApplicationID");
  private static final String OKTA_AWS_ROLE_ARN = OKTA_RESOURCE.getString("RoleARN");
  private static final String OKTA_IDP_ARN = OKTA_RESOURCE.getString("IdpARN");
  private static final String AAD_IDP_USERNAME = AAD_RESOURCE.getString("IdpUserName");
  private static final String AAD_IDP_PASSWORD = AAD_RESOURCE.getString("IdpPassword");
  private static final String AAD_ROLE_ARN = AAD_RESOURCE.getString("RoleARN");
  private static final String AAD_IDP_ARN = AAD_RESOURCE.getString("IdpARN");
  private static final String AAD_APPLICATION_ID = AAD_RESOURCE.getString("AADApplicationID");
  private static final String AAD_CLIENT_SECRET = AAD_RESOURCE.getString("AADClientSecret");
  private static final String AAD_TENANT_ID = AAD_RESOURCE.getString("AADTenant");

  /**
   * Creates a connection using the default connection URL and an {@link Properties} object
   * containing SDK configuration options. If no authentication options properties are provided in
   * the {@link Properties}, the driver will authenticate the connection using {@link
   * com.amazonaws.auth.AWSCredentialsProviderChain}, which looks for credentials in the following
   * order:
   * <ul>
   * <li>Environment variables – AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
   * <li>Java system properties — aws.accessKeyId and aws.secretKey.
   * <li>The credential profiles located at ~/.aws/credentials.
   * <li>Amazon ECS container credentials – AWS_CONTAINER_CREDENTIALS_RELATIVE_URI in the
   * environment variable.
   * </ul>
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithLocalCredentials() throws SQLException {
    final Properties prop = new Properties();
    prop.setProperty("RequestTimeout", "20000");
    prop.setProperty("SocketTimeout", "20000");
    prop.setProperty("MaxRetryCount", "10");
    prop.setProperty("MaxConnections", "3");
    prop.setProperty("Region", REGION);
    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection with the supported properties. The Timestream JDBC driver supports the
   * following optional database properties for basic authentication:
   * <ul>
   * <li>AccessKeyId — the AWS user access key id.
   * <li>SecretAccessKey — the AWS user secret access key.
   * <li>Region — the database's region; the default region is us-east-1.
   * <li>SessionToken - the temporary session token required to access a database with multi-factor
   * authentication (MFA) enabled.
   * </ul><p>
   * The driver also supports additional connection properties, examples for these options are in
   * {@link JdbcConnectionExample#createConnectionWithPropertiesFileCredentialsProvider} and {@link
   * JdbcConnectionExample#createConnectionWithInstanceProfileCredentialsProvider}:
   * <ul>
   * <li>AwsCredentialsProviderClass - the AWS credentials provider class to use.
   * <li>CustomCredentialsFilePath - the path to a custom credentials file for {@link com.amazonaws.auth.PropertiesFileCredentialsProvider}.
   * </ul>
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithProperties() throws SQLException {
    final Properties prop = new Properties();
    prop.put("AccessKeyId", ACCESS_KEY_ID);
    prop.put("SecretAccessKey", SECRET_ACCESS_KEY);
    prop.put("SessionToken", SESSION_TOKEN);
    prop.put("Region", REGION);
    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection using the {@link com.amazonaws.auth.PropertiesFileCredentialsProvider} as
   * the authentication method. When using the {@link com.amazonaws.auth.PropertiesFileCredentialsProvider},
   * the connection property {@code CustomCredentialsFilePath} also needs to be specified.
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithPropertiesFileCredentialsProvider()
    throws SQLException {
    final Properties prop = new Properties();
    prop.put("AwsCredentialsProviderClass", "PropertiesFileCredentialsProvider");
    prop.put("CustomCredentialsFilePath", "src/main/resources/cred.properties");
    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection using the {@link com.amazonaws.auth.InstanceProfileCredentialsProvider} as
   * the authentication method.
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithInstanceProfileCredentialsProvider()
    throws SQLException {
    final Properties prop = new Properties();
    prop.put("AwsCredentialsProviderClass", "InstanceProfileCredentialsProvider");
    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection using the Okta credentials to authenticate the user.
   *
   * <ul>
   *   <li>IdpName should be set to "Okta".</li>
   *   <li>IdpHost is the hostname of the Okta service that is used to authenticate.</li>
   *   <li>IdpUserName is the Okta email address.</li>
   *   <li>IdpPassword is the Okta password.</li>
   *   <li>OktaAppID is the unique ID of your AWS Application that is setup with Okta.</li>
   *   <li>RoleARN is the Amazon Resource Name (ARN) of the role that the user wants to assume.</li>
   *   <li>IdpARN is the Amazon Resource Name (ARN) of the Okta identity provider on AWS IAM page.</li>
   * </ul>
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred while creating a connection.
   */
  public static Connection createConnectionWithOktaIdpCredentials() throws SQLException {
    final Properties prop = new Properties();
    prop.put("IdpName", "Okta");
    prop.put("IdpHost", OKTA_IDP_HOST);
    prop.put("IdpUserName", OKTA_IDP_USERNAME);
    prop.put("IdpPassword", OKTA_IDP_PASSWORD);
    prop.put("OktaApplicationID", OKTA_AWS_APP_ID);
    prop.put("RoleARN", OKTA_AWS_ROLE_ARN);
    prop.put("IdpARN", OKTA_IDP_ARN);

    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection using the Azure AD credentials to authenticate the user.
   *
   * <ul>
   *   <li>IdpName should be set to "AzureAD".</li>
   *   <li>IdpUserName is the Azure AD user name.</li>
   *   <li>IdpPassword is the Azure AD password.</li>
   *   <li>RoleARN is the Amazon Resource Name (ARN) of the role that the user wants to assume.</li>
   *   <li>IdpARN is the Amazon Resource Name (ARN) of the Azure AD identity provider on AWS IAM page.</li>
   *   <li>AADApplicationID is the unique id of the registered application on Azure AD.</li>
   *   <li>AADClientSecret is the client secret associated with the registered application on Azure AD.</li>
   *   <li>AADTenant is the Azure AD Tenant ID.</li>
   * </ul>
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred while creating a connection.
   */
  public static Connection createConnectionWithAzureADIdpCredentials() throws SQLException {
    final Properties prop = new Properties();
    prop.put("IdpName", "AzureAD");
    prop.put("IdpUserName", AAD_IDP_USERNAME);
    prop.put("IdpPassword", AAD_IDP_PASSWORD);
    prop.put("RoleARN", AAD_ROLE_ARN);
    prop.put("IdpARN", AAD_IDP_ARN);
    prop.put("AADApplicationID", AAD_APPLICATION_ID);
    prop.put("AADClientSecret", AAD_CLIENT_SECRET);
    prop.put("AADTenant", AAD_TENANT_ID);

    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection using the URL. Each parameter is separated using a semicolon. An example
   * URL would be: jdbc:timestream://AccessKeyId=userName;SecretAccessKey=password;SessionToken=token;Region=region
   * <p>
   * All connection properties are optional. If no connection properties are specified in the URL:
   * {@code "jdbc:timestream"}, the default credentials chain will be used to authenticate the
   * connection.
   * <p>
   * If the same connection properties are specified in both the URL and the {@link Properties}, the
   * connection property in the URL will take precedence. For instance, in the following example
   * "foo" will be used during authentication:
   * <blockquote><pre>{@code
   * final String url = "jdbc:timestream://AccessKeyId=foo";
   * final Properties prop = new Properties();
   * prop.put("AccessKeyId", "userName");
   *
   * DriverManager.getConnection(url, prop)
   * }</pre></blockquote>
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithURL() throws SQLException {
    final StringJoiner joiner = new StringJoiner(";");
    joiner.add("AccessKeyId=" + ACCESS_KEY_ID);
    joiner.add("SecretAccessKey=" + SECRET_ACCESS_KEY);
    joiner.add("SessionToken=" + SESSION_TOKEN);
    joiner.add("Region=" + REGION);
    System.out.println(joiner);
    return DriverManager.getConnection(URL + URL_BRIDGE + joiner.toString(), new Properties());
  }

  /**
   * Create a connection to a specific service endpoint using the default credentials provider chain
   * to authenticate on the given signing region.
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithSpecificEndpoint() throws SQLException {
    final Properties prop = new Properties();
    prop.put("Endpoint", ENDPOINT);
    prop.put("Region", REGION);
    return DriverManager.getConnection(URL, prop);
  }

  /**
   * Creates a connection using {@link TimestreamDataSource}. All of the connection properties can
   * be configured using getters and setters.
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static Connection createConnectionWithDataSource() throws SQLException {
    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setLoginTimeout(1000);
    dataSource.setAccessKeyId(ACCESS_KEY_ID);
    dataSource.setSecretAccessKey(SECRET_ACCESS_KEY);
    dataSource.setSessionToken(SESSION_TOKEN);
    dataSource.setRegion(REGION);
    return dataSource.getConnection();
  }

  /**
   * Creates a pooled connection using {@link TimestreamDataSource}. All of the connection
   * properties can be configured using getters and setters.
   *
   * @return a {@link Connection}.
   * @throws SQLException if an error occurred when creating a connection.
   */
  public static PooledConnection createPooledConnectionWithDataSource() throws SQLException {
    final TimestreamDataSource dataSource = new TimestreamDataSource();
    dataSource.setLoginTimeout(1000);
    dataSource.setAccessKeyId(ACCESS_KEY_ID);
    dataSource.setSecretAccessKey(SECRET_ACCESS_KEY);
    dataSource.setSessionToken(SESSION_TOKEN);
    dataSource.setRegion(REGION);

    // Sets the SDK configuration options.
    dataSource.setMaxConnections(2);
    dataSource.setRequestTimeout(20000);
    dataSource.setSocketTimeout(20000);
    dataSource.setMaxRetryCount(10);
    return dataSource.getPooledConnection();
  }
}
