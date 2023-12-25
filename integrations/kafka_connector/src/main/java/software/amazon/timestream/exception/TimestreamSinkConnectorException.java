package software.amazon.timestream.exception;

/**
 * Base exception returned by the Timestream Sink connector
 */
public class TimestreamSinkConnectorException extends RuntimeException {

    private static final long serialVersionUID = -3872151051673375127L;

    /**
     * @param exception thrown from caller method
     */
    public TimestreamSinkConnectorException(final RuntimeException exception) {
        super(exception);
    }
    /**
     * @param error: Metadata of the error
     */

    public TimestreamSinkConnectorException(final TimestreamSinkConnectorError error, final Throwable exception) {
        super(error.getErrorCode() + ":" + error.getErrorMessage(), exception);
    }

    /**
     * @param code:    Error code for the issue found
     * @param message: Error message to be sent to the caller
     */
    public TimestreamSinkConnectorException(final String code, String message) {
        super(code + ":" + message);
    }

    /**
     * @param errorMessage: Error message to be sent to the caller
     */
    public TimestreamSinkConnectorException(String errorMessage) {
        super(errorMessage);
    }

    /**
     * @param error: Metadata of the error
     */

    public TimestreamSinkConnectorException(final TimestreamSinkConnectorError error) {
        super(error.getErrorCode() + ":" + error.getErrorMessage());
    }

}
