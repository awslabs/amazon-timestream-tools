package software.amazon.timestream.exception;

import java.util.List;

/**
 * Exception thrown when there is a validation error.
 * Sample validation errors:
 * 1. Given Timestream ingestion endpoint is not a valid URI
 * 2. S3 bucket name is not supplied
 */
public class TimestreamSinkConnectorValidationException extends TimestreamSinkConnectorException {

    private static final long serialVersionUID = -1892092587330382099L;

     /**
     * @param validationErrors: List of errors found during validation
     * @see TimestreamSinkConnectorError
     */
    public TimestreamSinkConnectorValidationException(final List<TimestreamSinkConnectorError> validationErrors) {
        super(TimestreamSinkErrorCodes.VALIDATION_ERROR, convertToMessage(validationErrors));
    }

    /**
     * Method to get a consolidated message for the given list of validation errors.
     *
     * @param validationErrors: List of validation errors found;
     * @return a consolidated error message for the given list of validation errors.
     * @see TimestreamSinkConnectorError
     */
    private static String convertToMessage(final List<TimestreamSinkConnectorError> validationErrors) {
        final StringBuilder builder = new StringBuilder();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            for (final TimestreamSinkConnectorError v : validationErrors) {
                builder.append(v.getErrorCode()).append(':').append(v.getErrorMessage()).append('\n');
            }
        }
        return builder.toString();
    }

}
