package software.amazon.timestream.exception;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Timestream connector exception is thrown along with its error code and message
 * This class is meant to capture the error metadata.
 */
@Getter
@EqualsAndHashCode
public class TimestreamSinkConnectorError {

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamSinkConnectorError.class);
    /**
     * Validation error message bundle
     */
    private static final ResourceBundle BUNDLE = ResourceBundle.getBundle("validation_messages");

    /**
     * Error code
     */
    private final String errorCode;
    /**
     * Formatted error message
     */
    private String errorMessage;

    /**
     * @param errorCode: Error Code as listed in {@link TimestreamSinkErrorCodes}
     * @param arguments: Arguments passed to format the validation message
     */
    public TimestreamSinkConnectorError(final String errorCode, final Object... arguments) {
        this.errorCode = errorCode;
        try {
            this.errorMessage = new MessageFormat(BUNDLE.getString(errorCode)).format(arguments);
        } catch (IllegalArgumentException ignored) {
            LOGGER.error("ERROR: Unable to locate the messages for the error code: {} and args: {} in the validation_messages.properties", errorCode, arguments);
        }
    }

}