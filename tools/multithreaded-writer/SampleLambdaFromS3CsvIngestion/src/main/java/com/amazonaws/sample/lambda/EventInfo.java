package com.amazonaws.sample.lambda;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@ToString
@RequiredArgsConstructor
@Getter
public class EventInfo {
    private static final Logger LOG = LoggerFactory.getLogger(EventInfo.class);

    private final String bucketName;
    private final String bucketKey;
    private final String localPath;

    public static EventInfo getInputFile(final Map<String, Object> event) {
        if (event.containsKey("local-test-path")) {
            return new EventInfo(null, null, (String) event.get("local-test-path"));
        }

        var bucketName = (String) event.get("bucket.name");
        var bucketKey = (String) event.get("bucket.key");
        Preconditions.checkArgument(!(bucketName == null || bucketName.isEmpty()),
                "Bucket Name - 'bucket.name' must be present and not-empty. Received: %s", bucketName);
        Preconditions.checkArgument(!(bucketKey == null || bucketKey.isEmpty()),
                "Bucket Key - 'bucket.key' must be present and not-empty. Received: %s", bucketKey);
        return new EventInfo(bucketName, bucketKey, null);
    }
}
