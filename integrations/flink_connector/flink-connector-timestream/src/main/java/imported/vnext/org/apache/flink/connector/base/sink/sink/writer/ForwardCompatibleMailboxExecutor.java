package imported.vnext.org.apache.flink.connector.base.sink.sink.writer;

import lombok.SneakyThrows;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Internal
public class ForwardCompatibleMailboxExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardCompatibleMailboxExecutor.class);

    private final ConcurrentLinkedQueue<AbstractMap.SimpleImmutableEntry<String, ThrowingRunnable<? extends Exception>>> mailQueue = new ConcurrentLinkedQueue();

    @SneakyThrows
    public void tryYield() {
        AbstractMap.SimpleImmutableEntry<String, ThrowingRunnable<? extends Exception>> poll = mailQueue.poll();
        while (poll != null) {
            LOG.debug(poll.getKey());
            poll.getValue().run();
            poll = mailQueue.poll();
        }
    }

    public void execute(final ThrowingRunnable<? extends Exception> command,
                        final String descriptionFormat,
                        final Object... descriptionArgs) {
        mailQueue.add(new AbstractMap.SimpleImmutableEntry(String.format(descriptionFormat, descriptionArgs), command));
    }
}
