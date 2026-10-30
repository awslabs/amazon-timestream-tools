/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.function.ThrowingRunnable;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class WriterTestContext {
    private WriterTestContext() {}

    public static WriterInitContext create() {
        return capture().context();
    }

    public static CapturedContext capture() {
        return capture(false);
    }

    public static CapturedContext captureWithQueuedMailbox() {
        return capture(true);
    }

    private static CapturedContext capture(boolean queuedMailbox) {
        Map<String, Counter> counters = new ConcurrentHashMap<>();
        Map<String, Gauge<?>> gauges = new ConcurrentHashMap<>();
        Map<String, Histogram> histograms = new ConcurrentHashMap<>();
        Map<String, Meter> meters = new ConcurrentHashMap<>();
        BlockingQueue<ThrowingRunnable<?>> mailboxActions = new LinkedBlockingQueue<>();
        OperatorIOMetricGroup ioMetricGroup =
                proxy(
                        OperatorIOMetricGroup.class,
                        (instance, method, arguments) -> {
                            if (method.getName().startsWith("getNum")
                                    && Counter.class.isAssignableFrom(method.getReturnType())) {
                                return counters.computeIfAbsent(
                                        standardCounterName(method.getName()),
                                        ignored -> new SimpleCounter());
                            }
                            return metricMethod(
                                    "",
                                    method.getName(),
                                    arguments,
                                    counters,
                                    gauges,
                                    histograms,
                                    meters);
                        });
        SinkWriterMetricGroup metricGroup =
                proxy(
                        SinkWriterMetricGroup.class,
                        (instance, method, arguments) -> {
                            if ("getIOMetricGroup".equals(method.getName())) {
                                return ioMetricGroup;
                            }
                            if (method.getName().startsWith("getNum")
                                    && Counter.class.isAssignableFrom(method.getReturnType())) {
                                return counters.computeIfAbsent(
                                        standardCounterName(method.getName()),
                                        ignored -> new SimpleCounter());
                            }
                            return metricMethod(
                                    "",
                                    method.getName(),
                                    arguments,
                                    counters,
                                    gauges,
                                    histograms,
                                    meters);
                        });
        MailboxExecutor mailbox =
                proxy(
                        MailboxExecutor.class,
                        (instance, method, arguments) -> {
                            if ("execute".equals(method.getName())) {
                                for (Object argument : arguments) {
                                    if (argument instanceof ThrowingRunnable) {
                                        ThrowingRunnable<?> action =
                                                (ThrowingRunnable<?>) argument;
                                        if (queuedMailbox) {
                                            mailboxActions.add(action);
                                        } else {
                                            try {
                                                action.run();
                                            } catch (Exception ignored) {
                                                // Fatal mailbox actions fail a real Flink task.
                                                // This lightweight context intentionally does not
                                                // emulate task failure.
                                            }
                                        }
                                        break;
                                    }
                                }
                                return null;
                            }
                            if ("yield".equals(method.getName()) && queuedMailbox) {
                                ThrowingRunnable<?> action =
                                        mailboxActions.poll(5, TimeUnit.SECONDS);
                                if (action == null) {
                                    throw new IllegalStateException(
                                            "Timed out waiting for a mailbox action");
                                }
                                action.run();
                                return null;
                            }
                            if ("tryYield".equals(method.getName()) && queuedMailbox) {
                                ThrowingRunnable<?> action = mailboxActions.poll();
                                if (action == null) {
                                    return false;
                                }
                                action.run();
                                return true;
                            }
                            return defaultValue(method.getReturnType());
                        });
        ScheduledFuture<?> timer =
                proxy(
                        ScheduledFuture.class,
                        (instance, method, arguments) -> {
                            switch (method.getName()) {
                                case "cancel":
                                case "isCancelled":
                                    return true;
                                case "isDone":
                                    return false;
                                case "compareTo":
                                    return 0;
                                case "getDelay":
                                    return 0L;
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });
        ProcessingTimeService timeService =
                proxy(
                        ProcessingTimeService.class,
                        (instance, method, arguments) -> {
                            if ("registerTimer".equals(method.getName())) {
                                return timer;
                            }
                            return defaultValue(method.getReturnType());
                        });
        WriterInitContext context =
                proxy(
                        WriterInitContext.class,
                        (instance, method, arguments) -> {
                            switch (method.getName()) {
                                case "getMailboxExecutor":
                                    return mailbox;
                                case "getProcessingTimeService":
                                    return timeService;
                                case "metricGroup":
                                    return metricGroup;
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });
        return new CapturedContext(context, counters, gauges, histograms, meters);
    }

    /**
     * Records metrics under their group path. Root metrics use their plain name; metrics in
     * {@code addGroup("a").addGroup("k", "v")} are recorded as {@code a.k=v.name}.
     */
    private static Object metricMethod(
            String prefix,
            String methodName,
            Object[] arguments,
            Map<String, Counter> counters,
            Map<String, Gauge<?>> gauges,
            Map<String, Histogram> histograms,
            Map<String, Meter> meters) {
        if ("addGroup".equals(methodName)) {
            String childPrefix =
                    arguments.length == 1
                            ? prefix + arguments[0] + "."
                            : prefix + arguments[0] + "=" + arguments[1] + ".";
            return proxy(
                    MetricGroup.class,
                    (instance, method, childArguments) ->
                            metricMethod(
                                    childPrefix,
                                    method.getName(),
                                    childArguments,
                                    counters,
                                    gauges,
                                    histograms,
                                    meters));
        }
        if ("counter".equals(methodName)) {
            String name = prefix + arguments[0];
            if (arguments.length == 2) {
                Counter counter = (Counter) arguments[1];
                counters.put(name, counter);
                return counter;
            }
            return counters.computeIfAbsent(name, ignored -> new SimpleCounter());
        }
        if ("gauge".equals(methodName)) {
            Gauge<?> gauge = (Gauge<?>) arguments[1];
            gauges.put(prefix + arguments[0], gauge);
            return gauge;
        }
        if ("histogram".equals(methodName)) {
            Histogram histogram = (Histogram) arguments[1];
            histograms.put(prefix + arguments[0], histogram);
            return histogram;
        }
        if ("meter".equals(methodName)) {
            Meter meter = (Meter) arguments[1];
            meters.put(prefix + arguments[0], meter);
            return meter;
        }
        return null;
    }

    private static String standardCounterName(String accessorName) {
        String counterName =
                accessorName.substring("get".length(), accessorName.length() - "Counter".length());
        return Character.toLowerCase(counterName.charAt(0)) + counterName.substring(1);
    }

    public static final class CapturedContext {
        private final WriterInitContext context;
        private final Map<String, Counter> counters;
        private final Map<String, Gauge<?>> gauges;
        private final Map<String, Histogram> histograms;
        private final Map<String, Meter> meters;

        private CapturedContext(
                WriterInitContext context,
                Map<String, Counter> counters,
                Map<String, Gauge<?>> gauges,
                Map<String, Histogram> histograms,
                Map<String, Meter> meters) {
            this.context = context;
            this.counters = counters;
            this.gauges = gauges;
            this.histograms = histograms;
            this.meters = meters;
        }

        public WriterInitContext context() {
            return context;
        }

        public Counter counter(String name) {
            return Objects.requireNonNull(counters.get(name), "Unknown counter: " + name);
        }

        public long gaugeValue(String name) {
            Gauge<?> gauge =
                    Objects.requireNonNull(gauges.get(name), "Unknown gauge: " + name);
            Object value = gauge.getValue();
            if (!(value instanceof Number)) {
                throw new IllegalStateException("Gauge is not numeric: " + name);
            }
            return ((Number) value).longValue();
        }

        public Histogram histogram(String name) {
            return Objects.requireNonNull(
                    histograms.get(name), "Unknown histogram: " + name);
        }

        public Meter meter(String name) {
            return Objects.requireNonNull(meters.get(name), "Unknown meter: " + name);
        }

        public double gaugeDouble(String name) {
            Gauge<?> gauge =
                    Objects.requireNonNull(gauges.get(name), "Unknown gauge: " + name);
            return ((Number) gauge.getValue()).doubleValue();
        }

        /** Returns the names of all registered metrics, including their group path. */
        public Set<String> metricNames() {
            Set<String> names = new TreeSet<>();
            names.addAll(counters.keySet());
            names.addAll(gauges.keySet());
            names.addAll(histograms.keySet());
            names.addAll(meters.keySet());
            return names;
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (type == void.class) {
            return null;
        }
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0.0F;
        }
        if (type == double.class) {
            return 0.0D;
        }
        throw new IllegalArgumentException("Unsupported primitive type: " + type);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T)
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (instance, method, arguments) -> {
                            if (method.getDeclaringClass() == Object.class) {
                                switch (method.getName()) {
                                    case "toString":
                                        return type.getSimpleName() + "Proxy";
                                    case "hashCode":
                                        return System.identityHashCode(instance);
                                    case "equals":
                                        return instance == arguments[0];
                                    default:
                                        return null;
                                }
                            }
                            return handler.invoke(instance, method, arguments);
                        });
    }
}
