package com.fitbit.core.finagle.tracing;

import com.google.common.base.Preconditions;
import com.twitter.finagle.stats.DefaultStatsReceiver$;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.util.AbstractClosable;
import com.twitter.util.Closables;
import com.twitter.util.Future;
import com.twitter.util.Time;
import scala.runtime.BoxedUnit;
import zipkin.finagle.ZipkinTracer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.IOException;

/**
 * Kafka implementation of the ZipkinTracer that leverages the newer Kafka Producer API rather than v0.8 used in the
 * community-provided zipkin-finagle-kafka implementation. This may be substituted out for the community implementation
 * when they upgrade to the new Kafka APIs.
 *
 * @author dgarson
 */
public class KafkaZipkinTracer extends ZipkinTracer {

    private static final String STATS_MODULE_SCOPE = "zipkin.kafka";

    private final Config config;
    private final KafkaZipkinSender kafkaSender;
    private final StatsReceiver statsReceiver;

    public KafkaZipkinTracer() {
        this(Config.builder().build());
    }

    public KafkaZipkinTracer(Config config) {
        this(config, DefaultStatsReceiver$.MODULE$.get().scope(STATS_MODULE_SCOPE));
    }

    KafkaZipkinTracer(Config config, StatsReceiver statsReceiver) {
        this(KafkaZipkinSender.builder()
                .bootstrapServers(config.getBootstrapServers())
                .topic(config.getTopic())
                .build(),
            config, statsReceiver);
    }

    KafkaZipkinTracer(KafkaZipkinSender kafkaSender, Config config, StatsReceiver statsReceiver) {
        super(Preconditions.checkNotNull(kafkaSender, "kafkaSender"),
            Preconditions.checkNotNull(config, "config"),
            Preconditions.checkNotNull(statsReceiver, "statsReceiver"));
        this.kafkaSender = kafkaSender;
        this.config = config;
        this.statsReceiver = statsReceiver;
    }

    @Override
    public Future<BoxedUnit> close(Time deadline) {
        return Closables.sequence(
            // first close our KafkaSender
            new AbstractClosable() {
                @Override
                public Future<BoxedUnit> close(Time deadline) {
                    try {
                        // TODO: blocking here
                        kafkaSender.close();
                        return Future.Done();
                    } catch (IOException ioe) {
                        return Future.exception(ioe);
                    }
                }
            },
            // then allow the superclass to handle its own cleanup
            new AbstractClosable() {
                @Override
                public Future<BoxedUnit> close(Time deadline) {
                    return KafkaZipkinTracer.super.close(deadline);
                }
            }).close(deadline);
    }

    /**
     * Create a new instance with default configuration.
     *
     * @param bootstrapServers a list of host/port pairs to use for establishing the initial
     *                         connection to the Kafka cluster. Like: host1:port1,host2:port2,... Does not to be all the
     *                         servers part of Kafka cluster.
     * @param statsReceiver gets notified when spans are accepted or dropped. If you are not interested in
     *                      these events you can use {@linkplain com.twitter.finagle.stats.NullStatsReceiver}
     */
    public static KafkaZipkinTracer create(@Nonnull String bootstrapServers, StatsReceiver statsReceiver) {
        return new KafkaZipkinTracer(Config.builder()
            .bootstrapServers(bootstrapServers).build(), statsReceiver);
    }

    /**
     * @param config includes flush interval and kafka properties
     * @param statsReceiver gets notified when spans are accepted or dropped. If you are not interested in
     *                      these events you can use {@linkplain com.twitter.finagle.stats.NullStatsReceiver}
     */
    public static KafkaZipkinTracer create(Config config, StatsReceiver statsReceiver) {
        return new KafkaZipkinTracer(config, statsReceiver);
    }

    @Immutable
    public static class Config implements ZipkinTracer.Config {

        private final String bootstrapServers;
        private final String topic;
        private final float initialSampleRate;

        Config(String bootstrapServers, String topic, float initialSampleRate) {
            this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrapServers");
            this.topic = Preconditions.checkNotNull(topic, "topic");
            Preconditions.checkArgument(initialSampleRate >= 0f && initialSampleRate <= 1f,
                "initialSampleRate must be between 0 and 1, inclusive, but got: " + initialSampleRate);
            this.initialSampleRate = initialSampleRate;
        }

        @Nonnull
        public String getBootstrapServers() {
            return bootstrapServers;
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }

        @Nonnegative
        @Override
        public float initialSampleRate() {
            return initialSampleRate;
        }

        public Builder toBuilder() {
            return builder()
                .bootstrapServers(bootstrapServers)
                .topic(topic)
                .initialSampleRate(initialSampleRate);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private String bootstrapServers;
            private String topic;
            private float initialSampleRate;

            Builder() {
                // package-private instantiation only
            }

            /**
             * Initial set of kafka servers to connect to, rest of cluster will be discovered (comma
             * separated). Default localhost:9092
             */
            public Builder bootstrapServers(@Nonnull String bootstrapServers) {
                this.bootstrapServers = bootstrapServers;
                return this;
            }

            /**
             * Specifies the Kafka topic for Zipkin to report to. Default topic name is <tt>zipkin</tt>
             */
            public Builder topic(String topic) {
                this.topic = topic;
                return this;
            }

            /**
             * @see ZipkinTracer.Config#initialSampleRate()
             */
            public Builder initialSampleRate(float initialSampleRate) {
                this.initialSampleRate = initialSampleRate;
                return this;
            }

            /**
             * Builds the configuration object from the properties in this builder.
             */
            @Nonnull
            public Config build() {
                return new Config(bootstrapServers, topic, initialSampleRate);
            }
        }
    }
}
