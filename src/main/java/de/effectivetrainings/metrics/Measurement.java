package de.effectivetrainings.metrics;

import io.dropwizard.metrics.Gauge;
import io.dropwizard.metrics.MetricName;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Measurements are Points that are reported exactly once.
 * This is not officially supported by dropwizard metrics.
 */
@Getter
@RequiredArgsConstructor
public class Measurement implements Gauge {

    @NonNull
    private MetricName metricName;
    @NonNull
    private Map<String, Object> value;

    private Date time = new Date();


    public Measurement withTags(Map<String, String> tags) {
        this.metricName = metricName.tagged(tags);
        return this;
    }

    public Measurement withTime(Date time) {
        this.time = Objects.requireNonNull(time);
        return this;
    }


}
