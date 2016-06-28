package de.effectivetrainings.metrics;

import io.dropwizard.metrics.MetricName;
import io.dropwizard.metrics.MetricRegistry;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MeasurementMetricRegistry extends MetricRegistry {

    public SortedMap<MetricName, Measurement> getMeasurements() {
        final Map<MetricName, Measurement> measurements = getMetrics()
                .entrySet()
                .stream()
                .filter(Measurement.class::isInstance)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (Measurement)entry.getValue()));
        return new TreeMap<>(measurements);
    }

    public void clearMeasurements(List<MetricName> measurements) {
        this.removeMatching((metricName, metric) -> measurements.contains(metricName));
    }
}
