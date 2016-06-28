package de.effectivetrainings.metrics.influx;

import com.google.common.collect.Lists;
import de.effectivetrainings.metrics.Measurement;
import de.effectivetrainings.metrics.MeasurementMetricRegistry;
import de.effectivetrainings.metrics.ServiceInfoProvider;
import io.dropwizard.metrics.MetricName;
import io.dropwizard.metrics.ScheduledReporter;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class InfluxReporter extends ScheduledReporter {

    private final InfluxDB influxDB;
    private final String database;
    private final Optional<ServiceInfoProvider> serviceMetricsMetaDataProvider;
    private boolean initialized;
    private MeasurementMetricRegistry registry;

    public InfluxReporter(MeasurementMetricRegistry registry,
                          String name,
                          io.dropwizard.metrics.MetricFilter filter,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          InfluxDB influxDB,
                          String database,
                          Optional<ServiceInfoProvider> serviceMetricsMetaDataProvider) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.influxDB = influxDB;
        this.database = database;
        this.serviceMetricsMetaDataProvider = serviceMetricsMetaDataProvider;
        this.registry = registry;
    }

    public InfluxReporter(MeasurementMetricRegistry registry, String name, InfluxDB influxDB, String database,
                          ServiceInfoProvider serviceInfoProvider
    ) {
        this(registry, name, io.dropwizard.metrics.MetricFilter.ALL, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, influxDB, database,
                Optional.of(serviceInfoProvider));
    }

    public InfluxReporter(MeasurementMetricRegistry registry, String name, InfluxDB influxDB, String database) {
        this(registry, name, io.dropwizard.metrics.MetricFilter.ALL, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, influxDB, database,
                Optional.empty());
    }


    @Override
    public void report(SortedMap<MetricName, io.dropwizard.metrics.Gauge> gauges,
                       SortedMap<MetricName, io.dropwizard.metrics.Counter> counters,
                       SortedMap<MetricName, io.dropwizard.metrics.Histogram> histograms,
                       SortedMap<MetricName, io.dropwizard.metrics.Meter> meters,
                       SortedMap<MetricName, io.dropwizard.metrics.Timer> timers
    ) {
        log.info("reporting to influx with InfluxDB : {}", influxDB);
        initializeDatabase();
        List<Point> counterPoints = reportCounters(counters);
        List<Point> gaugePoints = reportGauges(gauges);
        List<Point> meterPoints = reportMeters(meters);
        List<Point> timerPoints = reportTimers(timers);
        List<Point> histogramPoints = reportHistograms(histograms);
        Map<MetricName, Measurement> measurements = registry.getMeasurements();
        List<Point> measurementPoints = reportMeasurements(registry.getMeasurements());

        List<Point> points = Stream.of(counterPoints, gaugePoints, meterPoints, timerPoints, histogramPoints, measurementPoints)
                .flatMap
                     (List::stream)
                     .collect(Collectors.toList());

        final BatchPoints.Builder batchPointsBuilder = BatchPoints.database(database)
                .points(points.toArray(new Point[0]))
                .consistency(InfluxDB.ConsistencyLevel.ALL);

        serviceMetricsMetaDataProvider.ifPresent(provider -> provider.get().entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .forEach(entry ->
                        batchPointsBuilder.tag(entry.getKey(), entry.getValue()))
        );

        log.info("sending {} points", points.size());
        try {
            influxDB.write(batchPointsBuilder.build());
        } catch (Exception e) {
            //by intention we do not render the stacktrace, if there is a problem, the log files are polluted with
            // stacktraces as this runs often...
            //later we might enable that again, but for now, we need to first get it stable.
            log.warn("Cannot report to influx.", e.getMessage());
        }

        //clear measurements, as we want to report them only once.
        registry.clearMeasurements(measurements.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
    }


    private void initializeDatabase() {
        if (!initialized && !influxDB.describeDatabases().contains(database)) {
            influxDB.createDatabase(database);
            initialized = true;
        }
    }

    private List<Point> reportHistograms(SortedMap<MetricName, io.dropwizard.metrics.Histogram> histograms) {
        List<Point> histogramMetrics = new ArrayList<>();
        histograms.entrySet().stream().forEach(histogram -> {
            Set<InfluxField> fields = new HashSet<>();
            fields.add(field("count",
                    histogram.getValue().getCount()));
            fields.add(field("p75",
                    histogram.getValue().getSnapshot().get75thPercentile()));
            fields.add(field("p95",
                    histogram.getValue().getSnapshot().get95thPercentile()));
            fields.add(field("p98",
                    histogram.getValue().getSnapshot().get98thPercentile()));
            fields.add(field("p999",
                    histogram.getValue().getSnapshot().get999thPercentile()));
            fields.add(field("p99",
                    histogram.getValue().getSnapshot().get99thPercentile()));
            fields.add(field("max",
                    histogram.getValue().getSnapshot().getMax()));
            fields.add(field("min",
                    histogram.getValue().getSnapshot().getMin()));
            fields.add(field("mean",
                    histogram.getValue().getSnapshot().getMean()));
            fields.add(field("median",
                    histogram.getValue().getSnapshot().getMedian()));
            histogramMetrics.add(point(histogram.getKey(), fields));
        });
        return histogramMetrics;

    }

    private List<Point> reportTimers(SortedMap<MetricName, io.dropwizard.metrics.Timer> timers) {
        List<Point> timerMetrics = new ArrayList<>();
        timers.entrySet().stream().forEach(timer -> {
            Set<InfluxField> fields = new HashSet<>();
            fields.add(field("count",
                    convertDuration(timer.getValue().getCount())));
            fields.add(field("m15_rate",
                    convertDuration(timer.getValue().getFifteenMinuteRate())));
            fields.add(field("m5_rate",
                    convertDuration(timer.getValue().getFiveMinuteRate())));
            fields.add(field("mean_rate",
                    convertDuration(timer.getValue().getMeanRate())));
            fields.add(field("m1_rate",
                    convertDuration(timer.getValue().getOneMinuteRate())));
            fields.add(field("p75",
                    convertDuration(timer.getValue().getSnapshot().get75thPercentile())));
            fields.add(field("p95",
                    convertDuration(timer.getValue().getSnapshot().get95thPercentile())));
            fields.add(field("p98",
                    convertDuration(timer.getValue().getSnapshot().get98thPercentile())));
            fields.add(field("p999",
                    convertDuration(timer.getValue().getSnapshot().get999thPercentile())));
            fields.add(field("p99",
                    convertDuration(timer.getValue().getSnapshot().get99thPercentile())));
            fields.add(field("max",
                    convertDuration(timer.getValue().getSnapshot().getMax())));
            fields.add(field("min",
                    convertDuration(timer.getValue().getSnapshot().getMin())));
            fields.add(field(
                    "mean",
                    convertDuration(timer.getValue().getSnapshot().getMean())));
            fields.add(field("median",
                    convertDuration(timer.getValue().getSnapshot().getMedian())));
            timerMetrics.add(point(timer.getKey(), fields));
        });
        return timerMetrics;
    }

    private List<Point> reportGauges(SortedMap<MetricName, io.dropwizard.metrics.Gauge> gauges) {
        return gauges
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getValue() != null)
                .map(entry ->
                        point(entry.getKey(),
                                Collections.singleton(field("gauge",
                                        sanitizeGauge(entry.getValue().getValue())))))
                .collect(Collectors.toList());
    }

    /**
     * InfluxDB does not like "NaN" for number fields, use null instead
     *
     * @param value the value to sanitize
     * @return value, or null if value is a number and is finite
     */
    private Object sanitizeGauge(Object value) {
        final Object finalValue;
        if (value instanceof Double && (Double.isInfinite((Double) value) || Double.isNaN((Double) value))) {
            finalValue = null;
        } else if (value instanceof Float && (Float.isInfinite((Float) value) || Float.isNaN((Float) value))) {
            finalValue = null;
        } else if (value instanceof Collection) {
            finalValue = ((Collection) value).size();
        } else {
            finalValue = value;
        }
        return finalValue;
    }

    private List<Point> reportCounters(SortedMap<MetricName, io.dropwizard.metrics.Counter> counters) {
        return counters
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getCount() != 0)
                .map(entry -> point(entry.getKey(),
                        Collections.singleton(field("counter", entry
                                .getValue()
                                .getCount
                                        ()))))
                .collect(Collectors.toList());
    }

    private List<Point> reportMeters(SortedMap<MetricName, io.dropwizard.metrics.Meter> meters) {
        List<Point> meterMetrics = new ArrayList<>();
        meters.entrySet().stream().forEach(meter -> {
            Set<InfluxField> fields = new HashSet<>();
            fields.add(field("count",
                    meter.getValue().getCount()));
            fields.add(field("m15",
                    meter.getValue().getFifteenMinuteRate()));
            fields.add(field("m5",
                    meter.getValue().getFiveMinuteRate()));
            fields.add(field("mean",
                    meter.getValue().getMeanRate()));
            fields.add(field("m1",
                    meter.getValue().getOneMinuteRate()));
            meterMetrics.add(point(meter.getKey(), fields));
        });
        return meterMetrics;
    }

    private List<Point> reportMeasurements(SortedMap<MetricName, Measurement> measurements) {
        List<Point> measurementMetrics = new ArrayList<>();
        measurements.entrySet().stream().forEach(measurement -> {
            final Set<InfluxField> fields = measurement.getValue().getValue().entrySet().stream().map(
                    entry -> new InfluxField(entry.getKey(), entry.getValue(), new ArrayList<>())).collect(Collectors
                    .toSet());
            measurementMetrics.add(point(measurement.getKey(), fields));

        });
        return measurementMetrics;
    }

    private InfluxField field(String key, Object value) {
        return new InfluxField(key, value, Lists.newArrayList());
    }

    protected String sanitizeName(String name) {
        return name;
    }

    private Point point(MetricName metricName, Set<InfluxField> fields) {
        final Point.Builder pointBuilder =
                Point.measurement(sanitizeName(metricName.getKey())).tag(metricName.getTags());
        fields.stream().forEach(field -> pointBuilder.field(field.getName(), field.getValue()));
        return pointBuilder.build();
    }


}
