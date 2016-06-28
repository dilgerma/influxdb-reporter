package de.effectivetrainings.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Provides Meta Data that is rendered with every single metric.
 * Could be something like Service Name, Hostname etc. - everything that might
 * identify this service.
 */
public interface ServiceInfoProvider extends Supplier<Map<String, String>> {

    ServiceInfo serviceInfo();

    /**
     * provices a list of default tags for a system.
     */
    default Map<String, String> get() {
        Map<String, String> tags = new HashMap<>();
        tags.put("application", serviceInfo().getServiceName());
        tags.put("host", serviceInfo().getHost());
        tags.put("environment", serviceInfo().getEnvironment());
        return tags;
    };


}
