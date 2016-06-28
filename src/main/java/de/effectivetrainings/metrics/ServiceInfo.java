package de.effectivetrainings.metrics;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ServiceInfo {

    private String serviceName;
    private String host;
    private String environment;

}
