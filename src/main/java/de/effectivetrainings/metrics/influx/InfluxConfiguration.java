package de.effectivetrainings.metrics.influx;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class InfluxConfiguration {

    @NonNull
    private String url;
    private String username;
    private String password;

}
