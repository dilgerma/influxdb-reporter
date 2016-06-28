package de.effectivetrainings.metrics.influx;

import lombok.*;

import java.util.List;

@Getter
@EqualsAndHashCode
@ToString
@Builder
@AllArgsConstructor
public class InfluxField {

    @NonNull
    private String name;
    @NonNull
    private Object value;
    @NonNull
    List<String> tags;

}
