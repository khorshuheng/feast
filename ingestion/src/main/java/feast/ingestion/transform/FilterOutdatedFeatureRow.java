/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto;
import feast.types.FeatureRowProto;
import feast.types.FieldProto;
import feast.types.ValueProto;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class FilterOutdatedFeatureRow
    extends PTransform<
        PCollection<FeatureRowProto.FeatureRow>, PCollection<FeatureRowProto.FeatureRow>> {

  public static FilterOutdatedFeatureRow.Builder newBuilder() {
    return new AutoValue_FilterOutdatedFeatureRow.Builder();
  }

  public abstract Map<String, FeatureSetProto.FeatureSet> getFeatureSets();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract FilterOutdatedFeatureRow.Builder setFeatureSets(
        Map<String, FeatureSetProto.FeatureSet> featureSets);

    public abstract FilterOutdatedFeatureRow build();
  }

  private FeatureRowProto.FeatureRowKey getKey(FeatureRowProto.FeatureRow featureRow) {
    List<String> entityNames =
        getFeatureSets().get(featureRow.getFeatureSet()).getSpec().getEntitiesList().stream()
            .map(FeatureSetProto.EntitySpec::getName)
            .collect(Collectors.toList());
    List<ValueProto.Value> entityValues =
        featureRow.getFieldsList().stream()
            .filter(field -> entityNames.contains(field.getName()))
            .sorted(Comparator.comparing(FieldProto.Field::getName))
            .map(FieldProto.Field::getValue)
            .collect(Collectors.toList());
    return FeatureRowProto.FeatureRowKey.newBuilder()
        .addAllEntityValues(entityValues)
        .setFeatureSet(featureRow.getFeatureSet())
        .build();
  }

  public static class FilterDoFn
      extends DoFn<
          KV<FeatureRowProto.FeatureRowKey, FeatureRowProto.FeatureRow>,
          FeatureRowProto.FeatureRow> {
    private static final String LAST_UPDATED = "last_updated";

    FilterDoFn() {}

    @StateId(LAST_UPDATED)
    private final StateSpec<ValueState<Long>> lastUpdatedStateSpec =
        StateSpecs.value(VarLongCoder.of());

    @ProcessElement
    public void processElement(
        ProcessContext context, @StateId("last_updated") ValueState<Long> lastUpdatedState) {
      Long lastUpdatedTimestamp = lastUpdatedState.read();

      FeatureRowProto.FeatureRow featureRow = context.element().getValue();
      Long currentTimestamp = featureRow.getEventTimestamp().getSeconds();
      if (lastUpdatedTimestamp == null || currentTimestamp > lastUpdatedTimestamp) {
        context.output(featureRow);
        lastUpdatedState.write(currentTimestamp);
      }
    }
  }

  @Override
  public PCollection<FeatureRowProto.FeatureRow> expand(
      PCollection<FeatureRowProto.FeatureRow> featureRowCollection) {
    return featureRowCollection
        .apply(
            WithKeys.of(
                new SerializableFunction<
                    FeatureRowProto.FeatureRow, FeatureRowProto.FeatureRowKey>() {
                  @Override
                  public FeatureRowProto.FeatureRowKey apply(FeatureRowProto.FeatureRow input) {
                    return getKey(input);
                  }
                }))
        .apply(ParDo.of(new FilterDoFn()));
  }
}
