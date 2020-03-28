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

import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto;
import feast.types.FeatureRowProto;
import feast.types.FieldProto;
import feast.types.ValueProto;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class FilterOutdatedFeatureRowTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  private Map<String, FeatureSetProto.FeatureSet> getFeatureSets() {
    Map<String, FeatureSetProto.FeatureSet> featureSets = new HashMap<>();
    featureSets.put(
        "project/feature_set:1",
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetProto.FeatureSetSpec.newBuilder()
                    .addEntities(
                        FeatureSetProto.EntitySpec.newBuilder()
                            .setName("entity")
                            .setValueType(ValueProto.ValueType.Enum.STRING))
                    .addFeatures(
                        FeatureSetProto.FeatureSpec.newBuilder()
                            .setName("feature")
                            .setValueType(ValueProto.ValueType.Enum.INT32)))
            .build());
    return featureSets;
  }

  private FeatureRowProto.FeatureRow newFeatureRow(
      String entityValue, Integer featureValue, Long secondsSinceEpoch) {
    return FeatureRowProto.FeatureRow.newBuilder()
        .setEventTimestamp(Timestamp.newBuilder().setSeconds(secondsSinceEpoch).build())
        .setFeatureSet("project/feature_set:1")
        .addFields(
            FieldProto.Field.newBuilder()
                .setName("entity")
                .setValue(ValueProto.Value.newBuilder().setStringVal(entityValue)))
        .addFields(
            FieldProto.Field.newBuilder()
                .setName("feature")
                .setValue(ValueProto.Value.newBuilder().setInt32Val(featureValue).build()))
        .build();
  }

  @Test
  public void shouldFilterOutdatedFeatureRow() {

    FeatureRowProto.FeatureRow feature1Recent = newFeatureRow("1001", 1, 90L);
    FeatureRowProto.FeatureRow feature2Recent = newFeatureRow("1002", 2, 80L);
    FeatureRowProto.FeatureRow feature1Outdated = newFeatureRow("1001", 3, 80L);

    TestStream<FeatureRowProto.FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRowProto.FeatureRow.class))
            .advanceWatermarkTo(new Instant(0L))
            .addElements(feature1Recent, feature2Recent, feature1Outdated)
            .advanceWatermarkToInfinity();

    PCollection<FeatureRowProto.FeatureRow> filtered =
        p.apply(featureRowTestStream)
            .apply(FilterOutdatedFeatureRow.newBuilder().setFeatureSets(getFeatureSets()).build());
    PAssert.that(filtered).containsInAnyOrder(feature1Recent, feature2Recent);
    p.run();
  }
}
