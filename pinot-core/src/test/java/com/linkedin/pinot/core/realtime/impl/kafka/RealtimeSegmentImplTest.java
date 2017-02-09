/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;


/**
 * Test for RealtimeSegmentImpl
 */
public class RealtimeSegmentImplTest {
  @Test
  public void testDropInvalidRows() throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("potato")
        .addSingleValueDimension("dimension", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.LONG)
        .addTime("time", TimeUnit.SECONDS, FieldSpec.DataType.LONG)
        .build();

    RealtimeSegmentImpl realtimeSegment = new RealtimeSegmentImpl(schema, 100, "noTable", "noSegment", schema.getSchemaName(),
        new ServerMetrics(new MetricsRegistry()), null);

    // Segment should be empty
    Assert.assertEquals(realtimeSegment.getRawDocumentCount(), 0);

    Map<String, Object> genericRowContents = new HashMap<>();
    genericRowContents.put("dimension", "potato");
    genericRowContents.put("metric", 1234L);
    genericRowContents.put("time", 4567L);
    GenericRow row = new GenericRow();
    row.init(genericRowContents);

    // Add a valid row
    boolean notFull = realtimeSegment.index(row);
    Assert.assertEquals(notFull, true);
    Assert.assertEquals(realtimeSegment.getRawDocumentCount(), 1);

    // Add an invalid row
    genericRowContents.put("metric", null);
    notFull = realtimeSegment.index(row);
    Assert.assertEquals(notFull, true);
    Assert.assertEquals(realtimeSegment.getRawDocumentCount(), 1);

    // Add another valid row
    genericRowContents.put("metric", 2222L);
    notFull = realtimeSegment.index(row);
    Assert.assertEquals(notFull, true);
    Assert.assertEquals(realtimeSegment.getRawDocumentCount(), 2);
  }
}
