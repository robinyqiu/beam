/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.zetasql.type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.zetasql.functions.ZetaSQLDateTime.DateTimestampPart;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamZetaSqlDateTimePart}. */
@RunWith(JUnit4.class)
public class BeamZetaSqlDateTimePartTest {

  @Test
  public void testDefinitionConsistentWithZetaSql() {
    BeamZetaSqlDateTimePart[] valuesBeam = BeamZetaSqlDateTimePart.values();
    DateTimestampPart[] valuesZetaSql = DateTimestampPart.values();
    assertEquals(valuesBeam.length, valuesZetaSql.length);

    for (DateTimestampPart valueZetaSql : valuesZetaSql) {
      BeamZetaSqlDateTimePart valueBeam =
          BeamZetaSqlDateTimePart.forNumber(valueZetaSql.getNumber());
      assertNotNull(valueBeam);
      assertEquals(valueBeam.name(), valueZetaSql.name());
    }
  }
}
