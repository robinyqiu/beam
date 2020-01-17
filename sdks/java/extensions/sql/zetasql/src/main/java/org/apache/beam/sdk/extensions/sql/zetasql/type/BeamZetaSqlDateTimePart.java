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

import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;

/**
 * This class is a mirror of {@link com.google.zetasql.functions.ZetaSQLDateTime.DateTimestampPart}
 * with {@code toString()} overridden for week enums with weekdays.
 */
@Internal
public enum BeamZetaSqlDateTimePart {
  __DateTimePart__switch_must_have_a_default__(-1),
  YEAR(1),
  MONTH(2),
  DAY(3),
  DAYOFWEEK(4),
  DAYOFYEAR(5),
  QUARTER(6),
  HOUR(7),
  MINUTE(8),
  SECOND(9),
  MILLISECOND(10),
  MICROSECOND(11),
  NANOSECOND(12),
  DATE(13),
  WEEK(14),
  DATETIME(15),
  TIME(16),
  ISOYEAR(17),
  ISOWEEK(18),
  WEEK_MONDAY(19),
  WEEK_TUESDAY(20),
  WEEK_WEDNESDAY(21),
  WEEK_THURSDAY(22),
  WEEK_FRIDAY(23),
  WEEK_SATURDAY(24);

  private final int number;

  BeamZetaSqlDateTimePart(int number) {
    this.number = number;
  }

  public final int getNumber() {
    return this.number;
  }

  public static BeamZetaSqlDateTimePart forNumber(int value) {
    switch (value) {
      case -1:
        return __DateTimePart__switch_must_have_a_default__;
      case 0:
      default:
        return null;
      case 1:
        return YEAR;
      case 2:
        return MONTH;
      case 3:
        return DAY;
      case 4:
        return DAYOFWEEK;
      case 5:
        return DAYOFYEAR;
      case 6:
        return QUARTER;
      case 7:
        return HOUR;
      case 8:
        return MINUTE;
      case 9:
        return SECOND;
      case 10:
        return MILLISECOND;
      case 11:
        return MICROSECOND;
      case 12:
        return NANOSECOND;
      case 13:
        return DATE;
      case 14:
        return WEEK;
      case 15:
        return DATETIME;
      case 16:
        return TIME;
      case 17:
        return ISOYEAR;
      case 18:
        return ISOWEEK;
      case 19:
        return WEEK_MONDAY;
      case 20:
        return WEEK_TUESDAY;
      case 21:
        return WEEK_WEDNESDAY;
      case 22:
        return WEEK_THURSDAY;
      case 23:
        return WEEK_FRIDAY;
      case 24:
        return WEEK_SATURDAY;
    }
  }

  @Override
  public String toString() {
    String result = super.toString();
    if (getNumber() >= 19 && getNumber() <= 24) {
      List<String> tokens = Splitter.on('_').splitToList(result);
      result = tokens.get(0) + "(" + tokens.get(1) + ")";
    }
    return result;
  }
}
