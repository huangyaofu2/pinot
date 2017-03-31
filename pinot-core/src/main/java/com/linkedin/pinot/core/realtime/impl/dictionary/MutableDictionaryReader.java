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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.indexsegment.generator.SegmentPartitionConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.math.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MutableDictionaryReader implements Dictionary {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableDictionaryReader.class);
  private final String column;
  protected BiMap<Integer, Object> dictionaryIdBiMap;
  protected boolean hasNull = false;
  private final AtomicInteger dictionaryIdGenerator;
  private PartitionFunction partitionFunction = null;
  private List<IntRange> partitionValues = null;
  private boolean columnPartitioned = true;


  public MutableDictionaryReader(String column) {
    this.column = column;
    this.dictionaryIdBiMap = HashBiMap.<Integer, Object> create();
    dictionaryIdGenerator = new AtomicInteger(-1);
  }

  public void setPartitioner(String column, SegmentPartitionConfig segmentPartitionConfig) {
    partitionFunction = segmentPartitionConfig.getPartitionFunction(column);
    partitionValues = segmentPartitionConfig.getPartitionRanges(column);
  }

  protected void addToDictionaryBiMap(Object val) {
    if (!dictionaryIdBiMap.inverse().containsKey(val)) {
      dictionaryIdBiMap.put(dictionaryIdGenerator.incrementAndGet(), val);
    }
  }

  @Override
  public int length() {
    return dictionaryIdGenerator.get() + 1;
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.INT);
  }

  @Override
  public void readLongValues(int[] dictionaryIds, int startPos, int limit, long[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.LONG);
  }

  @Override
  public void readFloatValues(int[] dictionaryIds, int startPos, int limit, float[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.FLOAT);
  }

  @Override
  public void readDoubleValues(int[] dictionaryIds, int startPos, int limit, double[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.DOUBLE);
  }


  @Override
  public void readStringValues(int[] dictionaryIds, int startPos, int limit, String[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.STRING);
  }

  protected void readValues(int[] dictionaryIds, int startPos, int limit, Object values, int outStartPos, FieldSpec.DataType type) {
    int endPos = startPos + limit;

    switch (type) {

      case INT: {
        int[] outValues = (int[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getIntValue(dictId);
        }

      }
      break;
      case LONG: {
        long[] outValues = (long[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getLongValue(dictId);
        }
      }
      break;
      case FLOAT: {
        float[] outValues = (float[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getFloatValue(dictId);
        }
      }
      break;
      case DOUBLE: {
        double[] outValues = (double[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getDoubleValue(dictId);
        }
      }
      break;
      case STRING: {
        String[] outValues = (String[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getStringValue(dictId);
        }
      }
      break;
    }

  }

  public abstract Object getSortedValues();

  protected Integer getIndexOfFromBiMap(Object val) {
    Integer ret = dictionaryIdBiMap.inverse().get(val);
    if (ret == null) {
      ret = -1;
    }
    return ret;
  }

  protected Object getRawValueFromBiMap(int dictionaryId) {
    return dictionaryIdBiMap.get(new Integer(dictionaryId));
  }

  public boolean hasNull() {
    return hasNull;
  }

  public abstract Object getMinVal();

  public abstract Object getMaxVal();

  public abstract void index(Object rawValue);

  @Override
  public abstract int indexOf(Object rawValue);

  public abstract boolean contains(Object o);

  @Override
  public abstract Object get(int dictionaryId);

  public abstract boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper);

  public boolean inRange(String lower, String upper, int valueToCompare) {
    return inRange(lower, upper, valueToCompare, true, true);
  }

  @Override
  public abstract long getLongValue(int dictionaryId);

  @Override
  public abstract double getDoubleValue(int dictionaryId);

  @Override
  public abstract int getIntValue(int dictionaryId);

  @Override
  public abstract float getFloatValue(int dictionaryId);

  @Override
  public abstract String toString(int dictionaryId);

  public void print() {
    System.out.println("************* printing dictionary for column : " + column + " ***************");
    for (Integer key : dictionaryIdBiMap.keySet()) {
      System.out.println(key + "," + dictionaryIdBiMap.get(key));
    }
    System.out.println("************************************");
  }

  public boolean isEmpty() {
    return dictionaryIdBiMap.isEmpty();
  }

  /**
   * Checks if the given value lies within one of the partition ranges. If the value is not within
   * any of the partition ranges, partitioning is dropped. The effect of that is:
   * <ul>
   *   <li> Subsequent values will not be checked against partition ranges. </li>
   *   <li> Partition information will not be written out to the metadata for this column. </li>
   * </ul>
   *
   * @param value Column value to check.
   */
  protected void checkPartition(Object value) {
    if (partitionFunction != null) {
       int partition = partitionFunction.getPartition(value);

      for (IntRange partitionValue : partitionValues) {
        if (partition >= partitionValue.getMinimumInteger() && partition <= partitionValue.getMaximumInteger()) {
          return;
        }
      }

      LOGGER.warn("Column '{}' not partitioned as specified for value: '{}', dropping partition metadata.", column,
          value);
      partitionFunction = null;
      partitionValues = null;
      columnPartitioned = false;
    }
  }

  /**
   * Returns true if column was detected to be partitioned as per the specified partitioning scheme,
   * false otherwise.
   *
   * @return True if partitioning failed, false otherwise.
   */
  public boolean isColumnPartitioned() {
    return columnPartitioned;
  }

  public void setColumnPartitioned(boolean columnPartitioned) {
    this.columnPartitioned = columnPartitioned;
  }

  /**
   * Returns the {@link PartitionFunction} for the column.
   * @return Partition function for the column.
   */
  public PartitionFunction getPartitionFunction() {
    return partitionFunction;
  }

  /**
   * Returns the partition ranges within which the column values exist.
   *
   * @return List of ranges for the column values.
   */
  public List<IntRange> getPartitionRanges() {
    return partitionValues;
  }
}
