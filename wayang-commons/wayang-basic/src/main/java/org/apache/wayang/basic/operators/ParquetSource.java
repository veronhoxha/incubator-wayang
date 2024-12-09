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


package org.apache.wayang.basic.operators;

import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * This source operator reads records from a Parquet file.
 */
public class ParquetSource extends UnarySource<GenericRecord> {

    private final String inputPath;
    private final Schema projectionSchema;

    /**
     * Constructor without projection schema.
     *
     * @param inputPath the path to the Parquet file
     */
    public ParquetSource(String inputPath) {
        this(inputPath, null);
    }

    /**
     * Constructor with projection schema.
     *
     * @param inputPath the path to the Parquet file
     * @param projectionSchema the schema specifying the columns to read
     */
    public ParquetSource(String inputPath, Schema projectionSchema) {
        super(DataSetType.createDefault(GenericRecord.class));
        this.inputPath = inputPath;
        this.projectionSchema = projectionSchema;
    }

    /**
     * Copy constructor.
     *
     * @param that that should be copied
     */
    public ParquetSource(ParquetSource that) {
        super(that);
        this.inputPath = that.inputPath;
        this.projectionSchema = that.projectionSchema;
    }

    public String getInputPath() {
        return inputPath;
    }

    public Schema getProjectionSchema() {
        return projectionSchema;
    }

    @Override
    public String getName() {
        return String.format("ParquetSource (%s)", this.inputPath);
    }
}