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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.platform.JavaPlatform;

import org.apache.avro.generic.GenericRecord;
// import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class JavaParquetSource extends ParquetSource implements JavaExecutionOperator {

    public JavaParquetSource(String inputPath) {
        super(inputPath);
    }

    public JavaParquetSource(ParquetSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == 0;
        assert outputs.length == 1;

        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        String inputPath = this.getInputPath();

        Configuration conf = new Configuration();
        Path parquetFilePath = new Path(inputPath);

        ParquetReader<GenericRecord> reader;
        try {
            reader = AvroParquetReader.<GenericRecord>builder(parquetFilePath)
                    .withConf(conf)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create ParquetReader for: " + inputPath, e);
        }

        Iterable<GenericRecord> iterable = () -> new Iterator<GenericRecord>() {
            private GenericRecord nextRecord = fetchNext();

            private GenericRecord fetchNext() {
                try {
                    return reader.read();
                } catch (IOException e) {
                    throw new RuntimeException("Error reading Parquet file.", e);
                }
            }

            @Override
            public boolean hasNext() {
                return nextRecord != null;
            }

            @Override
            public GenericRecord next() {
                if (nextRecord == null) {
                    throw new NoSuchElementException();
                }
                GenericRecord current = nextRecord;
                nextRecord = fetchNext();
                return current;
            }
        };

        Stream<GenericRecord> recordStream = StreamSupport.stream(iterable.spliterator(), false)
                .onClose(() -> {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        throw new RuntimeException("Error closing ParquetReader.", e);
                    }
                });

        output.accept(recordStream);

        ExecutionLineageNode execNode = new ExecutionLineageNode(operatorContext);
        output.getLineage().addPredecessor(execNode);
        return new Tuple<>(Collections.singleton(execNode), Collections.emptyList());
    }

    @Override
    public JavaParquetSource copy() {
        return new JavaParquetSource(this.getInputPath());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.parquetsource.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("ParquetSource has no input channels.");
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public String getName() {
        return String.format("JavaParquetSource (%s)", this.getInputPath());
    }
}