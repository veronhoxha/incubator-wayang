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

package org.apache.wayang.spark.operators.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.KMeansOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.model.SparkMLModel;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.util.*;

public class SparkKMeansOperator extends KMeansOperator implements SparkExecutionOperator {

    private static final StructType schema = DataTypes.createStructType(
            new StructField[]{
                    DataTypes.createStructField(Attr.FEATURES, new VectorUDT(), false)
            }
    );

    private static Dataset<Row> data2Row(JavaRDD<double[]> inputRdd) {
        final JavaRDD<Row> rowRdd = inputRdd.map(e -> RowFactory.create(Vectors.dense(e)));
        return SparkSession.builder().getOrCreate().createDataFrame(rowRdd, schema);
    }

    public SparkKMeansOperator(int k) {
        super(k);
    }

    public SparkKMeansOperator(KMeansOperator that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        // TODO cached or uncached?
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        final JavaRDD<double[]> inputRdd = input.provideRdd();
        final Dataset<Row> df = data2Row(inputRdd);
        final KMeansModel model = new KMeans()
                .setK(this.k)
                .setFeaturesCol(Attr.FEATURES)
                .setPredictionCol(Attr.PREDICTION)
                .fit(df);
        final Model outputModel = new Model(model);
        output.accept(Collections.singletonList(outputModel));

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    public static class Model implements org.apache.wayang.basic.model.KMeansModel, SparkMLModel<double[], Integer> {
        private final KMeansModel model;

        public Model(KMeansModel model) {
            this.model = model;
        }

        @Override
        public int getK() {
            return model.getK();
        }

        @Override
        public double[][] getClusterCenters() {
            return Arrays.stream(model.clusterCenters()).map(Vector::toArray).toArray(double[][]::new);
        }

        @Override
        public JavaRDD<Tuple2<double[], Integer>> transform(JavaRDD<double[]> input) {
            final Dataset<Row> df = data2Row(input);
            final Dataset<Row> transform = model.transform(df);
            return transform.toJavaRDD()
                    .map(row -> new Tuple2<>(row.<Vector>getAs(Attr.FEATURES).toArray(), row.<Integer>getAs(Attr.PREDICTION)));
        }
    }
}
