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


package org.apache.wayang.apps.wordcount;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.avro.Schema;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

public class ParquetWordCountJob {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 3) {
            System.err.println("Usage: <platform> <input CSV file> <input Parquet file>");
            System.exit(1);
        }

        String platform = args[0];
        String csvInput = args[1];
        String parquetInput = args[2];

        WayangContext wayangContext = new WayangContext();
        switch (platform) {
            case "java":
                wayangContext.register(Java.basicPlugin());
                break;
            default:
                System.err.format("Unknown platform: \"%s\"\n", platform);
                System.exit(3);
                return;
        }

        // yelp review record schema
        Schema fullSchema = new Schema.Parser().parse(
            "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ReviewRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"label\", \"type\": \"long\"},\n" +
            "    {\"name\": \"text\", \"type\": \"string\"}\n" +
            "  ]\n" +
            "}"
        );

        // tried to implement the pushdown projection but it didn't work as expected so just leaving it here as a comment

        // yelp review record schema with label as optional
        // Schema partialSchema = new Schema.Parser().parse(
        //     "{\n" +
        //     "  \"type\":\"record\",\n" +
        //     "  \"name\":\"ReviewRecord\",\n" +
        //     "  \"fields\":[\n" +
        //     // "    {\"name\":\"label\", \"type\":[\"null\",\"long\"], \"default\":null},\n" +
        //     "    {\"name\":\"text\", \"type\":\"string\"}\n" +
        //     "  ]\n" +
        //     "}"
        // );

        // 6 total runs: 1 warm-up run (not recorded) + 5 measured runs
        int totalRuns = 6;
        int warmUpRuns = 1;
        int measuredRuns = totalRuns - warmUpRuns;

        long[] csvTimes = new long[measuredRuns];

        // warm-up run csv
        new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount-CSV-Warmup")
                .withUdfJarOf(ParquetWordCountJob.class)
                .readTextFile(csvInput)
                .flatMap(line -> {
                    String[] parts = line.split(",", 2);
                    String text = parts.length > 1 ? parts[1] : "";
                    text = text.replaceAll("^\"|\"$", "");
                    return Arrays.asList(text.split("\\W+"));
                })
                .filter(token -> !token.isEmpty())
                .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .collect();

        // measured runs csv
        for (int i = 0; i < measuredRuns; i++) {
            JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("WordCount-CSV-Run" + (i + 1))
                    .withUdfJarOf(ParquetWordCountJob.class);

            long start = System.currentTimeMillis();
            Collection<Tuple2<String, Integer>> result = planBuilder
                    .readTextFile(csvInput)
                    .flatMap(line -> {
                        String[] parts = line.split(",", 2);
                        String text = parts.length > 1 ? parts[1] : "";
                        text = text.replaceAll("^\"|\"$", "");
                        return Arrays.asList(text.split("\\W+"));
                    })
                    .filter(token -> !token.isEmpty())
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .collect();
            long end = System.currentTimeMillis();
            csvTimes[i] = (end - start);

            int totalWords = result.stream().mapToInt(Tuple2::getField1).sum();
            System.out.printf("Found %d words from CSV run\n", result.size(), i+1);
            System.out.println("CSV Run " + (i + 1) + ": " + csvTimes[i] + " ms");
            // uncomment the following line to print out the words
            // result.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
            // System.out.println();
        }
        double avgCsvTime = Arrays.stream(csvTimes).average().orElse(Double.NaN);


        // parquet full schema read without projection push down
        long[] parquetFullTimes = new long[measuredRuns];

        // warm-up run parquet full
        new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount-ParquetFull-Warmup")
                .withUdfJarOf(ParquetWordCountJob.class)
                .readParquetFile(parquetInput, fullSchema)
                .flatMap(record -> Arrays.asList(record.get("text").toString().split("\\W+")))
                .filter(token -> !token.isEmpty())
                .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .collect();

        // measured runs parquet full
        for (int i = 0; i < measuredRuns; i++) {
            JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("WordCount-ParquetFull-Run" + (i + 1))
                    .withUdfJarOf(ParquetWordCountJob.class);

            long start = System.currentTimeMillis();
            Collection<Tuple2<String, Integer>> result = planBuilder
                    .readParquetFile(parquetInput, fullSchema)
                    .flatMap(record -> Arrays.asList(record.get("text").toString().split("\\W+")))
                    .filter(token -> !token.isEmpty())
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .collect();
            long end = System.currentTimeMillis();
            parquetFullTimes[i] = (end - start);

            System.out.printf("Found %d words from Parquet (full Schema) run %d\n", result.size(), i+1);
            System.out.println("Parquet (full Schema) Run " + (i + 1) + ": " + parquetFullTimes[i] + " ms");
            
            // uncomment the following line to print out the words
            // result.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
            // System.out.println();
        }
        double avgParquetFullTime = Arrays.stream(parquetFullTimes).average().orElse(Double.NaN);


        // // parquet partial schema - projection push down
        // long[] parquetPartialTimes = new long[measuredRuns];

        // // warm-up run parquet partial - projection push down
        // new JavaPlanBuilder(wayangContext)
        //         .withJobName("WordCount-ParquetPartial-Warmup")
        //         .withUdfJarOf(ParquetWordCountJob.class)
        //         .readParquetFile(parquetInput, partialSchema)
        //         .flatMap(record -> Arrays.asList(record.get("text").toString().split("\\W+")))
        //         .filter(token -> !token.isEmpty())
        //         .map(word -> new Tuple2<>(word.toLowerCase(), 1))
        //         .reduceByKey(
        //                 Tuple2::getField0,
        //                 (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
        //         )
        //         .collect();

        // // measured runs parquet partial - projection push down
        // for (int i = 0; i < measuredRuns; i++) {
        //     JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
        //             .withJobName("WordCount-ParquetPartial-Run" + (i + 1))
        //             .withUdfJarOf(ParquetWordCountJob.class);

        //     long start = System.currentTimeMillis();
        //     Collection<Tuple2<String, Integer>> result = planBuilder
        //             .readParquetFile(parquetInput, partialSchema)
        //             .flatMap(record -> Arrays.asList(record.get("text").toString().split("\\W+")))
        //             .filter(token -> !token.isEmpty())
        //             .map(word -> new Tuple2<>(word.toLowerCase(), 1))
        //             .reduceByKey(
        //                     Tuple2::getField0,
        //                     (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
        //             )
        //             .collect();
        //     long end = System.currentTimeMillis();
        //     parquetPartialTimes[i] = (end - start);

        //     System.out.printf("Found %d words from Parquet (partial schema) run %d\n", result.size(), i+1);
        //     System.out.println("Parquet (partial schema) Run " + (i + 1) + ": " + parquetPartialTimes[i] + " ms");
            
        //     // uncomment the following line to print out the words
        //     // result.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
        //     // System.out.println();
        // }
        // double avgParquetPartialTime = Arrays.stream(parquetPartialTimes).average().orElse(Double.NaN);

        // average times over the measured runs
        System.out.println("\n=== Averages (based on 5 measured runs) ===");
        System.out.println("Average CSV Runtime: " + avgCsvTime + " ms");
        System.out.println("Average Parquet (full schema) Runtime: " + avgParquetFullTime + " ms");
        // System.out.println("Average Parquet (partial schema) Runtime: " + avgParquetPartialTime + " ms");
        System.out.println();
        // System.out.println("New experiment starting...");
        System.out.println();
    }
}
