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


package org.apache.wayang.api

import org.apache.wayang.api.serialization.SerializationUtils
import org.apache.wayang.basic.operators.{ObjectFileSink, SampleOperator, TextFileSink}
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.util.ReflectionUtils

import java.io.{FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class MultiContextDataQuanta[Out: ClassTag](val dataQuantaMap: Map[Long, DataQuanta[Out]])(val multiContextPlanBuilder: MultiContextPlanBuilder) {

  private def wrapInMultiContextDataQuanta[NewOut: ClassTag](f: DataQuanta[Out] => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](dataQuantaMap.mapValues(f))(this.multiContextPlanBuilder)

  private def wrapInMultiContextDataQuanta2[ThatOut: ClassTag, NewOut: ClassTag](thatMultiContextDataQuanta: MultiContextDataQuanta[ThatOut],
                                                                                 f: (DataQuanta[Out], DataQuanta[ThatOut]) => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](this.dataQuantaMap.map { case (key, thisDataQuanta) =>
      val thatDataQuanta = thatMultiContextDataQuanta.dataQuantaMap(key)
      key -> f(thisDataQuanta, thatDataQuanta)
    })(this.multiContextPlanBuilder)

  def withTargetPlatforms(platforms: Platform*): MultiContextDataQuanta[Out] = {
    new MultiContextDataQuanta[Out](this.dataQuantaMap.mapValues(_.withTargetPlatforms(platforms: _*)))(multiContextPlanBuilder)
  }

  def withTargetPlatforms(blossomContext: BlossomContext, platforms: Platform*): MultiContextDataQuanta[Out] = {
    val updatedDataQuanta = dataQuantaMap(blossomContext.id).withTargetPlatforms(platforms: _*)
    val updatedDataQuantaMap = dataQuantaMap.updated(blossomContext.id, updatedDataQuanta)
    new MultiContextDataQuanta[Out](updatedDataQuantaMap)(this.multiContextPlanBuilder)
  }

  def map[NewOut: ClassTag](udf: Out => NewOut): MultiContextDataQuanta[NewOut] =
    wrapInMultiContextDataQuanta(_.map(udf))

  def mapPartitions[NewOut: ClassTag](udf: Iterable[Out] => Iterable[NewOut]): MultiContextDataQuanta[NewOut] =
    wrapInMultiContextDataQuanta(_.mapPartitions(udf))

  def filter(udf: Out => Boolean): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.filter(udf))

  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut]): MultiContextDataQuanta[NewOut] =
    wrapInMultiContextDataQuanta(_.flatMap(udf))

  def sample(sampleSize: Int,
             datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
             seed: Option[Long] = None,
             sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.sample(sampleSize, datasetSize, seed, sampleMethod))

  def sampleDynamic(sampleSizeFunction: Int => Int,
                    datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                    seed: Option[Long] = None,
                    sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.sampleDynamic(sampleSizeFunction, datasetSize, seed, sampleMethod))

  def reduceByKey[Key: ClassTag](keyUdf: Out => Key,
                                 udf: (Out, Out) => Out): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.reduceByKey(keyUdf, udf))

  def groupByKey[Key: ClassTag](keyUdf: Out => Key): MultiContextDataQuanta[java.lang.Iterable[Out]] =
    wrapInMultiContextDataQuanta(_.groupByKey(keyUdf))

  def reduce(udf: (Out, Out) => Out): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.reduce(udf))

  def union(that: MultiContextDataQuanta[Out]): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta2(that, (thisDataQuanta, thatDataQuanta: DataQuanta[Out]) => thisDataQuanta.union(thatDataQuanta))

  def intersect(that: MultiContextDataQuanta[Out]): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta2(that, (thisDataQuanta, thatDataQuanta: DataQuanta[Out]) => thisDataQuanta.intersect(thatDataQuanta))

  import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}

  def join[ThatOut: ClassTag, Key: ClassTag](thisKeyUdf: Out => Key,
                                             that: MultiContextDataQuanta[ThatOut],
                                             thatKeyUdf: ThatOut => Key)
  : MultiContextDataQuanta[WayangTuple2[Out, ThatOut]] =
    wrapInMultiContextDataQuanta2(that, (thisDataQuanta, thatDataQuanta: DataQuanta[ThatOut]) => thisDataQuanta.join(thisKeyUdf, thatDataQuanta, thatKeyUdf))

  def coGroup[ThatOut: ClassTag, Key: ClassTag](thisKeyUdf: Out => Key,
                                                that: MultiContextDataQuanta[ThatOut],
                                                thatKeyUdf: ThatOut => Key)
  : MultiContextDataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] =
    wrapInMultiContextDataQuanta2(that, (thisDataQuanta, thatDataQuanta: DataQuanta[ThatOut]) => thisDataQuanta.coGroup(thisKeyUdf, thatDataQuanta, thatKeyUdf))

  def cartesian[ThatOut: ClassTag](that: MultiContextDataQuanta[ThatOut])
  : MultiContextDataQuanta[WayangTuple2[Out, ThatOut]] =
    wrapInMultiContextDataQuanta2(that, (thisDataQuanta, thatDataQuanta: DataQuanta[ThatOut]) => thisDataQuanta.cartesian(thatDataQuanta))

  def sort[Key: ClassTag](keyUdf: Out => Key): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.sort(keyUdf))

  def zipWithId: MultiContextDataQuanta[WayangTuple2[java.lang.Long, Out]] =
    wrapInMultiContextDataQuanta(_.zipWithId)

  def distinct: MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.distinct)

  def count: MultiContextDataQuanta[java.lang.Long] =
    wrapInMultiContextDataQuanta(_.count)

  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut]),
                                 numExpectedIterations: Int = 20)
  : MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.doWhile(udf, bodyBuilder, numExpectedIterations))

  def repeat(n: Int, bodyBuilder: DataQuanta[Out] => DataQuanta[Out]): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.repeat(n, bodyBuilder))


  def execute(): Unit = {

    val tempFilesToBeDeleted: ListBuffer[Path] = new ListBuffer[Path]() // To store the temp file names
    val processes: ListBuffer[Process] = ListBuffer() // To store the processes

    // For spawning child process using wayang-submit under wayang home
    val wayangHome = System.getenv("WAYANG_HOME")

    multiContextPlanBuilder.blossomContexts.foreach {
      context =>

        // Write context to temp file
        val multiContextPlanBuilderPath = MultiContextDataQuanta.writeToTempFileAsString(
          new MultiContextPlanBuilder(List(context)).withUdfJarsOf(multiContextPlanBuilder.withClassesOf: _*)
        )

        // Write operator to temp file
        val operatorPath = MultiContextDataQuanta.writeToTempFileAsString(dataQuantaMap(context.id).operator)
        tempFilesToBeDeleted += operatorPath

        tempFilesToBeDeleted += multiContextPlanBuilderPath

        println(s"About to start a process with args ${(operatorPath, multiContextPlanBuilderPath)}")

        // Spawn child process
        val processBuilder = new ProcessBuilder(
          s"$wayangHome/bin/wayang-submit",
          "org.apache.wayang.api.MultiContextDataQuanta",
          operatorPath.toString,
          multiContextPlanBuilderPath.toString)

        // Redirect children out to parent out
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

        val process = processBuilder.start()  // Start process
        processes += process // Store the process for later
    }

    // Wait for all processes to complete
    processes.foreach(_.waitFor())

    // Delete all temporary files
    tempFilesToBeDeleted.foreach(path => Files.deleteIfExists(path))
  }


  def executeAndReadSources(mergeContext: WayangContext): List[DataQuanta[Out]] = {

    // Execute multi context job
    this.execute()

    // Create plan builder for the new merge context
    val planBuilder = new PlanBuilder(mergeContext).withUdfJarsOf(classOf[MultiContextDataQuanta[_]])

    // Sources to merge
    var sources: List[DataQuanta[Out]] = List()

    // For each context, read its output from object file
    multiContextPlanBuilder.blossomContexts.foreach(context =>
      context.getSink match {

        case Some(objectFileSink: BlossomContext.ObjectFileSink) =>
          sources = sources :+ planBuilder.readObjectFile[Out](objectFileSink.textFileUrl)

        case None =>
          throw new WayangException("All contexts must be attached to an output sink.")

        case _ =>
          throw new WayangException("Invalid sink.")
      }
    )

    // Return list of DataQuanta
    sources
  }


  def mergeUnion(mergeContext: WayangContext): DataQuanta[Out] = {
    val sources: List[DataQuanta[Out]] = executeAndReadSources(mergeContext)
    sources.reduce((dq1, dq2) => dq1.union(dq2))
  }

}


object MultiContextDataQuanta {
  def main(args: Array[String]): Unit = {
    println("New process here")
    println(args.mkString("Array(", ", ", ")"))

    if (args.length != 2) {
      System.err.println("Expected two arguments: paths to the serialized operator and context.")
      System.exit(1)
    }

    // Parse file paths
    val operatorPath = Path.of(args(0))
    val multiContextPlanBuilderPath = Path.of(args(1))

    // Parse operator and multiContextPlanBuilder
    val operator = MultiContextDataQuanta.readFromTempFileFromString[Operator](operatorPath)
    val multiContextPlanBuilder = MultiContextDataQuanta.readFromTempFileFromString[MultiContextPlanBuilder](multiContextPlanBuilderPath)

    // Get context
    val context = multiContextPlanBuilder.blossomContexts.head

    // Get classes of and also add this one
    var withClassesOf = multiContextPlanBuilder.withClassesOf
    withClassesOf = withClassesOf :+ classOf[MultiContextDataQuanta[_]]

    // Get out output type to create sink with
    val outType = operator.getOutput(0).getType.getDataUnitType.getTypeClass

    // Connect to sink and execute plan
    context.getSink match {
      case Some(textFileSink: BlossomContext.TextFileSink) =>
        connectToSinkAndExecutePlan(new TextFileSink(textFileSink.textFileUrl, outType))

      case Some(objectFileSink: BlossomContext.ObjectFileSink) =>
        connectToSinkAndExecutePlan(new ObjectFileSink(objectFileSink.textFileUrl, outType))

      case None =>
        throw new WayangException("All contexts must be attached to an output sink.")

      case _ =>
        throw new WayangException("Invalid sink..")
    }


    def connectToSinkAndExecutePlan(sink: Operator): Unit = {
      operator.connectTo(0, sink, 0)
      context.execute(new WayangPlan(sink), withClassesOf.map(ReflectionUtils.getDeclaringJar).filterNot(_ == null): _*)
    }
  }


  def writeToTempFileAsBinary(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    val fos = new FileOutputStream(tempFile.toFile)
    try {
      fos.write(SerializationUtils.serialize(obj))
    } finally {
      fos.close()
    }
    tempFile
  }


  def readFromTempFileFromBinary[T : ClassTag](path: Path): T = {
    val fis = new FileInputStream(path.toFile)
    try {
      SerializationUtils.deserialize[T](fis.readAllBytes())
    } finally {
      fis.close()
      Files.deleteIfExists(path)
    }
  }


  def writeToTempFileAsString(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    val serializedString = SerializationUtils.serializeAsString(obj)
    Files.writeString(tempFile, serializedString, StandardCharsets.UTF_8)
    tempFile
  }


  def readFromTempFileFromString[T: ClassTag](path: Path): T = {
    val serializedString = Files.readString(path, StandardCharsets.UTF_8)
    val deserializedObject = SerializationUtils.deserializeFromString[T](serializedString)
    Files.deleteIfExists(path)
    deserializedObject
  }

}
