import java.io.{FileInputStream, PrintWriter}
import java.util
import java.util.{Date, Properties}

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.math._


object Colocation extends App {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkSession: SparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
//      .master("local[*]")
      .appName("Colocation").getOrCreate()
    GeoSparkSQLRegistrator.registerAll(sparkSession)

    val prop = new Properties()
    val resourcePath = System.getProperty("user.dir") + "/"
    val configPath = resourcePath + "conf/"
    val joinQueryPartitioningType = GridType.QUADTREE

    var ConfFile = new FileInputStream(configPath + "colo.point.properties")
    prop.load(ConfFile)
    val pointInputLocation = resourcePath + prop.getProperty("inputLocation")
    val pointLatOffset = prop.getProperty("latOffset").toInt
    val pointLonOffset = prop.getProperty("lonOffset").toInt
    val pointOffset = prop.getProperty("offset").toInt
    val pointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    val pointNumPartitions = prop.getProperty("numPartitions").toInt
    val pointIndexType = IndexType.RTREE
    val fromTime = prop.getProperty("fromTime")
    val toTime = prop.getProperty("toTime")
    val start = DateTime.parse(fromTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val end = DateTime.parse(toTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))


    ConfFile = new FileInputStream(configPath + "colo.poi.properties")
    prop.load(ConfFile)
    val poiInputLocation = resourcePath + prop.getProperty("inputLocation")
    val poiLatOffset = prop.getProperty("latOffset").toInt
    val poiLonOffset = prop.getProperty("lonOffset").toInt
    val poiOffset = prop.getProperty("offset").toInt
    val poiSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    val poiNumPartitions = prop.getProperty("numPartitions").toInt
    val poiIndexType = IndexType.RTREE
    val poiInputLocation2 = resourcePath + prop.getProperty("inputLocation2")

    ConfFile = new FileInputStream(configPath + "colo.polygon.properties")
    prop.load(ConfFile)
    val polygonInputLocation = resourcePath + prop.getProperty("inputLocation")
    val polygonOffset = prop.getProperty("offset").toInt
    val polygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    val polygonNumPartitions = prop.getProperty("numPartitions").toInt

    ConfFile = new FileInputStream(configPath + "colo.linestring.properties")
    prop.load(ConfFile)
    val lineStringInputLocation = resourcePath + prop.getProperty("inputLocation")

    ConfFile = new FileInputStream(configPath + "colo.run.properties")
    prop.load(ConfFile)
    val outputMode = prop.getProperty("outputMode").toInt
    val analysis = prop.getProperty("analysis")
    val storageLevel = StorageLevel.fromString(prop.getProperty("storageLevel"))
    val outputPath = prop.getProperty("outputPath")
    val correspondingTable = prop.getProperty("correspondingTable").toBoolean
    val firstX = prop.getProperty("firstX").toInt
    val epsg3857 = prop.getProperty("epsg3857").toBoolean
    val radius = prop.getProperty("radius").toDouble
    val radius2 = prop.getProperty("radius2").toDouble

    println("output: " + outputPath)


    //    filter(pointInputLocation)
    analysis match {
        case "distanceJoin" =>
            println("task: distanceJoin")
            println("point input: " + pointInputLocation)
            println("linestring input: " + lineStringInputLocation)
            distanceJoinQueryUsingIndex()
        case "spatialJoin" =>
            println("point input: " + pointInputLocation)
            println("poi input: " + poiInputLocation)
            println("task: spatialJoin")
            spatialJoinQueryUsingIndex()
        case "linestringBuffer" =>
            println("linestring input: " + lineStringInputLocation)
            println("task: linestringBuffer")
            linestringBuffer()
        case "pointBuffer" =>
            println("point input: " + poiInputLocation)
            println("task: linestringBuffer")
            pointBuffer(poiInputLocation, poiLonOffset, poiLatOffset)
        case "distJoinSQL" =>
            println("point input: " + pointInputLocation)
            println("linestring input: " + lineStringInputLocation)
            println("task: distJoinSQL")
            distanceJoinQuerySQL()
        case "distJoinSimp" =>
            println("point input: " + pointInputLocation)
            println("linestring input: " + lineStringInputLocation)
            println("task: distJoinSimp")
            distanceJoinSimplified()
        case "fishnet" =>
            println("point input: " + pointInputLocation)
            println("polygon input: " + polygonInputLocation)
            println("task: fishnet")
            fishnet()
        case "fishnet2" =>
            println("point input: " + pointInputLocation)
            println("polygon input: " + polygonInputLocation)
            println("task: fishnet")
            fishnet2()
        case "distanceJoinPoints" =>
            println("point input: " + pointInputLocation)
            println("poi input: " + poiInputLocation)
            println("task: distanceJoinPoints")
            distanceJoinPoints()
        case "doubleJoin" =>
            println("point input: " + pointInputLocation)
            println("poi input: " + poiInputLocation)
            println("poi input2: " + poiInputLocation2)
            println("task: doubleJoin")
            doubleJoin()
        case "elementsInPartition" =>
            println("point input: " + pointInputLocation)
            println("task: elementsInPartition")
            elementsInPartition(pointInputLocation)
    }
    println("Tasks finished")
    sparkSession.close()


    def transformLat(x: Double, y: Double): Double = {
        var ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * sqrt(abs(x))
        ret += (20.0 * sin(6.0 * x * Pi) + 20.0 * sin(2.0 * x * Pi)) * 2.0 / 3.0
        ret += (20.0 * sin(y * Pi) + 40.0 * sin(y / 3.0 * Pi)) * 2.0 / 3.0
        ret += (160.0 * sin(y / 12.0 * Pi) + 320 * sin(y * Pi / 30.0)) * 2.0 / 3.0
        ret
    }

    def transformLon(x: Double, y: Double): Double = {
        var ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * sqrt(abs(x))
        ret += (20.0 * sin(6.0 * x * Pi) + 20.0 * sin(2.0 * x * Pi)) * 2.0 / 3.0
        ret += (20.0 * sin(x * Pi) + 40.0 * sin(x / 3.0 * Pi)) * 2.0 / 3.0
        ret += (150.0 * sin(x / 12.0 * Pi) + 300.0 * sin(x / 30.0 * Pi)) * 2.0 / 3.0
        ret
    }

    def deltaLonLat(wgLon: Double, wgLat: Double, offset: Array[Double]): (Double, Double, Array[Double]) = {
        val AXIS = 6378245.0
        val OFFSET = 0.00669342162296594323
        var dLat = transformLat(wgLon - 105.0, wgLat - 35.0)
        var dLon = transformLon(wgLon - 105.0, wgLat - 35.0)
        val radLat = wgLat / 180.0 * Pi
        var magic = sin(radLat)
        magic = 1 - OFFSET * magic * magic
        val sqrtMagic = sqrt(magic)
        dLat = (dLat * 180.0) / ((AXIS * (1 - OFFSET)) / (magic * sqrtMagic) * Pi)
        dLon = (dLon * 180.0) / (AXIS / sqrtMagic * cos(radLat) * Pi)
        offset(1) = dLat
        offset(0) = dLon
        (wgLon, wgLat, offset)
    }


    def gcjToWGS84(gcjLon: Double, gcjLat: Double): (Double, Double) = {
        var wgLon = gcjLon
        var wgLat = gcjLat
        var deltaD = Array(0.0, 0.0)
        var deltalon = 1.0
        var deltalat = 1.0
        var itcnt = 0
        while ((deltalat > 1e-7 || deltalon > 1e-7) && (itcnt <= 5)) {
            val temp = deltaLonLat(wgLon, wgLat, deltaD)
            wgLon = temp._1
            wgLat = temp._2
            deltaD = temp._3
            if (itcnt == 0) {
                deltalon = abs(wgLon)
                deltalat = abs(wgLat)
            }
            else {
                deltalon = abs(gcjLon - deltaD(0) - wgLon)
                deltalat = abs(gcjLat - deltaD(1) - wgLat)
            }
            wgLon = gcjLon - deltaD(0)
            wgLat = gcjLat - deltaD(1)
            itcnt += 1
        }
        (wgLon, wgLat)
    }

    def readWithDF(filename: String, lonOffset: Int, latOffset: Int, sparkSession: SparkSession, epsg3857: Boolean, gcjToWGS: Boolean = false): SpatialRDD[Geometry] = {
        var spatialDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(filename)
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf.show()
        if (gcjToWGS) {
            //        def transLon = udf[Double, Double, Double]((x: Double, y: Double) => gcjToWGS84(x, y)._1)
            //        def transLat = udf[Double, Double, Double]((x: Double, y: Double) => gcjToWGS84(x, y)._2)
            sparkSession.udf.register("transLon", (x: Double, y: Double) => gcjToWGS84(x, y)._1)
            sparkSession.udf.register("transLat", (x: Double, y: Double) => gcjToWGS84(x, y)._2)
            //        spatialDf.withColumn("new_lng", transLon(spatialDf("lng"), spatialDf("lat")))
            //        spatialDf.withColumn("new_lat", transLat(spatialDf("lat"), spatialDf("lat")))
            spatialDf = sparkSession.sql("SELECT transLon(lng, lat) as lng, transLat(lng, lat) as lat FROM spatialDf")
            spatialDf.createOrReplaceTempView("spatialDf")
        }
        val temp = sparkSession.sql(s"select ST_Point(cast(spatialDf.lat as Decimal(24, 14)), cast(spatialDf.lng as Decimal(24, 14))) as point from spatialDf")
        val points = Adapter.toSpatialRdd(temp, "point")
        if (epsg3857) {
            points.CRSTransform("epsg:4326", "epsg:3857")
        }
        points.analyze()
        points
    }

    def elementsInPartition(filename: String): Unit = {
        val spatialDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "True").load(filename)
        spatialDf.createOrReplaceTempView("spatialDf")
        //        var points = new SpatialRDD[Geometry]
        var current = start
        while (end.getMillis - current.getMillis > 0) {
            //        for (i <- 0 until Days.daysBetween(start, end).getDays) {
            val plusOne = current.plusHours(1)
            //            val temp = sparkSession.sql(s"select ST_Point(cast(lat as Decimal(24, 14)), cast(lng as Decimal(24, 14))) as point from spatialDf WHERE sysTime > UNIX_TIMESTAMP('$fromTime') * 1000 AND sysTime < UNIX_TIMESTAMP('$toTime') * 1000")
            val temp = sparkSession.sql(s"select ST_Point(cast(lat as Decimal(24, 14)), cast(lng as Decimal(24, 14))) as point from spatialDf WHERE sysTime > UNIX_TIMESTAMP('${current.toLocalDateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH"))}', 'yyyy-MM-dd HH') * 1000 AND sysTime < UNIX_TIMESTAMP('${plusOne.toLocalDateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH"))}', 'yyyy-MM-dd HH') * 1000")
            if (!temp.head(1).isEmpty) {
                val points = Adapter.toSpatialRdd(temp, "point")
                points.analyze()
                points.boundaryEnvelope = new Envelope(39.59634, 40.3898263, 115.8528796421, 116.94819)
                points.spatialPartitioning(GridType.EQUALGRID, pointNumPartitions)
                val z = points.getPartitioner.getGrids.toArray()
                val test = sparkSession.sparkContext.parallelize(z).map(_.toString).zipWithIndex().map { case (k, v) => (v, k) }
                val x = points
                  .spatialPartitionedRDD
                  .rdd
                  .mapPartitionsWithIndex { case (i, rows) => Iterator((i, rows.size)) }
                //          .toDf("partition_number","number_of_records")
                //          .show()
                val temp1 = sparkSession.createDataFrame(test).toDF("partition_number", "env").as("env")
                val temp2 = sparkSession.createDataFrame(x).toDF("partition_number", "number_of_records").as("record") //
                val res = temp1.join(temp2, col("env.partition_number") === col("record.partition_number"), "inner").select("env", "number_of_records")
                res.write.mode("overwrite").csv(outputPath + "/" + current.toLocalDateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH")))
            }
            current = current.plusHours(1)
        }
    }


    def spatialJoinQueryUsingIndex(): Unit = {
        val start = new Date().getTime / 1000
        val pois = readWithDF(poiInputLocation, poiLonOffset, poiLatOffset, sparkSession, epsg3857)
        val pointRDD = readWithDF(pointInputLocation, pointLonOffset, pointLatOffset, sparkSession, epsg3857)
        pointRDD.spatialPartitioning(joinQueryPartitioningType)
        pointRDD.buildIndex(pointIndexType, true)
        val queryWindowRDD = new CircleRDD(pois, radius)
        queryWindowRDD.analyze()
        queryWindowRDD.spatialPartitioning(pointRDD.getPartitioner)
        val buffer = new Date().getTime / 1000
        println("Reading and indexing finished, elapsed time " + (buffer - start) + "s")
        val result = JoinQuery.SpatialJoinQuery(pointRDD, queryWindowRDD, true, false)
        writeToFile(result, analysis)
        val end = new Date().getTime / 1000
        println("JoinQuery and output finished, elapsed time " + (end - buffer) + "s")
    }

    def distanceJoinQueryUsingIndex(): Unit = {
        val start = new Date().getTime / 1000
        val pointRDD = readWithDF(pointInputLocation, pointLonOffset, pointLatOffset, sparkSession, epsg3857)
        val lineStringRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, lineStringInputLocation)
        var rawSpatialDf = Adapter.toDf(lineStringRDD, sparkSession)
        rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
        val query = "SELECT ST_GeomFromWKT(geometry) AS geom FROM rawSpatialDf"
        rawSpatialDf = sparkSession.sql(query)
        rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
        rawSpatialDf.show()
        val spatialDf = sparkSession.sql(s"SELECT ST_Buffer(geom, $radius) AS buff FROM rawSpatialDf")
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf.show()
        val queryWindowRDD = Adapter.toSpatialRdd(spatialDf, "buff")
        if (epsg3857) {
            queryWindowRDD.CRSTransform("epsg:3857", "epsg:3857")
        }
        queryWindowRDD.analyze()
        pointRDD.spatialPartitioning(joinQueryPartitioningType)
        queryWindowRDD.spatialPartitioning(pointRDD.getPartitioner)
        pointRDD.buildIndex(pointIndexType, true)
        val buffer = new Date().getTime / 1000
        println("Reading, buffering and indexing finished, elapsed time " + (buffer - start) + "s")
        val result = JoinQuery.SpatialJoinQueryWithDuplicates(pointRDD, queryWindowRDD, true, false)
        writeToFile(result, analysis)
        val end = new Date().getTime / 1000
        println("JoinQuery and output finished, elapsed time " + (end - buffer) + "s")
    }

    def distanceJoinQuerySQL(): Unit = {
        var pointDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(pointInputLocation)
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT *, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS point"
          + ", from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date FROM pointDf"
          + s" WHERE sysTime > UNIX_TIMESTAMP('$fromTime') * 1000 AND sysTime < UNIX_TIMESTAMP('$toTime') * 1000")
        pointDf.createOrReplaceTempView("pointDf")
        pointDf.show()
        val lineStringRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, lineStringInputLocation)
        var lineStringDf = Adapter.toDf(lineStringRDD, sparkSession)
        lineStringDf.createOrReplaceTempView("lineStringDf")
        lineStringDf = sparkSession.sql("SELECT *, ST_GeomFromWKT(geometry) AS geom FROM lineStringDf")
        lineStringDf.createOrReplaceTempView("lineStringDf")
        lineStringDf = sparkSession.sql(s"SELECT *, ST_Buffer(geom, $radius) AS buff FROM lineStringDf")
        lineStringDf.createOrReplaceTempView("bufferDf")
        val distJoin = sparkSession.sql("SELECT ID, date, COUNT(*) as count FROM pointDf, bufferDf WHERE ST_Within(point, buff) GROUP BY ID, date ORDER BY date, ID")
        distJoin.createOrReplaceTempView("distJoinDf")
        distJoin.write.option("header", "true").mode("overwrite").csv(outputPath)
    }

    def distanceJoinSimplified(): Unit = {
        var pointDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(pointInputLocation)
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT *, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS point"
          + ", from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date FROM pointDf"
          + s" WHERE sysTime > UNIX_TIMESTAMP('$fromTime') * 1000 AND sysTime < UNIX_TIMESTAMP('$toTime') * 1000")
        pointDf.createOrReplaceTempView("pointDf")
        pointDf.show()
        val lineStringRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, lineStringInputLocation)
        var lineStringDf = Adapter.toDf(lineStringRDD, sparkSession)
        lineStringDf.createOrReplaceTempView("lineStringDf")
        lineStringDf = sparkSession.sql("SELECT *, ST_GeomFromWKT(geometry) AS geom FROM lineStringDf")
        lineStringDf.createOrReplaceTempView("lineStringDf")
        lineStringDf = sparkSession.sql(s"SELECT *, ST_Buffer(geom, $radius) AS buff FROM lineStringDf")
        lineStringDf.createOrReplaceTempView("lineStringDf")
        val distJoin = sparkSession.sql("SELECT ID, date, COUNT(*) as count FROM pointDf, lineStringDf WHERE ST_Within(point, buff) GROUP BY ID, date")
        distJoin.createOrReplaceTempView("distJoinDf")
        distJoin.write.option("header", "true").mode("overwrite").csv(outputPath)
    }


    def distanceJoinPoints(): Unit = {
        var pointDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(pointInputLocation)
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS point"
          + ", from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date FROM pointDf"
          + s" WHERE sysTime > UNIX_TIMESTAMP('${start.toLocalDate.toString()}','yyyy-MM-dd') * 1000 AND sysTime < UNIX_TIMESTAMP('${end.toLocalDate.toString()}','yyyy-MM-dd') * 1000")
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT point, date FROM pointDf WHERE ST_Within(point, ST_Transform(ST_GeomFromText('POLYGON((116.230633649 39.979627788,116.230633649 39.863608449,116.44120325 39.863608449,116.44120325 39.979627788,116.230633649 39.979627788))'),'epsg:4326', 'epsg:3857'))")
        pointDf.show(false)
        var poiDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(poiInputLocation)
        poiDf.createOrReplaceTempView("poiDf")
        poiDf = sparkSession.sql("SELECT OBJECTID, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS poi FROM poiDf")
        poiDf.createOrReplaceTempView("poiDf")
        poiDf.show()
        val distJoin = sparkSession.sql(s"SELECT OBJECTID, date, COUNT(*) as Join_Count FROM pointDf, poiDf WHERE ST_Distance(point, poi) <= $radius GROUP BY OBJECTID, date ORDER BY date")
        distJoin.createOrReplaceTempView("distJoinDf")
        distJoin.write.option("header", "true").mode("overwrite").csv(outputPath)
    }

    def doubleJoin(): Unit = {
        var poiDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(poiInputLocation)
        poiDf.createOrReplaceTempView("poiDf")
        poiDf = sparkSession.sql("SELECT OBJECTID, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS poi FROM poiDf")
        poiDf.createOrReplaceTempView("poiDf")
        poiDf.show() // normal areas
        var poiDf2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(poiInputLocation2)
        poiDf2.createOrReplaceTempView("poiDf2")
        poiDf2.show() // infected areas
        poiDf2 = sparkSession.sql("SELECT date, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS poi2 FROM poiDf2")
        poiDf2.createOrReplaceTempView("poiDf2")
        var distJoinPoi = sparkSession.sql(s"SELECT OBJECTID, poi, date FROM poiDf, poiDf2 WHERE ST_Distance(poi, poi2) <= $radius2")
        distJoinPoi.createOrReplaceTempView("poiDf")
        distJoinPoi.show()
        //        distJoinPoi = sparkSession.sql("SELECT (UNIX_TIMESTAMP(poiDf.date,'yyyy-MM-dd')+864000)*1000 FROM poiDf")
        //        distJoinPoi.show()
        var pointDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(pointInputLocation)
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT sysTime, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:3857') AS point"
          + ", from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date FROM pointDf"
          + s" WHERE sysTime > UNIX_TIMESTAMP('${start.toLocalDate.toString()}','yyyy-MM-dd') * 1000 AND sysTime < UNIX_TIMESTAMP('${end.toLocalDate.toString()}','yyyy-MM-dd') * 1000")
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT point, date, sysTime FROM pointDf WHERE ST_Within(point, ST_Transform(ST_GeomFromText('POLYGON((116.230633649 39.979627788,116.230633649 39.863608449,116.44120325 39.863608449,116.44120325 39.979627788,116.230633649 39.979627788))'),'epsg:4326', 'epsg:3857'))")
        pointDf.createOrReplaceTempView("pointDf")
        pointDf.show(false)
        val distJoin = sparkSession.sql(s"SELECT OBJECTID, pointDf.date, COUNT(*) as Join_Count FROM pointDf, poiDf WHERE ST_Distance(point, poi) <= $radius AND sysTime>= (UNIX_TIMESTAMP(poiDf.date,'yyyy-MM-dd')-864000)*1000 AND sysTime <= (UNIX_TIMESTAMP(poiDf.date,'yyyy-MM-dd')+864000)*1000 GROUP BY OBJECTID, pointDf.date ORDER BY pointDf.date")
        distJoin.createOrReplaceTempView("distJoinDf")
        distJoin.write.option("header", "true").mode("overwrite").csv(outputPath)
    }

    def fishnet(): Unit = {
        for (i <- (0 until Days.daysBetween(start, end).getDays)) {
            val current = start.plusDays(i)
            val plusOne = current.plusDays(1)
            var pointDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(pointInputLocation)
            pointDf.createOrReplaceTempView("pointDf")
            pointDf = sparkSession.sql("SELECT ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:32650') AS point"
              + ", from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date FROM pointDf"
              + s" WHERE sysTime > UNIX_TIMESTAMP('${current.toLocalDate.toString()}','yyyy-MM-dd') * 1000 AND sysTime < UNIX_TIMESTAMP('${plusOne.toLocalDate.toString()}','yyyy-MM-dd') * 1000")
            pointDf.createOrReplaceTempView("pointDf")
            pointDf.show(false)
            val polygonRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, polygonInputLocation)
            var polygonDf = Adapter.toDf(polygonRDD, sparkSession)
            polygonDf.createOrReplaceTempView("polygonDf")
            polygonDf = sparkSession.sql("SELECT OID_, ST_GeomFromWKT(geometry) AS geom FROM polygonDf")
            polygonDf.createOrReplaceTempView("polygonDf")
            polygonDf.show(false)
            val distJoin = sparkSession.sql("SELECT OID_, COUNT(*) as Join_Count FROM pointDf, polygonDf WHERE ST_Within(point, geom) GROUP BY OID_")
            distJoin.createOrReplaceTempView("distJoinDf")
            distJoin.write.option("header", "true").mode("overwrite").csv(outputPath + "/" + current.toLocalDate.toString)
        }
    }

    def fishnet2(): Unit = {
        var pointDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(pointInputLocation)
        pointDf.createOrReplaceTempView("pointDf")
        pointDf = sparkSession.sql("SELECT ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), 'epsg:4326', 'epsg:32650') AS point"
          + ", from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date FROM pointDf"
          + s" WHERE sysTime > UNIX_TIMESTAMP('$fromTime') * 1000 AND sysTime < UNIX_TIMESTAMP('$toTime') * 1000")
        pointDf.createOrReplaceTempView("pointDf")
        pointDf.show(false)
        val pointRDD = Adapter.toSpatialRdd(sparkSession.sql(s"select point from pointDf"), "point")
        pointRDD.analyze()
        pointRDD.spatialPartitioning(joinQueryPartitioningType)
        pointRDD.buildIndex(pointIndexType, true)
        val polygonRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, polygonInputLocation)
        polygonRDD.analyze()
        polygonRDD.spatialPartitioning(pointRDD.getPartitioner)
        val result = JoinQuery.SpatialJoinQuery(pointRDD, polygonRDD, true, false)
        val x = result.rdd.mapValues(x => x.size()).collect()
        val y = 1
        //        result.rdd.mapValues(x=> x.size()).saveAsTextFile(outputPath + analysis + "Result")
    }


    def writeToFile(result: JavaPairRDD[_, _], analysis: String): Unit = {
        outputMode match {
            case 1 =>
                val pw = new PrintWriter(outputPath + analysis + "Result")
                pw.write(result.count().toString)
                pw.close()
            case 2 =>
                result.saveAsTextFile(outputPath + analysis + "_result")
            case 3 =>
                result.sortBy(x => x._2.asInstanceOf[util.HashSet[_]].size() / x._1.asInstanceOf[Geometry].getArea, ascending = false).saveAsTextFile(outputPath + analysis + "_result")
            case 4 =>
                val x = result.sortBy(x => x._2.asInstanceOf[util.HashSet[_]].size() / x._1.asInstanceOf[Geometry].getArea, ascending = false).take(firstX)
                val pw = new PrintWriter(outputPath + analysis + "Result")
                for (i <- x) {
                    pw.write(i._1.toString + "," + i._2.asInstanceOf[util.HashSet[_]].size() + "," + i._2.asInstanceOf[util.HashSet[_]].size() / i._1.asInstanceOf[Geometry].getArea + '\n')
                }
                pw.close()
            case 5 =>
                val x = result.sortBy(x => x._2.asInstanceOf[util.HashSet[_]].size(), ascending = false).take(firstX)
                val pw = new PrintWriter(outputPath + analysis + "Result")
                for (i <- x) {
                    pw.write(i._1.toString + "," + i._2.asInstanceOf[util.HashSet[_]].size() + '\n')
                }
                pw.close()
            case 6 =>
                val x = print(result.count())
        }
    }

    def linestringBuffer(): Unit = {
        val lineStringRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, lineStringInputLocation)
        var rawSpatialDf = Adapter.toDf(lineStringRDD, sparkSession)
        rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
        val query = "SELECT ST_GeomFromWKT(geometry) AS geom | FROM rawSpatialDf"
        rawSpatialDf = sparkSession.sql(query)
        rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
        val spatialDf = sparkSession.sql(s"SELECT ST_Buffer(geom, $radius) AS buff, geom FROM rawSpatialDf")
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf.rdd.sortBy(_.toString()).saveAsTextFile(outputPath + analysis + "Result")
    }

    def pointBuffer(filename: String, lonOffset: Int, latOffset: Int): Unit = {
        var spatialDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(filename)
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf = sparkSession.sql(s"select ST_Point(cast(spatialDf._c$latOffset as Decimal(24, 18)), cast(spatialDf._c$lonOffset as Decimal(24, 18))) as geometry, spatialDf._c2 as name from spatialDf")
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf = sparkSession.sql(s"SELECT ST_Transform(geometry, 'epsg:4326','epsg:3857') AS buff, geometry, name FROM spatialDf")
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf.rdd.sortBy(_.toString()).saveAsTextFile(outputPath + analysis + "Result")
    }

    def filter(filename: String): DataFrame = {
        var spatialDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(filename)
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf = sparkSession.sql("SELECT *, from_unixtime(_c5/1000,'yyyy-MM-dd HH:mm:ss') AS date FROM spatialDf")
        spatialDf.createOrReplaceTempView("spatialDf")
        spatialDf.show(false)
        spatialDf = sparkSession.sql("SELECT * FROM spatialDf WHERE MONTH(date) IN (12, 1, 2)")
        spatialDf.show(false)
        spatialDf
    }


}