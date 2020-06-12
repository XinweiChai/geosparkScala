import java.awt.Color
import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties

import com.vividsolutions.jts.geom.Envelope
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.ImageGenerator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparkviz.extension.visualizationEffect.{HeatMap, ScatterPlot}
import org.datasyslab.geosparkviz.utils.ImageType

object Plot extends App {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      //      .config("spark.debug.maxToStringFields", "100")
//      .master("local[*]")
      .appName("Plot").getOrCreate()
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    //    val sparkConf = new SparkConf().setAppName("Plot")
    //      .set("spark.serializer", classOf[KryoSerializer].getName)
    //      .set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
    //        .setMaster("local[*]")
    //    val sparkContext = new SparkContext(sparkConf)
    val sparkContext = sparkSession.sparkContext

    val prop = new Properties()
    val resourcePath = System.getProperty("user.dir") + "/"
    val configPath = System.getProperty("user.dir") + "/conf/"

    var ConfFile = new FileInputStream(configPath + "plot.point.properties")
    prop.load(ConfFile)
    val pointInputLocation = resourcePath + prop.getProperty("inputLocation")
    val pointLatOffset = prop.getProperty("latOffset").toInt
    val pointLonOffset = prop.getProperty("lonOffset").toInt
    val pointOffset = prop.getProperty("offset").toInt
    val pointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    val pointNumPartitions = prop.getProperty("numPartitions").toInt
    val pointOutputPath = resourcePath + prop.getProperty("outputLocation")
    val reversedLatLon = prop.getProperty("reversedLatLon").toBoolean
    val epsg3857 = prop.getProperty("epsg3857").toBoolean

    ConfFile = new FileInputStream(configPath + "plot.linestring.properties")
    prop.load(ConfFile)
    val lineStringInputLocation = resourcePath + prop.getProperty("inputLocation")
    val lineStringOutputLocation = resourcePath + prop.getProperty("outputLocation")

    ConfFile = new FileInputStream(configPath + "plot.run.properties")
    prop.load(ConfFile)
    val storageLevel = StorageLevel.fromString(prop.getProperty("storageLevel"))
    val minZoom = prop.getProperty("minZoom").toInt
    val maxZoom = prop.getProperty("maxZoom").toInt
    val cache = prop.getProperty("cache").toBoolean
    val plot = prop.getProperty("plot")
    val onlyBeijing = prop.getProperty("onlyBeijing").toBoolean
    println("point input: " + pointInputLocation)
    println("linestring input: " + lineStringInputLocation)
    println("point output: " + pointOutputPath)
    println("linestring output: " + lineStringOutputLocation)
    plot match {
        case "points" =>
            parallelRenderPointsBigData()
        case "linestrings" =>
            parallelPlotLinestring()
    }
    System.out.println("All GeoSparkViz tasks have passed.")
    sparkContext.stop()


    def bound3857(p1: Mercator, p2: Mercator): (Mercator, Mercator) = {
        val (t1, t2) = (p1.toTile, p2.toTile)
        t2._x += 1
        t2._y += 1
        (t1.toMercator, t2.toMercator)
    }


    def using[A <: {def close(): Unit}, B](resource: A)(f: A => B): B =
        try {
            f(resource)
        } finally {
            resource.close()
        }

    def time[R](block: => R): Unit = {
        val t0 = System.nanoTime()
        val result = block // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / 1000000000 + "s")
    }


    def parallelRenderPointsBigData(): Unit = {
        val spatialRDD = Colocation.readWithDF(pointInputLocation, pointLonOffset, pointLatOffset, sparkSession, epsg3857, gcjToWGS = true)
        if (cache) {
            spatialRDD.rawSpatialRDD.cache()
        }
        var env = spatialRDD.boundaryEnvelope
        if (onlyBeijing) {
            env = new Envelope(1.2900283340426097E7, 1.3017686759110853E7, 4807455.730608667, 4915931.87752231)
        }
        val (x1, x2, y2, y1) = (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
        // Tile numbering under Mercator projection
        val pw = new PrintWriter(new File(pointOutputPath + "TileOffset"))
        for (zoom <- 0 to maxZoom) {
            val tb = (Mercator(x1, y1, zoom).toTile, Mercator(x2, y2, zoom).toTile)
            pw.write(tb._1.z + "," + tb._1._x + "," + tb._1._y + "\n")
        }
        pw.close()

        for (zoom <- minZoom to maxZoom) {
            time {
                val boundary = bound3857(Mercator(x1, y1, zoom), Mercator(x2, y2, zoom))
                val BeijingBoundary = new Envelope(boundary._1.x, boundary._2.x, boundary._1.y, boundary._2.y)
                val tb = (Mercator(x1, y1, zoom).toTile, Mercator(x2, y2, zoom).toTile)
                println(zoom + " " + BeijingBoundary + " " + tb)
                val scaleX = tb._2._x - tb._1._x + 1
                val scaleY = tb._2._y - tb._1._y + 1
                val visualizationOperator = new HeatMap(256 * scaleX, 256 * scaleY, BeijingBoundary, false, 1, scaleX, scaleY, true, true)
                // If blurRadius is set above 0, outOfBoundary problem will arise
                //                var maxPixelCount = pow(2, 17 - zoom)
                //                if (maxPixelCount < 10.0) {
                val maxPixelCount = 2.0
                //                }
                visualizationOperator.setMaxPixelCount(maxPixelCount)
                visualizationOperator.Visualize(sparkContext, spatialRDD)
                val imageGenerator = new ImageGenerator
                imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, pointOutputPath + "tile", ImageType.PNG, zoom, scaleX, scaleY)
            }
        }
    }

    def parallelPlotLinestring(): Unit = {
        var spatialRDD = ShapefileReader.readToGeometryRDD(sparkContext, lineStringInputLocation)
        spatialRDD.analyze()
        var env = spatialRDD.boundaryEnvelope
        if (onlyBeijing) {
            var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
            spatialDf.createOrReplaceTempView("spatialDf")
            val query =
                """
                  | SELECT ST_GeomFromWKT(geometry)
                  | FROM spatialDf
                  | WHERE ST_CONTAINS(ST_PolygonFromEnvelope(1.2900283340426097E7, 4807455.730608667, 1.3017686759110853E7, 4915931.87752231), ST_GeomFromWKT(geometry))
                                     """.stripMargin
            spatialDf = sparkSession.sql(query)
            spatialDf.createOrReplaceTempView("spatialDf")
            spatialRDD = Adapter.toSpatialRdd(spatialDf, "geometry")
            env = new Envelope(1.2900283340426097E7, 1.3017686759110853E7, 4807455.730608667, 4915931.87752231)
        }
        val (x1, x2, y2, y1) = (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
        val pw = new PrintWriter(new File(lineStringOutputLocation + "TileOffset"))
        for (zoom <- 0 to maxZoom) {
            val tb = (Mercator(x1, y1, zoom).toTile, Mercator(x2, y2, zoom).toTile)
            pw.write(tb._1.z + "," + tb._1._x + "," + tb._1._y + "\n")
        }
        pw.close()

        for (zoom <- minZoom to maxZoom) {
            time {
                val boundary = bound3857(Mercator(x1, y1, zoom), Mercator(x2, y2, zoom))
                val BeijingBoundary = new Envelope(boundary._1.x, boundary._2.x, boundary._1.y, boundary._2.y)
                val tb = (Mercator(x1, y1, zoom).toTile, Mercator(x2, y2, zoom).toTile)
                println(zoom + " " + BeijingBoundary + " " + tb)
                val scaleX = tb._2._x - tb._1._x + 1
                val scaleY = tb._2._y - tb._1._y + 1
                val visualizationOperator = new ScatterPlot(256 * scaleX, 256 * scaleY, BeijingBoundary, false, scaleX, scaleY, true)
                visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
                visualizationOperator.Visualize(sparkContext, spatialRDD)
                val imageGenerator = new ImageGenerator
                imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, lineStringOutputLocation + "tile", ImageType.PNG, zoom, scaleX, scaleY)
            }
        }
    }

}