import scala.math._

case class Tile(var _x: Int, var _y: Int, var z: Int) {
  def toLatLon = LatLonPoint(
    toDegrees(atan(sinh(Pi * (1.0 - 2.0 * _y.toDouble / (1 << z))))),
    _x.toDouble / (1 << z) * 360.0 - 180.0,
    z)

  def toURI = new java.net.URI("https://tile.openstreetmap.org/" + z + "/" + _x + "/" + _y + ".png")

  def toMercator: Mercator = {
    val equator = 40075016.68557849
    val x = _x.toDouble * equator / (1 << z) - equator / 2.0
    val y = equator / 2.0 - _y.toDouble * equator / (1 << z)
    Mercator(x, y, z)
  }
}

case class LatLonPoint(lat: Double, lon: Double, z: Int = 18) {
  def toTile = Tile(
    ((lon + 180.0) / 360.0 * (1 << z)).toInt,
    ((1 - log(tan(toRadians(lat)) + 1 / cos(toRadians(lat))) / Pi) / 2.0 * (1 << z)).toInt,
    z)
}

case class Mercator(x: Double, y: Double, z: Int) {
  def toTile: Tile = {
    val equator = 40075016.68557849
    val pixelX = ((x + equator / 2.0) / equator * (1 << z)).toInt
    val pixelY = ((equator / 2.0 - y) / equator * (1 << z)).toInt
    Tile(pixelX, pixelY, z)
  }
}

