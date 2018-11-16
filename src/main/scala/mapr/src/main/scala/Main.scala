import com.uhg.mapr.context.Context
object Main extends Context with App {

  println(config.getString("conf.spark.master"))

  val data = Map(
    "key1" -> 3.0,
    "key2" -> 4.0,
    "key3" -> 3.5)

  data foreach ((movie) => println(movie._1 + "," + movie._2.toString))

}