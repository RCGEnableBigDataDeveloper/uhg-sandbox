import com.uhg.mapr.context.Context
object Main extends Context with App {

  println(config.getString("conf.spark.master"))
  
}