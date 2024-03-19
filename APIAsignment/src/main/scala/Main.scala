import com.sun.org.apache.xalan.internal.lib.ExsltDatetime.time
import play.api.libs.json.JsPath.\
import sttp.client4.quick._
import sttp.client4.Response
import play.api.libs.json._
import play.api.libs.json.{JsValue, Json, _}

import scala.collection.immutable.Nil.foreach
import scala.collection.immutable.Seq
import scala.util.Success
object Main {
  val header: String = "Bearer BQDopcQPtpQ12ZR7JytnCF8uoki7Ov-ZARaNCggST6bZgtg9-4lQL0yXAQVNWOG7E_g03xSFCcRbuODw_WVUrCwNyoyCTliwo_pDRcNtKCkk6hgRZbXJJRhVP9TA5LKY9JWb3MIkVvnB-fzPLspUVJ0_pB4-wwpnTk79PzwVNyg9UQzEGv1bhONRl-G3T9YIS3IIhX1UlpNar1gVYfw"
  val root_url = "https://api.spotify.com/v1/playlists/5Rrf7mqN8uus2AaQQQNdc1"
  var data_save: Array[Map[String, Any]] = Array()

  def main(args: Array[String]): Unit = {
    process(root_url)
//    data_save.foreach(println)
    println(data_save.length)

    val topTracks = data_save.sortBy(-_("duration_ms").asInstanceOf[Number].longValue).take(10)
    var nametot: List[Tuple2[Int,String]] = List()
    println("question 1")
    topTracks.foreach { track =>
      println(s"${track("name")}, ${track("duration_ms")}")
//      println(track("artists"))
    }
    println("\nquestion 2")
    topTracks.foreach { track =>
      track("artists") match {
        case list: List[_] => list.foreach(li => {
          var (tot,name) = people(li.toString)
          Thread.sleep(200)
          nametot :+= Tuple2(tot,name)
//          println(s"${name}, ${tot}")
        })
      }
    }
    val sortedNametot = nametot.sortWith((a, b) => a._1 > b._1).take(10)
    val distinctNametot = sortedNametot.distinctBy(_._2)
    distinctNametot.foreach(tuple => println(tuple._2, tuple._1))


  }

  def process(uri: String): Unit = {
    val response: Response[String] = quickRequest
      .header("Authorization", header)
      .get(uri"${uri}")
      .send()
    val json: JsValue = Json.parse(response.body.toString)
    val trackOpt: Option[JsValue] = (json \ "tracks").asOpt[JsValue]
    var items: Seq[JsValue] = Seq()
    if (trackOpt.isDefined) {
      items= (json \ "tracks" \ "items").as[Seq[JsValue]]
    } else {
      items= (json \ "items").as[Seq[JsValue]]
    }

    var trackDetails: Array[Map[String, Any]] = Array()

    items.foreach { item =>
      val track = (item \ "track").as[JsValue]
      val name: String = (track \ "name").as[String]
      val artistHrefs: Seq[String] = (track \ "artists").as[Seq[JsValue]].map { artist =>
        (artist \ "id").as[String]}
      val durationMs: Int = (track \ "duration_ms").as[Int]
//      trackDetails :+= Map("name" -> name, "duration_ms" -> durationMs)
      data_save :+= Map("name" -> name, "duration_ms" -> durationMs, "artists" -> artistHrefs)
    }


    val tracksNext: Option[String] = (json \ "tracks" \ "next").asOpt[String]
    val next: Option[String] = (json \ "next").asOpt[String]

    val st: Option[String] = tracksNext.orElse(next)

    st.foreach(process)


  }
  def people(id:String) = {
    val s: String = "https://api.spotify.com/v1/artists/"+id
    val response: Response[String] = quickRequest
      .header("Authorization", header)
      .get(uri"${s}")
      .send()
      val json:JsValue = Json.parse(response.body.toString)

    val totalFollowers: Option[Int] = (json \ "followers" \ "total").asOpt[Int]
    val name: Option[String] = (json \ "name").asOpt[String]
   (totalFollowers.get,name.get)



  }

}
