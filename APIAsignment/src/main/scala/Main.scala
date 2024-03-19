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
  private var cachedToken: Option[String] = None
  val header: String = getToken()
  val root_url = "https://api.spotify.com/v1/playlists/5Rrf7mqN8uus2AaQQQNdc1"
  var data_save: Array[Map[String, Any]] = Array()

  def main(args: Array[String]): Unit = {
    val header: String = getToken()
    process(root_url,header:String)
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
          var (tot,name) = people(li.toString,header)
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

  def process(uri: String,header:String): Unit = {
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

    st foreach(i => process(i,header))


  }
  def people(id:String,header:String) = {
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

  case class TokenResponse(access_token: String, token_type: String, expires_in: Int)

  def getToken(client_id: String = "06f17073266c4c2d965fc2e1bb48a556", client_secret: String = "6ada7986cd564f77ba185c02ad37f8d4"): String = {
    val uri = uri"https://accounts.spotify.com/api/token"

    val requestBody = Map(
      "client_id" -> client_id,
      "client_secret" -> client_secret,
      "grant_type" -> "client_credentials"
    )

    val request = basicRequest
      .post(uri)
      .header("Content-Type", "application/x-www-form-urlencoded")
      .body(requestBody)

    val response = request.send()
    val tokenResponse = response.body match {
      case Right(body) => body
      case Left(error) => throw new RuntimeException("Failed to get response: " + error)
    }
    println(tokenResponse)

    val json: JsValue = Json.parse(tokenResponse)
    (json \ "token_type").as[String] + " " + (json \ "access_token").as[String]
  }
}
