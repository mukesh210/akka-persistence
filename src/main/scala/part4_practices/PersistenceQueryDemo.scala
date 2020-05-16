package part4_practices

import akka.NotUsed
import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.{EventEnvelope, Offset, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Random

object PersistenceQueryDemo extends App {

  val system = ActorSystem("PersistenceQueryDemo", ConfigFactory.load().getConfig("persistenceQuery"))

  // read journal
  val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // give me all persistence IDs
  val persistenceIds: Source[String, NotUsed] = readJournal.persistenceIds()

  implicit val materializer = ActorMaterializer()(system)
//  persistenceIds.runForeach { persistentId =>
//    println(s"Found persistent Id: ${persistentId}")
//  }

  class SimplePersistentActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "persistent-query-id-1"

    override def receiveCommand: Receive = {
      case m => persist(m) { _ =>
        log.info(s"Persisted: ${m}")
      }
    }

    override def receiveRecover: Receive = {
      case e =>
        log.info(s"Recovered: ${e}")
    }
  }

   val simplePersistentActor = system.actorOf(Props[SimplePersistentActor], "simplePersistentActor")
  import system.dispatcher
  system.scheduler.scheduleOnce(5 seconds) {
    simplePersistentActor ! "hello a second time"
  }

  // events by persistence ID
  val events: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("persistent-query-id-1", 0, Long.MaxValue)

  events.runForeach { event =>
    println(s"Read event: ${event}")
  }

  // events by tags
  val genres = Array("pop", "rock", "hip-hop", "jazz", "disco")
  case class Song(artist: String, title: String, genre: String)
  // COMMAND
  case class Playlist(songs: List[Song])
  // event
  case class PlaylistPurchased(id: Int, songs: List[Song])

  class MusicStoreCheckoutActor extends PersistentActor with ActorLogging {
    var latestPlaylistId = 0
    override def persistenceId: String = "music-store-checkout"

    override def receiveCommand: Receive = {
      case Playlist(songs) =>
        persist(PlaylistPurchased(latestPlaylistId, songs)) { e =>
          log.info(s"User purchased: $songs")
          latestPlaylistId += 1
        }
    }

    override def receiveRecover: Receive = {
      case event @ PlaylistPurchased(id, songs) =>
        log.info(s"Recovered: $event")
        latestPlaylistId = id
    }
  }

  class MusicStoreEventAdapter extends WriteEventAdapter {
    override def manifest(event: Any): String = "musicStore"

    override def toJournal(event: Any): Any = event match {
      case event @ PlaylistPurchased(_, songs) =>
        val genres = songs.map(_.genre).toSet
        Tagged(event, genres)

      case event => event
    }
  }

  val musicStoreCheckoutActor = system.actorOf(Props[MusicStoreCheckoutActor], "musicStoreCheckoutActor")

  val r = new Random

  for (_ <- 1 to 10) {
    val maxSongs = r.nextInt(5)
    val songs = for(i <- 1 to maxSongs) yield {
      val randomGenre = genres(r.nextInt(5))
      Song(s"Artist ${i}", s"My love songs ${i}", randomGenre)
    }

    musicStoreCheckoutActor ! Playlist(songs.toList)
  }

  // searches across persistent id
  val rockPlaylists: Source[EventEnvelope, NotUsed] = readJournal.eventsByTag("rock", Offset.noOffset)
  rockPlaylists.runForeach { event =>
      println(s"Found playlist with rock song: ${event}")
  }

}