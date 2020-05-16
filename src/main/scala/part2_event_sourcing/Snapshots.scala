package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

import scala.collection.mutable

object Snapshots extends App {

  // COMMANDS
  case class ReceivedMessage(contents: String) // message from your contact
  case class SentMessage(contents: String)  // message TO your contact


  //EVENT
  case class ReceivedMessageRecord(id: Int, contents: String)
  case class SentMessageRecordId(id: Int, contents: String)

  object Chat {
    def props(owner: String, contact: String): Props =
      Props(new Chat(owner, contact))
  }
  class Chat(owner: String, contact: String) extends PersistentActor with ActorLogging {
    val MAX_MESSAGES = 10

    var commandsWithoutCheckpoint = 0
    var currentMessageId: Int = 0
    val lastMessages = new mutable.Queue[(String, String)]()  // (originator, content)

    override def persistenceId: String = s"$owner-$contact-chat"

    override def receiveCommand: Receive = {
      case ReceivedMessage(contents) =>
        persist(ReceivedMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Received message: $contents")
          mayBeReplaceMessage(contact, contents)
          currentMessageId += 1
          mayBeCheckPoint()
        }
      case SentMessage(contents) =>
        persist(SentMessageRecordId(currentMessageId, contents)) { e =>
          log.info(s"Sent Message: $contents")
          mayBeReplaceMessage(owner, contents)
          currentMessageId += 1
          mayBeCheckPoint()
        }
      case "print" =>
        log.info(s"Most recent message: $lastMessages")
        // snapshot related messages
      case SaveSnapshotSuccess(metadata) =>
        log.info(s"Saving snapshot succedded: $metadata")
      case SaveSnapshotFailure(metadata, throwable) =>
        log.info(s"Saving snapshot ${metadata} failed due to ${throwable}")
    }

    override def receiveRecover: Receive = {
      case ReceivedMessageRecord(id, contents) =>
        log.info(s"Recovered received message $id: $contents")
        mayBeReplaceMessage(contact, contents)
        currentMessageId = id
      case SentMessageRecordId(id, contents) =>
        log.info(s"Recovered sent message $id: $contents")
        mayBeReplaceMessage(owner, contents)
        currentMessageId = id
      case SnapshotOffer(metadata, contents) =>
        log.info(s"Recovered snapshot: $metadata")
        contents.asInstanceOf[mutable.Queue[(String, String)]].foreach(lastMessages.enqueue(_))
    }

    def mayBeReplaceMessage(sender: String, contents: String) = {
      if(lastMessages.size >= MAX_MESSAGES) {
        lastMessages.dequeue()
      }
      lastMessages.enqueue((sender, contents))
    }

    def mayBeCheckPoint(): Unit = {
      commandsWithoutCheckpoint += 1
      if(commandsWithoutCheckpoint >= MAX_MESSAGES) {
        log.info("Saving Checkpoint...")
        saveSnapshot(lastMessages)  // async operation
        commandsWithoutCheckpoint = 0
      }
    }
  }
  val system = ActorSystem("SnapshotsDemo")
  val chat = system.actorOf(Chat.props("danile123", "martin345"))

//  for(i <- 1 to 100000) {
//    chat ! ReceivedMessage(s"Akka Rocks $i")
//    chat ! SentMessage(s"Akka Rules $i")
//  }

  /**
    * Recovering millions of messages take lots of time... so Snapshots
    */
  chat ! "print"

  /*
    event 1
    event 2
    event 3
    snapshot 1
    event 4
    snapshot 2
    event 5
    event 6

    If we recover above message, only "snapshot 2, event 5 and event 6" will be recovered
   */

  /*
    pattern:
      - after each persist, maybe save a snapshot(logic is upto us)
      - if you save a snapshot, handle the SnapShotOffer message in receiveRecover
      - (optional) handle SaveSnapshotSuccess and SaveSnapshotFailure in "receiveCommand"
   */


}
