package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, Recovery, RecoveryCompleted, SnapshotSelectionCriteria}

object RecoveryDemo extends App {

  case class Command(contents: String)
  case class Event(id: Int, contents: String)

  class RecoveryActor extends PersistentActor with ActorLogging {

    override def persistenceId: String = "recovery-actor"

    override def receiveCommand: Receive = online(0)

    def online(latestPersistedEventId: Int): Receive = {
      case Command(contents) =>
        persist(Event(latestPersistedEventId, contents)) { event =>
          log.info(s"Successfully persisted: $event, recovery is: ${if(this.recoveryFinished) "" else "NOT"} finished")
          context.become(online(latestPersistedEventId + 1))
        }
    }

    // does not take any parameter
    override def receiveRecover: Receive = {
      case Event(id, contents) =>
//        if(contents.contains("314"))
//          throw new RuntimeException("I can't take this anymore")
        log.info(s"Recovered: $contents, recovery is: ${if(this.recoveryFinished) "" else "NOT"} finished")
        context.become(online(id + 1))
        /*
          this WILL NOT change the event's handler during Recovery
          After Recovery, the "normal" handler will be the result of ALL the stacking of context.become
         */

      case RecoveryCompleted =>
        // additional Initialization
        log.info("I have finished recovery")
    }

    override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      log.error("I failed at recovery")
      super.onRecoveryFailure(cause, event)
    }

    // override def recovery: Recovery = Recovery(toSequenceNr = 100)
    // override def recovery: Recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria.Latest)
    // override def recovery: Recovery = Recovery.none // recovery will not take place
  }

  val system = ActorSystem("ReceoveryDemo")
  val recoveryActor = system.actorOf(Props[RecoveryActor], "recoveryActor")
  /*
    Stashing of commands
   */
//  for ( i <- 1 to 1000) {
//    recoveryActor ! Command(s"command $i")
//  }

  /**
    * ALL COMMANDS SENT DURING RECOVERY ARE STASHED
    * so, already present Events will be recovered
    * only after that current sent messages will be processed
    */

  /*
    2- Failure during recovery
      - onRecoveryFailure: the actor is STOPPED because if there is failure
          during recovery, State is not correct
          Read EventSourced.scala/recoveryBehavior
   */

  /*
    3 - customizing recovery: For recovering only upto certain number
    - DO NOT persist more events after a customized _incomplete_ recovery
    - override "def recovery" method to provide a config for recovering
   */

  /*
    4 - Recovery status or knowing when you are done recovering
    USE "this.recoveryFinished"
      - getting signal when you are done recovering
   */

  /*
    5 - stateless actors: Use context.become
   */
  recoveryActor ! Command("special command 1")
  recoveryActor ! Command("special command 2")
}
