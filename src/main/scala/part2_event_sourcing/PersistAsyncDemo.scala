package part2_event_sourcing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

/**
  * persist is async, so when we persist some data(send message to Journal to store)
  * there is some TIME GAP between "when persist is sent to Journal" and "callback being called",
  * in this TIME GAP, all the messages received by persistent actor is stashed
  *
  * persistAsync: in TIME GAP mentioned above, messages are not stashed.... they are processed
  *
  * persistAsync: relaxes the guarantee that no message will be processed in above TIME GAP
  *
  * observed output by changing "persist" with "persistAsync"
  */
object PersistAsyncDemo extends App {

  case class Command(contents: String)
  case class Event(contents: String)

  object CriticalStreamProcessor {
    def props(eventAggregator: ActorRef): Props = Props(new CriticalStreamProcessor(eventAggregator))
  }

  class CriticalStreamProcessor(eventAggregator: ActorRef) extends PersistentActor with ActorLogging {
    override def persistenceId: String = "critical-stream-processor"

    override def receiveCommand: Receive = {
      case Command(contents) =>
        eventAggregator ! s"Processing $contents"
        persistAsync(Event(contents)) /*       TIME GAP       */ { e =>
          eventAggregator ! e
        }

        // some actual computation
        val processedContents = contents + "_processed"
        persistAsync(Event(processedContents)) /*       TIME GAP       */  { e =>
          eventAggregator ! e
        }
    }

    override def receiveRecover: Receive = {
      case message =>
        log.info(s"Recovered: $message")
    }
  }

  class EventAggregator extends Actor with ActorLogging {
    override def receive: Receive = {
      case message =>
        log.info(s"$message")
    }
  }

  val system = ActorSystem("persistAsyncDemo")
  val eventAggregator = system.actorOf(Props[EventAggregator], "eventAggregator")
  val streamProcessor = system.actorOf(CriticalStreamProcessor.props(eventAggregator), "streamProcessor")

  streamProcessor ! Command("command1")
  streamProcessor ! Command("command2")

}

/**
  * when to use persist/persistAsync?
  *
  * - persistAsync            vs        persist
  * - perf(high-throughput)
  * - no ordering guarantee             ordering guarantee
  *
  */
