package part2_event_sourcing

import java.util.Date

import akka.actor.{ActorLogging, ActorSystem, PoisonPill, Props}
import akka.persistence.PersistentActor

object PersistentActors extends App {
  /*
    Scenario: we have a business and accountant which keeps track of our invoices
   */

  // COMMAND: we send to persistent actor
  case class Invoice(recipient: String, date: Date, amount: Int)
  case class InvoiceBulk(invoices: List[Invoice])

  // Special messages
  case object Shutdown

  // EVENT: data structures that persistent actor sends to persistent store/journal
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant extends PersistentActor with ActorLogging {
    var latestInvoiceId = 0
    var totalAmount = 0

    override def persistenceId: String = "simple-accountant" // best practice: make it unique
    /**
      * Normal receive method
      * @return
      */
    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>
        /*
          when you receive a command
           1. create an Event to persist into the store
           2. you persist the event, pass in a callback which will get triggered once the event is written
           3. update the actor state when the event has persisted
         */
        log.info(s"Receive invoice for amount ${amount}")
        val event = InvoiceRecorded(latestInvoiceId, recipient, date, amount)
        // persist is async
        persist(event) /*time gap: all other messages sent to this actor are STASHED */
        { e =>
          // safe to access mutable state here
          // update state
          latestInvoiceId += 1
          totalAmount += amount

          // correctly identify the sender of the COMMAND
          // sender() ! "PersistentACK"
          log.info(s"Persisted $e as invoice ${e.id}, for total amount ${totalAmount}")
        }
        // act like a normal actor - not compulsory to persist
      case InvoiceBulk(invoices) =>
        /*
          1. create events
          2. persist all events
          3. update the actor state when each event is persisted
         */
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoiceIds).map {pair =>
          val id = pair._2
          val invoice = pair._1

          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)
        }

        persistAll(events) { e =>
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Persisted SINGLE $e as invoice ${e.id}, for total amount ${totalAmount}")
        }
      case Shutdown =>
        context.stop(self)

      case "print" =>
        log.info(s"Latest invoice id: ${latestInvoiceId}, total amount: ${totalAmount}")
    }

    /**
      * Handler that is called on recovery
      * @return
      */
    override def receiveRecover: Receive = {
      /*
      best practice: follow the logic in the persist steps of receiveCommand
       */
      case InvoiceRecorded(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
        log.info(s"Recovered invoice $id for amount $amount, total amount: $totalAmount")
    }

    /*
      persist -> journal tried to save event in db -> if works replied with event
      This method is called if persisting failed (we don;t know if journal
      persisted event or not so state is corrupted)
      The actor will be STOPPED irrespective of supervision strategy

      Best practice: start the actor again after a while
      Use BACKOFF SUPERVISOR
     */
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to Persist $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    /*
      Called if Journal fails to persist the event
      The actor is RESUMED because we know for sure that event was not persisted
     */
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for $event because of $cause")
      super.onPersistRejected(cause, event, seqNr)
    }
  }

  val system = ActorSystem("PersistentActor")
  val accountant = system.actorOf(Props[Accountant], "simpleAccountant")

  /**
    * If we comment below code in 2nd run, there will be 10 records in persistent db
    * Akka will query db and will run receiveRecover with every message it retrieves
    */
    for(i <- 1 to 10) {
    accountant ! Invoice("The Sofa Company", new Date(), i * 1000)
  }

  /*
    Persistent Failures:
      1. Persist fails
      2. Persist Rejected
   */

  /**
    * Persisting multiple events
    *
    * use persistAll
    */
//  val newInvoices = for (i <- 1 to 5) yield Invoice("Awesome Chairs", new Date, i * 2000)
//  accountant ! InvoiceBulk(newInvoices.toList)

  /*
    NEVER EVER CALL PERSIST OR PERSISTALL FROM FUTURES
    Futures run in different thread so there might be 2 threads persisting
    so this will corrupt
   */

  /**
    *  Shutdown of persistent actors
    *  DON'T USE POISONPILL OR KILL FOR KILLING PERSISTENT ACTOR
    *
    *  Best Practice: define your own shutdown message
    */
  // accountant ! PoisonPill (different mailbox for handling PoisonPill and Kill)
  accountant ! Shutdown
}
