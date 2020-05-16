package part2_event_sourcing

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

/**
  *  Use PersistAll for persisting multiple events of same type
  *
  *  For events of different type, use Persist multiple times.
  *  Persisting event is similar to sending message to actor, so
  *  Persist happens in same order as mentioned
  *  Similarly, callback is called in same order as mentioned
  */
object MultiplePersist extends App {

  /*
    Diligent accountant: with every invoice, will persist 2 events
      - a tax record for the fiscal authority
      - an invoice record for personal logs or some auditing authority
   */

  // COMMAND
  case class Invoice(recipient: String, date: Date, amount: Int)

  // EVENT
  case class TaxRecord(taxId: String, recordId: Int, date: Date, totalAmount: Int)
  case class InvoiceRecord(invoiceRecordId: Int, recipient: String, date: Date, amount: Int)

  object DiligentAccountant {
    def props(taxId: String, taxAuthority: ActorRef): Props =
      Props(new DiligentAccountant(taxId, taxAuthority))
  }

  class DiligentAccountant(taxId: String, taxAuthority: ActorRef) extends PersistentActor with ActorLogging {
    var latestTaxRecordId = 0
    var latestInvoiceRecordId = 0

    override def persistenceId: String = "diligent-accountant"

    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>
        // journal ! TaxRecord
        persist(TaxRecord(taxId, latestTaxRecordId, new Date(), amount / 3)) { record =>
          taxAuthority ! record
          latestTaxRecordId += 1

          persist("I hereby declare this taxRecord to be true and Complete.") { declaration =>
            taxAuthority ! declaration
          }
        }

        // journal ! InvoiceRecord
        persist(InvoiceRecord(latestInvoiceRecordId, recipient, date, amount)) { invoiceRecord =>
          taxAuthority ! invoiceRecord
          latestInvoiceRecordId += 1

          persist("I hereby declare this invoiceRecord to be true and Complete.") { declaration =>
            taxAuthority ! declaration
          }
        }
    }

    override def receiveRecover: Receive = {
      case event => log.info(s"Recovered $event")
    }
  }

  class TaxAuthority extends Actor with ActorLogging {
    override def receive: Receive = {
      case message =>
        log.info(s"Received: $message")
    }
  }

  val system = ActorSystem("MultiplePersistDemo")
  val taxAuthority = system.actorOf(Props[TaxAuthority], "HMRC")
  val accountant = system.actorOf(DiligentAccountant.props("UK3234534_3454", taxAuthority))

  accountant ! Invoice("The Sofa Company", new Date(), 2000)
  /*
    The message ordering(TaxRecord -> InvoiceRecord) is GUARENTEED
   */
  /**
    *  PERSISTENT IS ALSO BASED ON MESSAGE PASSING
    */

  // nested persisting

  accountant ! Invoice("The Supercar Company", new Date(), 2000345645)
}
