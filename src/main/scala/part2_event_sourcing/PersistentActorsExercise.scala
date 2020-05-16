package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PersistentActorsExercise extends App {

  /*
    Persistent Actor for a voting station
    Keep:
      - the citizens who voted
      - the poll: mapping between a candidate and the number of received votes so far

      The Actor must be able to recover its state if it's shut down or restarted
   */

  // COMMAND
  case class Vote(citizenPID: String, candidate: String)

  // EVENT


  class VotingPersistentActor extends PersistentActor with ActorLogging {
    var votedCitizens = mutable.HashSet[String]()
    var pollResults = mutable.HashMap[String, Int]()

    override def persistenceId: String = "voting-persistent-actor"

    override def receiveCommand: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        if(!votedCitizens.contains(vote.citizenPID)) {
          persist(Vote(citizenPID, candidate)) { e =>
            handleInternalStateChange(citizenPID, candidate)
            log.info(s"Persisted $e")
          }
        } else {
          log.warning(s"Citizen $citizenPID is trying to vote multiple times")
        }
      case "print" =>
        log.info(s"Voted candidate: ${votedCitizens}")
        log.info(s"Poll Results: ${pollResults}")
    }

    def handleInternalStateChange(citizenPID: String, candidate: String) = {
        votedCitizens.add(citizenPID)
        val votes = pollResults.getOrElse(candidate, 0)
        pollResults.put(candidate, votes + 1)
    }

    override def receiveRecover: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        handleInternalStateChange(citizenPID, candidate)
        log.info(s"Recovered $vote")
    }
  }

  val system = ActorSystem("VotingActorSystem")
  val votingPersistentActor = system.actorOf(Props[VotingPersistentActor], "votingPersistentActor")

  val votesMap = Map[String, String](
    "Alice" -> "Martin",
    "Bob" -> "Roland",
    "Charlie" -> "Martin",
    "David" -> "Jonah",
    "Daniel" -> "Martin",
  )

//  votesMap.keys.foreach { citizen =>
//    votingPersistentActor ! Vote(citizen, votesMap(citizen))
//  }
//
    votingPersistentActor ! Vote("Daniel", "Daniel")
    votingPersistentActor ! "print"

}
