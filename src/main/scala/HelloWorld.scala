
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

case class HelloWorld(who: String)

class HelloWorldActor extends Actor with ActorLogging {
  def receive = {
    case HelloWorld(person) => log.info("Hello " + person)
  }
}

object TestHelloWorld {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("MySystem")
    val helloWorldActorRef = system.actorOf(Props[HelloWorldActor], name = "hello")
    helloWorldActorRef ! HelloWorld("Mr.X")
  }
}
