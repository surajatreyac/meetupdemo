import scala.collection._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scala.io._
import java.io.RandomAccessFile
import java.io.File
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import akka.actor.ActorRef
import com.typesafe.config.ConfigFactory

object Test {

  trait FileChunk
  case class Sendline(raf: RandomAccessFile) extends FileChunk
  case class ReceiveLine(line: String, raf: RandomAccessFile) extends FileChunk
  case class Start(f: File, s: Long, e: Long, actRef: List[ActorRef]) extends FileChunk
  case class Interrupt(re: String) extends FileChunk

  def main(args: Array[String]): Unit = {

    val customConf = ConfigFactory.parseString("""
    akka {
      actor{
    	my-dispatcher {
			type = PinnedDispatcher
			executor = "thread-pool-executor"
		  }
	   }
    }
    """)

    implicit val system = ActorSystem("MySystem", ConfigFactory.load(customConf))

    val file = new File("log.txt")

    //Create 4 actors with 'Pinned dispatcher' which creates one thread per actor 

    val testRaf1 = system.actorOf(Props[TestRAF], name = "TestRAF1")
    val testRaf2 = system.actorOf(Props[TestRAF], name = "TestRAF2")
    val testRaf3 = system.actorOf(Props[TestRAF], name = "TestRAF3")
    val testRaf4 = system.actorOf(Props[TestRAF], name = "TestRAF4")

    val chunks = Runtime.getRuntime().availableProcessors()

    //Calculate offsets for a file to provide this to individual actors
    //RandomAccessFile lets us seek between files
    val raf = new RandomAccessFile(file, "r")

    var found = true
    var offsets = ListBuffer[Long]()

    for (i <- 1 to chunks) {
      raf.seek(i * file.length() / chunks);

      while (found) {
        val read = raf.read()
        if (read == '\n' || read == -1) {
          found = false
        }
      }
      if (found == false) {
        offsets += raf.getFilePointer()
        found = true
      }
    }
    raf.close();

    //val chunkReaderList = List(testRaf1)
    val chunkReaderList = List(testRaf1, testRaf2, testRaf3, testRaf4) //List of ActorRef
    val offsetList = (0 :: offsets.toList).sliding(2).collect { case List(a, b) => (a, b) }
    val msgs = chunkReaderList zip offsetList.toList // Create a tuple of ActorRef along with file offsets

    for (msg <- msgs) {
      println(msg._2._1.toString.toLong + " " + msg._2._2.toString.toLong)
      msg._1 ! Start(file, msg._2._1.toString.toLong, msg._2._2.toString.toLong, chunkReaderList)
    }

  }

  class TestRAF extends Actor{

    var start = 0L
    var end = 0L
    var actorRefList = List[ActorRef]()

    //Hard coded for now for testing. The regexes are copied from Siemens context
    var regexList = List(
      ".+SystemType=([^;]+);.+",
      ".+Computername=([^;]*);.+",
      ".+SWUpgradePacks=([^;]*);.+",
      ".+Version=([^;]*);.+",
      ".+WorkplaceType=([^;]*);.+",
      ".+MaterialNumber=([^;]*);.+",
      ".+Country=([^;]*);.+",
      ".+HospitalName=([^;]*);.+",
      ".+City=([^;]*);.+",
      ".+Data\\s*Slipring=([^;]*);.+",
      ".+Power\\s*Slipring=([^;]*);.+",
      ".+Patient\\s*Table=([^;]*);.+",
      ".+DMS=([^;]*);.+",
      ".+Cooling=([^;]*);.+",
      ".+ConsecutiveDayId=([^;]*);.*")

    var it = regexList.iterator

    def receive = {

      case Start(f: File, s: Long, e: Long, actRef: List[ActorRef]) =>
        val raf = new RandomAccessFile(f, "r")
        raf.seek(s);
        start = s
        end = e
        actorRefList = actRef
        actorRefList.foreach(println)
        self ! Sendline(raf)

      case Sendline(raf: RandomAccessFile) =>
        val rfp = raf.getFilePointer()
        if (rfp < end) {
          it = regexList.iterator
          self ! ReceiveLine(raf.readLine(), raf)
        }
        if (rfp == end) {
          println("Done")
        }

      case ReceiveLine(line: String, raf: RandomAccessFile) =>

        val regex = if (it.hasNext) it.next else ""
        val re = new Regex(regex)
        re.findFirstIn(line) match {
          case None => ""
          case a: Option[String] =>
            println("Matched:" + re + " " + a)

            //This actor found a match. Send message to all other actors to stop working
            // on this regex if working or remove from their respective regex list if present
            actorRefList.foreach(a => a ! Interrupt(regex))
        }

        if (!regexList.isEmpty)
          self ! Sendline(raf)

      case Interrupt(re: String) =>
        //If the actors containing the regex currently worked on if present, remove them
        //because some other actor found a match first
        if (!regexList.isEmpty) {
          println("Interrupted...")
          regexList = regexList.dropWhile(_.contains(re))
        }
    }
  }
}
