package com.github.haoch.experimental

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._

object Bootstrap extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("on-spray-can")

  // create and start our service actor
  val service = system.actorOf(Props[CEPServiceActor], "control-service")

  implicit val timeout = Timeout(5.seconds)

  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "localhost", port = 8080)
}

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class CEPServiceActor extends Actor with CEPService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(route)
}

// this trait defines our service behavior independently from the service actor
trait CEPService extends HttpService {
  val route: Route = {
    path("") {
      get {
        respondWithMediaType(`text/html`) { // XML is marshalled to `text/xml` by default, so we simply override here
          complete {
            <html>
              <body>
                <h1>Say hello to
                  <i>spray-routing</i>
                  on
                  <i>spray-can</i>
                  !</h1>
              </body>
            </html>
          }
        }
      }
    }

    path("/api/v1/queries/:id") {
      put {
        respondWithMediaType(`application/json`) {
          complete {
            ???
          }
        }
      }
      delete {
        respondWithMediaType(`application/json`) {
          ???
        }
      }
    }

    path("/api/v1/queries") {
      get {
        respondWithMediaType(`application/json`) {
          complete {
            ???
          }
        }
      }
      post {
        respondWithMediaType(`application/json`) {
          complete {
            ???
          }
        }
      }
    }
  }
}