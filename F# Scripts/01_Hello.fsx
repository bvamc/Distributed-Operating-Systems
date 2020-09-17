#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.FSharp

// #Using Actor
// Actors are one of Akka's concurrent models.
// An Actor is a like a thread instance with a mailbox.
// It can be created with system.ActorOf: use receive to get a message, and <! to send a message.
// This example is an EchoServer which can receive messages then print them.

let system = ActorSystem.Create("FSharp")

type EchoServer =
    inherit Actor //Untyped Actor

    override x.OnReceive message =
        match message with
        | :? string as msg -> printfn "Hello %s" msg
        | _ ->  printfn "Unknown type %A" (message.GetType())

let echoServer = system.ActorOf(Props(typedefof<EchoServer>, Array.empty))

echoServer <! "F#!"
echoServer <! 42

system.Terminate()