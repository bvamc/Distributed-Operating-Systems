#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

// #Simplify Actor
// There is a simpler way to define an Actor

let system = ActorSystem.Create("FSharp")

let echoServer = 
    spawn system "EchoServer"
    <| fun mailbox ->
            actor {
                let! message = mailbox.Receive()
                match box message with
                | :? string as msg -> printfn "Hello %s" msg
                | _ ->  failwith "unknown message"
            } 

echoServer <! "F#!"
echoServer <! 42

system.Terminate()