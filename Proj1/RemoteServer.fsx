#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

// #Remote Actor
// Actor is not only a concurrency model, it can also be used for distributed computing.
// This example builds an EchoServer using an Actor.
// Then it creates a client to access the Akka URL.
// The usage is the same as with a normal Actor.

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote {
                helios.tcp {
                    port = 8777
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("RemoteActorSystem", configuration)
let numberOfActors = Environment.ProcessorCount
let printActor = "akka.tcp://RemoteFSharp@localhost:7777/user/PrintServer";
let actorList =
    [ 1 .. numberOfActors ] |> List.map (fun id ->
        spawn system ("actor"+string(id))
        <| fun mailbox ->
            let rec loop() =
                actor {
                    let! message = mailbox.Receive()
                    let sender = mailbox.Sender()
                    printfn "%A" message
                    match box message with
                    | :? string -> 
                            printfn "super uint64!"
                            system.ActorSelection(printActor) <! message
                            return! loop()
                    | _ ->  failwith "unknown message"
                } 
            loop())

let mutable indx = 0;
let assignActor = 
    spawn system "Distributor"
    <| fun mailbox ->
        let rec loop() =
            actor {
                let! message = mailbox.Receive()
                let sender = mailbox.Sender()
                printfn "Hey %A" message
                match box message with
                | :? string -> 
                        actorList.Item((indx%actorList.Length)) <! message
                        indx <- indx+1
                        return! loop()
                | _ ->  failwith "unknown message"
            }  
        loop()

Console.Read() |> ignore