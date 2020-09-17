#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.FSharp

type Message =
    | Increment
    | Print

type OtherMessage =
    | Decrement
    | Display
  
type SimpleActor() as this =
    inherit ReceiveActor() //Untyped Actor

    // Declare a reference.
    let state = ref 0  //mutable is safe

    do
        //specialize receive
        this.Receive<Message>(fun m -> 
                                match m with 
                                | Print -> printfn "%i" !state
                                | Increment  -> state := !state + 1 )
        this.Receive<OtherMessage>(fun m -> 
                                match m with 
                                | Display -> printfn "%i" !state
                                | Decrement  -> state := !state - 1 )

    override this.Unhandled(msg) = //can be used for dead letter
        printfn "Unknown type %A" (msg.GetType())

let system = ActorSystem.Create("System02")
let actor = system.ActorOf<SimpleActor>()

actor.Tell Print    //Message
actor.Tell Increment
actor.Tell Increment
actor.Tell Increment
actor.Tell Print

actor <! Display    //OtherMessage
actor <! Decrement
actor <! Decrement
actor <! Decrement
actor <! Display

actor <! "Random"


    