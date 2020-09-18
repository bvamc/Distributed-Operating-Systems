#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

// if (fsi.CommandLineArgs.Length<>2) 
//     then printfn "Invalid number of arguments"
// else 

let system = ActorSystem.Create("FSharp")
let maxNum = fsi.CommandLineArgs.[1] |> int
let len = fsi.CommandLineArgs.[2] |> int


let sumOfSquares numberList = numberList |> List.sumBy (fun x -> x * x)
let isPerfectSquare (number:int) = sqrt (float number) |> fun n -> (n = floor(n))

type EchoServer(name) =
    inherit Actor()
    
    override x.OnReceive message =
       match message with
        | :? int as n -> 
            sumOfSquares [n .. n+len-1] |> fun sq -> if(isPerfectSquare sq) then printfn "%i" n
        | _ -> failwith "unknown message"

let echoServers =
    [ 1 .. maxNum ] |> List.map (fun id ->
        let properties = [| string (id) :> obj |]
        system.ActorOf(Props(typedefof<EchoServer>, properties)))

let actorRef = echoServers
// printfn "%A" actorRef
let mutable m = 1
for ref in actorRef do
    ref <! m
    m <- m+1

Console.Read() |> ignore
system.Terminate()