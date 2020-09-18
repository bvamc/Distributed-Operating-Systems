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
let isPerfectSquare (number:int64) = sqrt (float number) |> fun n -> (n = floor(n))

type EchoServer(name) =
    inherit Actor()
    
    override x.OnReceive message =
       match message with
        | :? int64 as n -> 
            sumOfSquares [n .. (n + int64(len-1))] |> fun sq -> if(isPerfectSquare sq) then printfn "%i" n
        | _ -> failwith "unknown message"

let echoServers =
    [ 1 .. 8 ] |> List.map (fun id ->
        let properties = [| string (id) :> obj |]
        system.ActorOf(Props(typedefof<EchoServer>, properties)))

let actorRef = echoServers
// printfn "%A" actorRef
let mutable cur = 0
for num in [1 .. maxNum] do
    cur <- (num%8)
    actorRef.Item(cur) <! int64(num)

Console.Read() |> ignore
system.Terminate()