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

let numberOfActors = 8
let workUnit = 100

let sumOfSquares numberList = numberList |> List.sumBy (fun x -> x * x)
let isPerfectSquare (number:uint64) = sqrt (float number) |> fun n -> (n = floor(n))

let mutable count = numberOfActors 

type PrintServer(name) =
    inherit Actor()
    
    override x.OnReceive message =
       match message with
        | :? uint64 as n -> 
            printfn "%i" n
        | _ -> failwith "unknown message"

let properties = [| "printer" :> obj |]
let printActor = system.ActorOf(Props(typedefof<PrintServer>, properties))

type EchoServer(name) =
    inherit Actor()
    
    override x.OnReceive message =
       match message with
        | :? uint64 as n ->
            let rangeStart = uint64(n*uint64(workUnit) + uint64(1))
            let rangeEnd = min (rangeStart + uint64(workUnit-1)) (uint64(maxNum))
            for k in [rangeStart .. rangeEnd] do
                sumOfSquares [k .. (k + uint64(len-1))] |> fun sq -> if(isPerfectSquare sq) then printActor <! k
            
            if rangeEnd + uint64(workUnit * workUnit) > uint64(maxNum) then
                count <- count + 1
        | _ -> failwith "unknown message"



let echoServers =
    [ 1 .. numberOfActors ] |> List.map (fun id ->
        let properties = [| string (id) :> obj |]
        system.ActorOf(Props(typedefof<EchoServer>, properties)))


let actorRef = echoServers
let mutable cur = 0
let temp = maxNum/workUnit


for num in [0 .. temp] do
    cur <- (num%numberOfActors)
    if num < numberOfActors then
        count <- count - 1
    actorRef.Item(cur) <! uint64(num)


while count <> numberOfActors do
    cur <- cur 

system.Terminate |> ignore