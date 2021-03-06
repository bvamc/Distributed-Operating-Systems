#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.FSharp

let clock = Diagnostics.Stopwatch()

let mutable nodesCount =
    int (string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol = string (fsi.CommandLineArgs.GetValue 3)

type MessageType =
    | Initialize of IActorRef []
    | IntializeAll of IActorRef []
    | DoGossip of String
    | ReportRumourRecv of String
    | DoPushSum of Double
    | ComputePushSum of Double * Double * Double
    | TerminationCheck of Double * Double
    | SetNumNodes of int

type Receiver() =
    inherit Actor()
    let mutable numberOfRumoursReceived = 0
    let mutable numberOfNodes = 0

    override x.OnReceive(rmsg) =
        match rmsg :?> MessageType with
        | ReportRumourRecv message ->
            numberOfRumoursReceived <- numberOfRumoursReceived + 1
            if numberOfRumoursReceived = numberOfNodes then
                clock.Stop()
                printfn "Time taken for convergence: %O" clock.Elapsed
                Environment.Exit(0)

        | TerminationCheck (sum, weight) ->
            printfn "Sum = %f Weight= %f Average=%f" sum weight (sum / weight)
            numberOfRumoursReceived <- numberOfRumoursReceived + 1
            // printfn "Hearing Actor %i %i" numberOfRumoursReceived nodesCount
            if numberOfRumoursReceived = nodesCount then
                clock.Stop()
                printfn "Answer! Sum = %f Weight= %f Average=%f" sum weight (sum / weight)
                printfn "Time for convergence: %O" clock.Elapsed
                Environment.Exit(0)

        | SetNumNodes numberofnodes -> numberOfNodes <- numberofnodes
        | _ -> failwith "unknown message"

type Node(listener: IActorRef, numResend: int, nodeNum: int) =
    inherit Actor()
    let mutable numMsgHeard = 0
    let mutable neighbourNodes: IActorRef [] = [||]
    let mutable totalNodes: IActorRef [] = [||]
    let mutable sum = nodeNum |> float
    let mutable weight = 1.0
    let mutable epochCount = 1
    let mutable convergence = false

    override x.OnReceive(num) =
        match num :?> MessageType with
        | Initialize arrRef -> neighbourNodes <- arrRef
        | IntializeAll arrRef -> totalNodes <- arrRef
        | DoGossip rumour ->
            numMsgHeard <- numMsgHeard + 1
            if (numMsgHeard = 1) then listener <! ReportRumourRecv(rumour)
            if (numMsgHeard <= 10 * nodesCount) then
                if (topology <> "imp2D") then
                    let index =
                        System.Random().Next(0, neighbourNodes.Length)
                    neighbourNodes.[index] <! DoGossip(rumour)
                else
                    let index = System.Random().Next(0, 5)
                    if (index < 4) then
                        let nindex =
                            System.Random().Next(0, neighbourNodes.Length)
                        neighbourNodes.[nindex] <! DoGossip(rumour)
                    else
                        let mutable fullindex = System.Random().Next(0, nodesCount - 1)
                        while (fullindex = nodeNum) do
                            fullindex <- System.Random().Next(0, nodesCount - 1)
                        totalNodes.[fullindex] <! DoGossip(rumour)

        | DoPushSum delta ->
            let index =
                System.Random().Next(0, neighbourNodes.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbourNodes.[index]
            <! ComputePushSum(sum, weight, delta)

        | ComputePushSum (s: float, w, delta) ->
            let newsum = sum + s
            let newweight = weight + w

            let ratioDifference =
                sum / weight - newsum / newweight |> abs

            if (ratioDifference > delta) then
                epochCount <- 0
                sum <- sum + s
                weight <- weight + w
                sum <- sum / 2.0
                weight <- weight / 2.0
                if (topology <> "imp2D") then
                    let index =
                        System.Random().Next(0, neighbourNodes.Length)

                    neighbourNodes.[index]
                    <! ComputePushSum(sum, weight, delta)
                else
                    let index = System.Random().Next(0, 5)
                    if (index < 4) then
                        let nindex =
                            System.Random().Next(0, neighbourNodes.Length)

                        neighbourNodes.[nindex]
                        <! ComputePushSum(sum, weight, delta)
                    else
                        let mutable fullindex = System.Random().Next(0, nodesCount - 1)
                        while (fullindex = nodeNum) do
                            fullindex <- System.Random().Next(0, nodesCount - 1)
                        totalNodes.[fullindex]
                        <! ComputePushSum(sum, weight, delta)
            //elif (termRound>=3) then
            else
                if (not convergence) then
                    listener <! TerminationCheck(sum, weight)
                    convergence <- true

                //else
                sum <- sum + s
                weight <- weight + w
                sum <- sum / 2.0
                weight <- weight / 2.0
                epochCount <- epochCount + 1
                if (topology <> "imp2D") then
                    let index =
                        System.Random().Next(0, neighbourNodes.Length)

                    neighbourNodes.[index]
                    <! ComputePushSum(sum, weight, delta)
                else
                    let index = System.Random().Next(0, 5)
                    if (index < 4) then
                        let nindex =
                            System.Random().Next(0, neighbourNodes.Length)

                        neighbourNodes.[nindex]
                        <! ComputePushSum(sum, weight, delta)
                    else
                        let mutable fullindex = System.Random().Next(0, nodesCount - 1)
                        while (fullindex = nodeNum) do
                            fullindex <- System.Random().Next(0, nodesCount - 1)
                        totalNodes.[fullindex]
                        <! ComputePushSum(sum, weight, delta)


        | _ -> failwith "unknown message"






let system = ActorSystem.Create("System")

let mutable actualNumOfNodes = float (nodesCount)


nodesCount =
    if topology = "2D" || topology = "imp2D"
    then int (floor ((actualNumOfNodes ** 0.5) ** 2.0))
    else nodesCount

let nodeList = [ 0 .. nodesCount ] 

let receiver =
    system.ActorOf(Props.Create(typeof<Receiver>), "receiver")

match topology with
| "full" ->
    let nodeArrayOfActors = Array.zeroCreate (nodesCount + 1)
    for i in nodeList do
        nodeArrayOfActors.[i] <- system.ActorOf(Props.Create(typeof<Node>, receiver, 10, i + 1), "demo" + string (i))
    for i in nodeList do
        nodeArrayOfActors.[i] <! Initialize(nodeArrayOfActors)

    let leader = System.Random().Next(0, nodesCount)
    if protocol = "gossip" then
        receiver <! SetNumNodes(nodesCount)
        clock.Start()
        printfn "Starting Protocol Gossip for full topology"
        nodeArrayOfActors.[leader] <! DoGossip("Hello")
    else if protocol = "push-sum" then
        clock.Start()
        printfn "Starting Push Protocol for full topology"
        nodeArrayOfActors.[leader] <! DoPushSum(10.0 ** -10.0)

| "line" ->
    let nodeArray = Array.zeroCreate (nodesCount)
    for i in [ 0 .. nodesCount - 1 ] do
        nodeArray.[i] <- system.ActorOf(Props.Create(typeof<Node>, receiver, 10, i + 1), "demo" + string (i))
    for i in [ 0 .. nodesCount - 1 ] do
        let mutable neighbourArray: IActorRef [] = [||]
        if i = 0 then
            neighbourArray <- [| nodeArray.[i + 1] |]
        else if i = nodesCount - 1 then
            neighbourArray <- [| nodeArray.[i - 1] |]
        else
            neighbourArray <-
                [| nodeArray.[i - 1]
                   nodeArray.[i + 1] |]

        nodeArray.[i] <! Initialize(neighbourArray)
    let leader = System.Random().Next(0, nodesCount)
    receiver <! SetNumNodes(nodesCount)
    if protocol = "gossip" then
        clock.Start()
        printfn "Starting Protocol Gossip for line topology"
        nodeArray.[leader]
        <! DoGossip("This is Line Topology")
    else if protocol = "push-sum" then
        clock.Start()
        printfn "Starting Push Sum Protocol for line topology"
        nodeArray.[leader] <! DoPushSum(10.0 ** -10.0)



| "2D" ->
    let gridSize = int (ceil (sqrt actualNumOfNodes))
    let totGrid = gridSize * gridSize
    let nodeArray = Array.zeroCreate (totGrid)
    for i in [ 0 .. (gridSize * gridSize - 1) ] do
        nodeArray.[i] <- system.ActorOf(Props.Create(typeof<Node>, receiver, 10, i + 1), "demo" + string (i))

    for i in [ 0 .. gridSize - 1 ] do
        for j in [ 0 .. gridSize - 1 ] do
            let mutable neighbours: IActorRef [] = [||]
            if j + 1 < gridSize
            then neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j + 1] |])
            if j - 1 >= 0
            then neighbours <- Array.append neighbours [| nodeArray.[i * gridSize + j - 1] |]
            if i - 1 >= 0
            then neighbours <- Array.append neighbours [| nodeArray.[(i - 1) * gridSize + j] |]
            if i + 1 < gridSize
            then neighbours <- (Array.append neighbours [| nodeArray.[(i + 1) * gridSize + j] |])
            nodeArray.[i * gridSize + j]
            <! Initialize(neighbours)



    let leader = System.Random().Next(0, totGrid - 1)
    if protocol = "gossip" then
        receiver <! SetNumNodes(totGrid - 1)
        clock.Start()
        printfn "Starting Protocol Gossip for 2D topology"
        nodeArray.[leader]
        <! DoGossip("This is 2D Topology")
    else if protocol = "push-sum" then
        clock.Start()
        printfn "Starting Push Sum Protocol for 2D topology"
        nodeArray.[leader] <! DoPushSum(10.0 ** -10.0)


| "imp2D" ->
    let gridSize = int (ceil (sqrt actualNumOfNodes))
    let totGrid = gridSize * gridSize
    let nodeArray = Array.zeroCreate (totGrid)
    for i in [ 0 .. (gridSize * gridSize - 1) ] do
        nodeArray.[i] <- system.ActorOf(Props.Create(typeof<Node>, receiver, 10, i + 1), "demo" + string (i))

    for i in [ 0 .. gridSize - 1 ] do
        for j in [ 0 .. gridSize - 1 ] do
            let mutable neighbours: IActorRef [] = [||]
            if j + 1 < gridSize
            then neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j + 1] |])
            if j - 1 >= 0
            then neighbours <- Array.append neighbours [| nodeArray.[i * gridSize + j - 1] |]
            if i - 1 >= 0
            then neighbours <- Array.append neighbours [| nodeArray.[(i - 1) * gridSize + j] |]
            if i + 1 < gridSize
            then neighbours <- (Array.append neighbours [| nodeArray.[(i + 1) * gridSize + j] |])
            nodeArray.[i * gridSize + j]
            <! Initialize(neighbours)
            nodeArray.[i * gridSize + j]
            <! IntializeAll(nodeArray)

    let leader = System.Random().Next(0, totGrid - 1)
    if protocol = "gossip" then
        receiver <! SetNumNodes(totGrid - 1)
        clock.Start()
        printfn "Starting Protocol Gossip for imp2D topology"
        nodeArray.[leader]
        <! DoGossip("This is imp2D Topology")
    else if protocol = "push-sum" then
        clock.Start()
        printfn "Starting Push Sum Protocol for imp2D topology"
        nodeArray.[leader] <! DoPushSum(10.0 ** -10.0)


| _ -> failwith "unknown message"

System.Console.ReadLine() |> ignore