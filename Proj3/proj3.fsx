#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

let clock = Diagnostics.Stopwatch()

//Take inputs
let system = ActorSystem.Create("Pastry")

let mutable numNodes =
    int (string (fsi.CommandLineArgs.GetValue 1))
let mutable numReqs =
    int (string (fsi.CommandLineArgs.GetValue 2))
let nodes = [ 0 .. numNodes]
let actorsArray = Array.zeroCreate (numNodes + 1)
let mutable masterNode: IActorRef = null
let dic = new Dictionary<string, int>()
let rdic = new Dictionary<int, String>()

//Implement random key or id function
let getRandomId id : String = 
    let mutable rid = ""
    let mutable found = false
    while not found do
        for i in [0 .. 7] do
            rid <- rid + System.Random().Next(0, 8).ToString()
        if dic.ContainsKey rid then
            rid <- "" 
            found <- false
        else
            dic.Add(rid, id)
            rdic.Add(id,rid)
            found <- true
    rid

let getRandomKey : String = 
    let mutable rid = ""
    for i in [0 .. 7] do
        rid <- rid + System.Random().Next(0, 8).ToString()
    rid

//Implement Messages
type RouteMessage = 
    { rid : string
      rtable : String[,]
      largeLeafSet : String list 
      smallLeafSet : String list
      level : int}

type DeliveryMessage = 
    { dRouteTable : String[,]
      dLargeLeafSet : String list 
      dSmallLeafSet : String list
    }

type NextPeerMessage = 
    { nextPeer : String
      dRouteTable : String[,]
      dLargeLeafSet : String list 
      dSmallLeafSet : String list
      level : int
    }

type ForwardMessage = 
    { des : String
      level : int
      noHops : int
    }

type NodeMessages = 
    | FirstJoin of String
    | Init of String
    | Join of String
    | AddMe of RouteMessage
    | Deliver of DeliveryMessage
    | NextPeer of NextPeerMessage
    | Finished of int
    | StartRouting of String
    | Forward of ForwardMessage
    | PrintLeaf of String

type MasterMessages = 
    | FirstInit of IActorRef
    | Init of int
    | Joined of String
    | Finished of int


//Implement Pastry Node
type PastryNode() = 
    inherit Actor()
    let mutable largeLeafSet = []
    let mutable smallLeafSet = []
    let mutable nset = []
    let mutable routingTable = Array2D.init 8 8 (fun i j -> "")
    let mutable nodeId = ""
    //let mutable masterNode: IActorRef = null
    

    //Implement Shared Length
    let shl (nodeID1:String, nodeId2:String) : int = 
        let mutable i = 0
        if nodeID1 = "" || nodeId2 = "" then i
        else
            while i<8 && nodeID1.[i] = nodeId2.[i] do
                i<-i+1
            i
    // Implement Routing Function
    let route curr dest level func : String = 
        //printfn "Finding route from curr %A to dest %A for level %A with func %A" curr dest level func
        let mutable found = false
        let mutable next = ""
        let mutable nextDec = ""
        if level = 8 then
            found <- true
            next <- "null"

        //Search in Leaf Tables
        if not found then
            if String.Compare (dest ,curr) > 0 then
                if largeLeafSet.Length > 0 then
                    //Find next Node in large leaf set for Join   
                    if func="join" then
                        if String.Compare(dest, largeLeafSet.[largeLeafSet.Length-1]) < 0 then
                            for i in [0 .. largeLeafSet.Length-1] do
                                if String.Compare(largeLeafSet.[i], dest) <0 then
                                    nextDec <- largeLeafSet.[i]
                    else if func = "route" then
                        if String.Compare(dest, largeLeafSet.[largeLeafSet.Length-1]) <= 0 then
                            for i in [0 .. largeLeafSet.Length-1] do
                                if String.Compare(largeLeafSet.[i], dest) <= 0 then
                                    nextDec <- largeLeafSet.[i]

                if nextDec<>"" then
                    next <- nextDec
                    found<-true
            else if String.Compare (dest ,curr) < 0 then
                if smallLeafSet.Length > 0 then
                    //Find next Node in Small leaf set for Join
                    if func="join" then
                        if String.Compare(dest, smallLeafSet.[smallLeafSet.Length-1]) > 0 then
                            for i in [0 .. smallLeafSet.Length-1] do
                                if String.Compare(smallLeafSet.[i], dest) <0 then
                                    nextDec <- smallLeafSet.[i]
                    else if func = "route" then
                        if String.Compare(dest, smallLeafSet.[smallLeafSet.Length-1]) <= 0 then
                            for i in [0 .. smallLeafSet.Length-1] do
                                if String.Compare(smallLeafSet.[i], dest) <= 0 then
                                    nextDec <- smallLeafSet.[i]

                if nextDec<>"" then
                    next <- nextDec
                    found<-true

        //Search in Route Tables
        if not found then
            let mutable dl = int(dest.[level]) - int '0'
            if routingTable.[level,dl]<>""then
                // printfn "Leaf sets are Empty!!"
                if func="join" then
                    if String.Compare(routingTable.[level,dl], dest) < 0 then
                        next <- routingTable.[level,dl]
                    else
                        next <- ""
                    found <- true
                else if func="route" then
                    if String.Compare(routingTable.[level,dl], dest) <= 0 then
                        next <- routingTable.[level,dl]
                        // printfn "From RT : %A" next
                        found <- true
        
        //Search Nearby
        if not found then
            let mutable setOfNodes = Set.empty
            for node in largeLeafSet do
                if shl(dest, node) >= level then
                    setOfNodes <- setOfNodes.Add(node) 

            for node in smallLeafSet do
                if shl(dest, node) >= level then
                    setOfNodes <- setOfNodes.Add(node)
            
            for i in [0..7] do
                for j in [0..7] do
                    if routingTable.[i,j] <> "" then
                        setOfNodes <- setOfNodes.Add(routingTable.[i,j])

            let mutable currVal = nodeId |> int64
            let mutable desVal = dest |> int64
            let mutable mindiff = Math.Abs (currVal-desVal)
            for node in setOfNodes do
                let nodeValue = node |> int64
                if nodeValue<> desVal then
                    let diff = Math.Abs (nodeValue-desVal)
                    if diff < mindiff then
                        mindiff <- diff
                        next <- node
                        found <- true

        if not found then
            next <- ""
        next

    let updateLeafTables level dest = 
        let mutable isLargeFull = false
        let mutable isSmallFull = false

        if largeLeafSet.Length >=4 then
            isLargeFull <- true

        if smallLeafSet.Length >=4 then
            isSmallFull <- true

        if String.Compare(dest, nodeId) > 0 then
            if not (List.contains dest largeLeafSet) then
                if isLargeFull then
                    largeLeafSet <- List.append largeLeafSet [dest]
                    largeLeafSet <- List.sort largeLeafSet
                    largeLeafSet <- largeLeafSet.[0..3]
                else
                    largeLeafSet <- List.append largeLeafSet [dest]
                    largeLeafSet <- List.sort largeLeafSet
        else if String.Compare(dest, nodeId) < 0 then
            if not (List.contains dest smallLeafSet) then
                if isSmallFull then
                    smallLeafSet <- List.append smallLeafSet [dest]
                    smallLeafSet <- List.sort smallLeafSet
                    smallLeafSet <- smallLeafSet.[0..3]
                else
                    smallLeafSet <- List.append smallLeafSet [dest]
                    smallLeafSet <- List.sort smallLeafSet

    let updateRouteTable (level:int,dest:string):unit = 
        let dLevel: int = int(dest.[level]) - int '0'
        routingTable.[level, dLevel] <- dest

    let updateNewRouteTable (dest:String, level:int, rt: String[,], lastLevel:int) : String[,] = 
        let mutable newRT = Array2D.init 8 8 (fun i j -> rt.[i,j])

        for i in [lastLevel..level] do
            for j in [0..7] do
                if routingTable.[i,j] <> "" then
                    newRT.[i,j] <- routingTable.[i,j]

        let dLevel: int = int(nodeId.[level]) - int '0'
        if newRT.[level, dLevel] <> "" then
            if String.Compare(newRT.[level, dLevel], nodeId) > 0 then
                newRT.[level, dLevel] <- newRT.[level, dLevel]
            else
                newRT.[level, dLevel] <- nodeId
        else
            newRT.[level, dLevel] <- nodeId

        let rLevel: int = int(dest.[level]) - int '0'
        newRT.[level, rLevel] <- ""
        newRT

    let updateNewLarge (currentDec: String, dest:String, largeL: String list) : String list = 
        let mutable isLargeFull = false
        let mutable tempLargeL = largeL
        if largeL.Length >= 4 then
            isLargeFull <- true
        
        if String.Compare(currentDec, dest) > 0 then
            if not (List.contains currentDec tempLargeL) then
                if not isLargeFull then
                    tempLargeL <- List.append tempLargeL [currentDec]
                    tempLargeL <- List.sort tempLargeL
                    tempLargeL <- tempLargeL.[0..3]
                else
                    tempLargeL <- List.append tempLargeL [currentDec]
                    tempLargeL <- List.sort tempLargeL

        tempLargeL

    let updateNewSmall (currentDec: String, dest:String, smallL: String list) : String list = 
        let mutable isSmallFull = false
        let mutable tempSmallL = smallL
        if smallL.Length >= 4 then
            isSmallFull <- true
        
        if String.Compare(currentDec, dest) < 0 then
            if not (List.contains currentDec tempSmallL) then
                if not isSmallFull then
                    tempSmallL <- List.append tempSmallL [currentDec]
                    tempSmallL <- List.sort tempSmallL
                    tempSmallL <- tempSmallL.[0..3]
                else
                    tempSmallL <- List.append tempSmallL [currentDec]
                    tempSmallL <- List.sort tempSmallL
        tempSmallL

    let makeRoute =
        let count = dic.GetValueOrDefault(nodeId, 0)
        let mutable rid = ""
        let mutable level =0
        for i in [0..count-1] do
            rid <- rdic.GetValueOrDefault(i,"")
            if rid<>"" && rid<>nodeId then
                level <- shl(rid, nodeId)
                let dLevel: int = int(rid.[level])
                routingTable.[level,dLevel] <- rid


    override x.OnReceive(rmsg) =
        match rmsg :?> NodeMessages with
        | FirstJoin rid ->
            nodeId <- rid
            // printfn "First Node being added : %A" nodeId
            masterNode <! Joined nodeId

        | Join rid ->
            nodeId <- rid
            // printfn "New Node to be added : %A" nodeId
            actorsArray.[dic.GetValueOrDefault(rid)-1] <! AddMe {rid = rid; rtable = routingTable; largeLeafSet = largeLeafSet; smallLeafSet = smallLeafSet; level = 0;}
        
        | AddMe routeMsg ->
            
            let mutable lastHop = false
            let mutable destinationRouteTable = Array2D.init 8 8 (fun i j -> routeMsg.rtable.[i,j])
            let mutable deliverSmallT = routeMsg.smallLeafSet
            let mutable deliverLargeT = routeMsg.largeLeafSet
            if smallLeafSet.Length = 0 && largeLeafSet.Length = 0 then
                lastHop <- true
            let level = shl(nodeId, routeMsg.rid)
            let next = route nodeId routeMsg.rid level "join"
            if next="" then
                lastHop<-true

            updateLeafTables level routeMsg.rid
            updateRouteTable (level,routeMsg.rid)
            destinationRouteTable <- updateNewRouteTable(routeMsg.rid, level, routeMsg.rtable, routeMsg.level)
            deliverLargeT <- updateNewLarge(nodeId, routeMsg.rid, deliverLargeT)
            deliverSmallT <- updateNewSmall(nodeId, routeMsg.rid, deliverSmallT)
            for curr in largeLeafSet do
                deliverLargeT <- updateNewLarge(curr, routeMsg.rid, deliverLargeT)
                deliverSmallT <- updateNewSmall(curr, routeMsg.rid, deliverSmallT)
            for curr in smallLeafSet do
                deliverLargeT <- updateNewLarge(curr, routeMsg.rid, deliverLargeT)
                deliverSmallT <- updateNewSmall(curr, routeMsg.rid, deliverSmallT)

            let nodeIdx = dic.GetValueOrDefault(routeMsg.rid,0)
            if not lastHop then
                actorsArray.[nodeIdx] <! NextPeer {nextPeer = next;dRouteTable=destinationRouteTable; dLargeLeafSet=deliverLargeT; dSmallLeafSet=deliverSmallT; level=level}
            else
                actorsArray.[nodeIdx] <! Deliver {dRouteTable = destinationRouteTable; dLargeLeafSet=deliverLargeT; dSmallLeafSet=deliverSmallT}


            // printfn "Next Node : %A" next

        | Deliver deliverMsg -> 
            routingTable <- deliverMsg.dRouteTable
            largeLeafSet <- deliverMsg.dLargeLeafSet
            smallLeafSet <- deliverMsg.dSmallLeafSet
            makeRoute
            masterNode <! Joined nodeId
        | NextPeer nextPeerMsg -> 
            routingTable <- nextPeerMsg.dRouteTable
            largeLeafSet <- nextPeerMsg.dLargeLeafSet
            smallLeafSet <- nextPeerMsg.dSmallLeafSet
            let nextId = dic.GetValueOrDefault(nextPeerMsg.nextPeer, 0)
            actorsArray.[nextId] <! AddMe {rid = nodeId; rtable = routingTable; largeLeafSet = largeLeafSet; smallLeafSet = smallLeafSet; level =nextPeerMsg.level;}             
        
        | StartRouting message ->
            let key = getRandomKey
            let level = shl(nodeId,key)
            let id = dic.GetValueOrDefault(nodeId,0)
            actorsArray.[id] <! Forward  {des = key; level = level; noHops = 0}

        |Forward forwardMessage ->
            let mutable hops = forwardMessage.noHops
            let mutable next = route nodeId forwardMessage.des forwardMessage.level "route"
            if next = "" || hops>=10 then
                masterNode<!Finished hops
            else 
                hops <- hops+1
                let newlevel = shl(forwardMessage.des,next)
                let id = dic.GetValueOrDefault(next,0)
                actorsArray.[id] <! Forward {des = forwardMessage.des; level = newlevel; noHops = hops}
        // |PrintLeaf id ->
            //for i in [0..largeLeafSet.Length] do+
            
            // printfn "%A LargeLeaf" largeLeafSet.[i] 

            //for i in [0..smallLeafSet.Length] do
              //  printfn "%A SmallLeaf" smallLeafSet.[i]     
            // printfn "Node %i Lset" largeLeafSet.Length
            // printfn "Node %i Sset" smallLeafSet.Length
            // printfn "Routing Table %A %A " nodeId routingTable 
        
        | _ -> failwith "unknown message"

    

//Implement Master

for id in nodes do
    actorsArray.[id] <- system.ActorOf(Props.Create(typeof<PastryNode>), "node" + id.ToString())

type Master() =
    inherit Actor()
    let mutable numberOfRumoursReceived = 0
    let mutable countOfNodes = 0
    let mutable finishedNodes = 0
    let mutable finishedHops = 0
    // let b = 3
    // let l = 6
    override x.OnReceive(rmsg) =
        match rmsg :?> MasterMessages with
        | FirstInit master ->
            let rid = getRandomId countOfNodes
            masterNode <- master
            actorsArray.[0] <! FirstJoin rid

        | Init id ->
            let rid = getRandomId countOfNodes
            actorsArray.[id] <! Join rid           

        | Joined nodeId ->
            countOfNodes <- countOfNodes + 1
            if countOfNodes >= numNodes - 1 then
                printfn "All Nodes joined."
                printfn "Initiating routing."
                for i in [0 .. actorsArray.Length-1] do
                    for j in [0..numReqs-1] do
                        // actorsArray.[i] <! PrintLeaf "id"
                        actorsArray.[i] <! StartRouting "Message"
            else 
                // printfn "Joined" 
                masterNode <! Init countOfNodes
        |Finished numberOfhops ->
            finishedNodes <- finishedNodes + 1
            finishedHops <- finishedHops + numberOfhops
            if(finishedNodes >= (numNodes * numReqs)) then
                
                printfn "All nodes have finished routing ...\n"
                printfn "******************************"
                printfn "Total routes count = %A" (numNodes * numReqs)
                printfn "Total requests count = %A" finishedHops
                printfn "Average number of request = %A" ( (double) finishedHops / ( ((double) numNodes) * ((double) numReqs)))
                printfn "******************************"
                Environment.Exit(0)

        | _ -> failwith "unknown message"


masterNode <- system.ActorOf(Props.Create(typeof<Master>), "master")




//Implement Project3 starter:



printfn "Starting initialization of network with %i nodes to send %i reqs each." numNodes numReqs

masterNode <! FirstInit masterNode
System.Console.ReadLine() |> ignore
