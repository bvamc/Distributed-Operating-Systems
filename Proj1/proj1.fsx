#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic


let sw = Diagnostics.Stopwatch()
let mutable numOfNodes = int(string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol= string (fsi.CommandLineArgs.GetValue 3)

type Gossip =
    |Initialize of IActorRef[]
    |IntializeFull of IActorRef[]
    |StartGossip of String
    |ReportMsgRecvd of String
    |StartPushSum of Double
    |ComputePushSum of Double * Double * Double
    |Result of Double * Double
    |RecordNumPeople of int
let rnd  = System.Random(1)
type Listener() =
    inherit Actor()
    let mutable msgRecieved = 0
    let mutable numPeople =0

    override x.OnReceive(rmsg) = 
        match rmsg :?>Gossip with 
        | ReportMsgRecvd message ->
            let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
            msgRecieved <- msgRecieved + 1
            //printfn "Hearing Actor %i" msgRecieved
            if msgRecieved = numPeople then
                printfn "Hearing Actor %i" msgRecieved 
                sw.Stop()
                printfn "Time for convergence: %O" sw.Elapsed
                Environment.Exit(0)  
           

        | Result (sum,weight) ->
            printfn "Sum = %f Weight= %f Average=%f" sum weight (sum/weight)
            //sw.Stop()
            //printfn "Time for convergence: %O" sw.Elapsed
            msgRecieved <- msgRecieved + 1
            printfn "Hearing Actor %i %i" msgRecieved numOfNodes
            if msgRecieved = numOfNodes then
                sw.Stop()
                printfn "Answer! Sum = %f Weight= %f Average=%f" sum weight (sum/weight)
                printfn "Time for convergence: %O" sw.Elapsed
                Environment.Exit(0)  

        | RecordNumPeople numpeople ->
            numPeople <-numpeople
        | _->()
 
type Node(listener: IActorRef, numResend: int, nodeNum: int)=
    inherit Actor()
    let mutable numMsgHeard = 0 
    let mutable  neighbours:IActorRef[]=[||]
    let mutable  fullNeighbours:IActorRef[]=[||]
    //used for push sum
    let mutable sum1= nodeNum |> float
    let mutable weight = 1.0
    let mutable termRound = 1
    let mutable convergence = false

    
 
    override x.OnReceive(num)=
         // printfn "Avik %A"
         match num :?>Gossip with 
         | Initialize aref->
                neighbours<-aref

         | IntializeFull aref->
                fullNeighbours<-aref

         | StartGossip msg ->
                numMsgHeard<- numMsgHeard+1
                if(numMsgHeard=1) then 
                      listener <! ReportMsgRecvd(msg)
                
                if(numMsgHeard <= 10 * numOfNodes) then

                    if (topology <> "imp2D") then
                        let index= System.Random().Next(0,neighbours.Length)
                        neighbours.[index] <! StartGossip(msg)
                    else 
                        let index= System.Random().Next(0,5)
                        if(index < 4) then
                            let nindex= System.Random().Next(0,neighbours.Length)
                            neighbours.[nindex] <! StartGossip(msg)
                        else
                            let mutable fullindex = System.Random().Next(0,numOfNodes-1)
                            while(fullindex = nodeNum) do
                                fullindex <- System.Random().Next(0,numOfNodes-1)
                            fullNeighbours.[fullindex] <! StartGossip(msg)

         | StartPushSum delta -> 
                        let index= System.Random().Next(0,neighbours.Length)
                        sum1<- sum1/2.0
                        weight <-weight/2.0
                        neighbours.[index] <! ComputePushSum(sum1,weight,delta)

         | ComputePushSum (s:float,w,delta) -> 
                          let  newsum = sum1+s
                          let newweight = weight + w
                          let cal = sum1/weight - newsum/newweight |> abs
                          if(cal >delta) then
                            termRound<- 0
                            sum1 <- sum1+s
                            weight <- weight + w
                            sum1 <- sum1/2.0
                            weight <- weight/2.0
                            if (topology <> "imp2D") then
                                let index= System.Random().Next(0,neighbours.Length)
                                neighbours.[index] <! ComputePushSum(sum1,weight,delta)
                            else
                            let index= System.Random().Next(0,5)
                            if(index < 4) then
                                let nindex= System.Random().Next(0,neighbours.Length)
                                neighbours.[nindex] <! ComputePushSum(sum1,weight,delta)
                            else
                                let mutable fullindex = System.Random().Next(0,numOfNodes-1)
                                while(fullindex = nodeNum) do
                                    fullindex <- System.Random().Next(0,numOfNodes-1)
                                fullNeighbours.[fullindex] <! ComputePushSum(sum1,weight,delta)
                           //elif (termRound>=3) then
                           else
                             if (convergence=false) then
                                listener<! Result(sum1,weight)
                                convergence <- true

                            //else
                             sum1 <- sum1+s
                             weight <- weight + w
                             sum1<- sum1/2.0
                             weight <- weight/2.0
                             termRound<- termRound+1
                             if (topology <> "imp2D") then
                                let index= System.Random().Next(0,neighbours.Length)
                                neighbours.[index] <! ComputePushSum(sum1,weight,delta)
                             else
                             let index= System.Random().Next(0,5)
                             if(index < 4) then
                                let nindex= System.Random().Next(0,neighbours.Length)
                                neighbours.[nindex] <! ComputePushSum(sum1,weight,delta)
                             else
                                let mutable fullindex = System.Random().Next(0,numOfNodes-1)
                                while(fullindex = nodeNum) do
                                    fullindex <- System.Random().Next(0,numOfNodes-1)
                                fullNeighbours.[fullindex] <! ComputePushSum(sum1,weight,delta)


         | _-> ()






let system = ActorSystem.Create("System")

let mutable actualNumOfNodes=float(numOfNodes)


numOfNodes = if topology="2D" || topology="imp2D" then 
                 int(floor((actualNumOfNodes ** 0.5) ** 2.0))
             else
                 numOfNodes
          

let listener=system.ActorOf(Props.Create(typeof<Listener>),"listener")

match topology  with 
      | "full"->
          let nodeArray= Array.zeroCreate (numOfNodes+1)
          for i in [0..numOfNodes] do
              nodeArray.[i]<-system.ActorOf(Props.Create(typeof<Node>,listener,10,i+1),"demo"+string(i))
          for i in [0..numOfNodes] do
              nodeArray.[i]<!Initialize(nodeArray)
              
          let leader = System.Random().Next(0,numOfNodes)
          if protocol="gossip" then
            listener<!RecordNumPeople(numOfNodes)
            sw.Start()
            printfn "Starting Protocol Gossip"
            nodeArray.[leader]<!StartGossip("Hello")
          else if protocol="push-sum" then
            sw.Start()
            printfn "Starting Push Protocol"
            nodeArray.[leader]<!StartPushSum(10.0 ** -10.0)
      |"line"->
          let nodeArray= Array.zeroCreate (numOfNodes)
          for i in [0..numOfNodes-1] do
              nodeArray.[i]<-system.ActorOf(Props.Create(typeof<Node>,listener,10,i+1),"demo"+string(i))
          for i in [0..numOfNodes-1] do
              let mutable neighbourArray:IActorRef[] = [||]
              if i=0 then
                neighbourArray <- [|nodeArray.[i+1]|]
              else if i=numOfNodes-1 then
                neighbourArray <- [|nodeArray.[i-1]|]
              else
                neighbourArray <- [|nodeArray.[i-1];nodeArray.[i+1]|]
              
              nodeArray.[i]<!Initialize(neighbourArray)
          let leader = System.Random().Next(0,numOfNodes)
          listener<!RecordNumPeople(numOfNodes)
          if protocol="gossip" then
            sw.Start()
            printfn "Starting Protocol Gossip"
            nodeArray.[leader]<!StartGossip("This is Line Topology")
          else if protocol="push-sum" then
            sw.Start()
            printfn "Starting Push Sum Protocol for Line"
            nodeArray.[leader]<!StartPushSum(10.0 ** -10.0)

      |"2D"->
           let gridSize=int(ceil(sqrt actualNumOfNodes))
           let totGrid=gridSize*gridSize
           let nodeArray= Array.zeroCreate (totGrid)
           for i in [0..(gridSize*gridSize-1)] do
              nodeArray.[i]<-system.ActorOf(Props.Create(typeof<Node>,listener,10,i+1),"demo"+string(i))
           
           for i in [0..gridSize-1] do
               for j in [0..gridSize-1] do
                    let mutable neighbours:IActorRef[]=[||]
                    if j+1<gridSize then
                        neighbours<-(Array.append neighbours [|nodeArray.[i*gridSize+j+1]|])
                    if j-1>=0 then
                        neighbours<-Array.append neighbours [|nodeArray.[i*gridSize+j-1]|]
                    if i-1>=0 then
                        neighbours<-Array.append neighbours [|nodeArray.[(i-1)*gridSize+j]|]
                    if i+1<gridSize then
                        neighbours<-(Array.append neighbours [|nodeArray.[(i+1)*gridSize+j]|])
                    nodeArray.[i*gridSize+j]<!Initialize(neighbours)

       
               
           let leader = System.Random().Next(0,totGrid-1)  
           if protocol="gossip" then
            listener<!RecordNumPeople(totGrid-1)
            sw.Start()
            printfn "Starting Protocol Gossip"
            nodeArray.[leader]<!StartGossip("This is 2D Topology")
           else if protocol="push-sum" then
            sw.Start()
            printfn "Starting Push Sum Protocol for Line"
            nodeArray.[leader]<!StartPushSum(10.0 ** -10.0)
       |"imp2D"->
           let gridSize=int(ceil(sqrt actualNumOfNodes))
           let totGrid=gridSize*gridSize
           let nodeArray= Array.zeroCreate (totGrid)
           for i in [0..(gridSize*gridSize-1)] do
              nodeArray.[i]<-system.ActorOf(Props.Create(typeof<Node>,listener,10,i+1),"demo"+string(i))
           
           for i in [0..gridSize-1] do
               for j in [0..gridSize-1] do
                    let mutable neighbours:IActorRef[]=[||]
                    if j+1<gridSize then
                        neighbours<-(Array.append neighbours [|nodeArray.[i*gridSize+j+1]|])
                    if j-1>=0 then
                        neighbours<-Array.append neighbours [|nodeArray.[i*gridSize+j-1]|]
                    if i-1>=0 then
                        neighbours<-Array.append neighbours [|nodeArray.[(i-1)*gridSize+j]|]
                    if i+1<gridSize then
                        neighbours<-(Array.append neighbours [|nodeArray.[(i+1)*gridSize+j]|])
                    nodeArray.[i*gridSize+j]<!Initialize(neighbours)
                    nodeArray.[i*gridSize+j]<!IntializeFull(nodeArray)

           let leader = System.Random().Next(0,totGrid-1)  
           if protocol="gossip" then
            listener<!RecordNumPeople(totGrid-1)
            sw.Start()
            printfn "Starting Protocol Gossip"
            nodeArray.[leader]<!StartGossip("This is imp2D Topology")
           else if protocol="push-sum" then
            sw.Start()
            printfn "Starting Push Sum Protocol for Line"
            nodeArray.[leader]<!StartPushSum(10.0 ** -10.0)
      | _-> ()
System.Console.ReadLine()|>ignore