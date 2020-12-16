#load "references.fsx"
#time "on"


open System
open Akka.Actor
open Akka.FSharp
open System.Threading
open FSharp.Json
open WebSocketSharp
open Akka.Configuration


// number of user
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : OFF
            loglevel : OFF
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 8555
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("TwitterSim", configuration)
let echoServer = new WebSocket("ws://localhost:8080/websocket")
echoServer.OnOpen.Add(fun args -> System.Console.WriteLine("Open"))
echoServer.OnClose.Add(fun args -> System.Console.WriteLine("Close"))
echoServer.OnMessage.Add(fun args -> System.Console.WriteLine("Msg: {0}", args.Data))
echoServer.OnError.Add(fun args -> System.Console.WriteLine("Error: {0}", args.Message))

echoServer.Connect()


let mutable clientsCount = 0
let mutable lastUserSubscribed = false
clientsCount<-0
let IncrementCount (mailbox: Actor<_>)=
    let rec loop () = actor {
        let! message = mailbox.Receive ()
        clientsCount <- clientsCount + 1
        return! loop()     
    }
    loop ()

let incrementCount = spawn system "incrementCount" IncrementCount
type MessageType = {
    OperationName : string
    UserName : string
    Password : string
    SubscribeUserName : string
    TweetData : string
    Queryhashtag : string
    QueryAt : string
}


let TwitterClient (mailbox: Actor<string>)=
    let mutable userName = ""
    let mutable password = ""

    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        
        let result = message.Split ','
        let operation = result.[0]

        if operation = "Register" then
            userName <- result.[1]
            password <- result.[2]
            //let serverOp = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "

            let serverJson: MessageType = {OperationName = "reg"; UserName = userName; Password = password; SubscribeUserName = ""; TweetData = ""; Queryhashtag = ""; QueryAt = ""} 
            let json = Json.serialize serverJson
            //echoServer <! serverOp
            echoServer.Send json
            //printfn "[command]%s" serverOp
            incrementCount <! 1
            return! loop()  
        else if operation = "Subscribe" then
            //let serverOp = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            //echoServer.Send serverOp

            let serverJson: MessageType = {OperationName = "subscribe"; UserName = userName; Password = password; SubscribeUserName = result.[1]; TweetData = ""; Queryhashtag = ""; QueryAt = ""} 
            let json = Json.serialize serverJson
            echoServer.Send json
            //echoServer <! serverOp
        else if operation = "SendTweet" then
            let serverJson: MessageType = {OperationName = "send"; UserName = userName; Password = password; SubscribeUserName = ""; TweetData = "tweet from "+userName+" "+result.[1]; Queryhashtag = ""; QueryAt = ""} 
            let json = Json.serialize serverJson
            echoServer.Send json
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore
        else if operation = "RecievedTweet" then
            printfn "[%s] : %s" userName result.[1]  
        else if operation = "Querying" then
          

            let serverJson: MessageType = {OperationName = "querying"; UserName = userName; Password = password; SubscribeUserName = ""; TweetData = ""; Queryhashtag = ""; QueryAt = ""} 
            let json = Json.serialize serverJson
            echoServer.Send json
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "Logout" then
            //echoServer <! "logout, ,"+userName+","+password+", , , , , "

            let serverJson: MessageType = {OperationName = "logout"; UserName = userName; Password = password; SubscribeUserName = ""; TweetData = ""; Queryhashtag = ""; QueryAt = ""} 
            let json = Json.serialize serverJson
            echoServer.Send json
        else if operation = "QueryHashtags" then
        
            let serverJson: MessageType = {OperationName = "#"; UserName = userName; Password = password; SubscribeUserName = ""; TweetData = ""; Queryhashtag = result.[1]; QueryAt = ""} 
            let json = Json.serialize serverJson
            echoServer.Send json
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "QueryMentions" then
            
            let serverJson: MessageType = {OperationName = "@"; UserName = userName; Password = password; SubscribeUserName = ""; TweetData = ""; Queryhashtag =""; QueryAt =  result.[1]} 
            let json = Json.serialize serverJson
            echoServer.Send json
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        return! loop()     
    }
    loop ()
//
(*
let clients = Array.zeroCreate (10)
for id in 0..1 do
    clients.[id] <- spawn system ("User"+(string id)) TwitterClient

let username = "User0" 
clients.[0] <! "Register,"+username+",password"


let mutable a = 0
while true do
    Thread.Sleep(5000)
    clients.[0] <! "SendTweet,"+(string 1)
    a <- a + 1
*)
// Register,username,password

let client = spawn system ("User"+(string 1)) TwitterClient
let rec readInput () =
    Console.Write("Enter command: ")
    let input = Console.ReadLine()
    let inpMessage = input.Split ','
    let serverOp = inpMessage.[0]
    
    match (serverOp) with
    | "Register" -> 
        let username = inpMessage.[1]
        let password = inpMessage.[2]    
        client <! "Register,"+username+","+password
        readInput()
    | "Subscribe" ->
        let username = inpMessage.[1] 
        client <! "Subscribe,"+username
        readInput()
    | "Send" ->
        let message = inpMessage.[1] 
        client <! "SendTweet,"+message
        readInput()
    | "Query" ->
        client <! "Querying"
        readInput()
    | "QueryHashtag" ->
        client <! "QueryHashtags,"+inpMessage.[1]
        readInput()
    | "QueryMention" ->
        client <! "QueryMentions,"+inpMessage.[1]
        readInput()
    | "Exit" ->
        printfn "Exiting Client!"
    | _ -> 
        printfn "Invalid Input, Please refer the Report"
        readInput()


readInput()
(*printfn "*************************************" 
printfn "Starting Client registrations!   " 
printfn "*************************************"
let stopwatch = System.Diagnostics.Stopwatch.StartNew()
i<-0

while i<numberOfClients-1 do
    let username = "User" + (string i)
    //clients.[i] <! Register(username,"password")
    clients.[i] <! "Register,"+username+",password"
    i<-i+1

while clientsCount<numberOfClients-2 do
    Thread.Sleep(50)

//clients.[i] <! SyncRegister("User" + (string (numberOfClients-1)),"password")
clients.[i] <! "SyncRegister,"+"User" + (string (numberOfClients-1))+",password"
while clientsCount<numberOfClients-1 do
    Thread.Sleep(50)
stopwatch.Stop()

let timeRegister = stopwatch.Elapsed.TotalMilliseconds


printfn "*************************************" 
printfn "Starting subscriptions based on Zipf distribution!   " 
printfn "*************************************"
let mutable step = 1
let subsStopwatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..numberOfClients-1 do
    for j in 0..step..numberOfClients-1 do
        if j<>i then
                let serverOp = "SyncSubscribe,"+"User" + (string (i))
                let task = clients.[j] <? serverOp
                Async.RunSynchronously (task, 10000) |> ignore
    step <- step+1

let timeZipfSubscribe = subsStopwatch.Elapsed.TotalMilliseconds


let clientsLoggedOut = numberOfClients - ((numberOfClients*logoutPercentage)/100)
for i in 0..clientsLoggedOut do
    clients.[random.Next(numberOfClients)] <! "Logout"

printfn "*************************************" 
printfn "Starting sending of tweets from each user   " 
printfn "*************************************"
let sendStopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..numberOfClients-1 do
    for j in 0..1 do
        let serverOp = "SendTweet,"+(string j)
        let task = clients.[i] <? serverOp
        Async.RunSynchronously (task, 10000) |> ignore
        (*let response = Async.RunSynchronously (task, 10000)
        printfn "[Reply]%s" (string(response))
        printfn "%s" ""*)
sendStopWatch.Stop()
let timeSend = sendStopWatch.Elapsed.TotalMilliseconds


printfn "*************************************" 
printfn "Now querying tweets for each user " 
printfn "*************************************"
let queryStopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..numberOfClients-1 do
    let serverOp = "Querying"
    let task = clients.[i] <? serverOp
    Async.RunSynchronously (task, 10000) |> ignore
    (*let response = Async.RunSynchronously (task, 10000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""*)
sendStopWatch.Stop()
let queryTime = queryStopWatch.Elapsed.TotalMilliseconds


printfn "*************************************" 
printfn "Now querying HashTags for each user " 
printfn "*************************************"
let queryHash = System.Diagnostics.Stopwatch.StartNew()
for i in 0..numberOfClients-1 do
    let serverOp = "QueryHashtags"
    let task = clients.[i] <? serverOp
    Async.RunSynchronously (task, 10000) |> ignore
    (*let response = Async.RunSynchronously (task, 10000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""*)
sendStopWatch.Stop()
let hashTime = queryHash.Elapsed.TotalMilliseconds

printfn "*************************************" 
printfn "Now querying Mentions for each user " 
printfn "*************************************"
let queryMentions = System.Diagnostics.Stopwatch.StartNew()
for i in 0..numberOfClients-1 do
    let serverOp = "QueryMentions"
    let task = clients.[i] <? serverOp
    Async.RunSynchronously (task, 10000) |> ignore
    (*let response = Async.RunSynchronously (task, 10000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""*)
sendStopWatch.Stop()
let mentionsTime = queryMentions.Elapsed.TotalMilliseconds

printfn "*************************************" 
printfn "Now performing random actions for each user " 
printfn "*************************************"
let randomWatch = System.Diagnostics.Stopwatch.StartNew()

for i in 0..numberOfRandomOps-1 do
    let serverOp = "RandomOp"
    let task = clients.[i] <? serverOp
    Async.RunSynchronously (task, 10000) |> ignore
    //let response = Async.RunSynchronously (task, 10000)
    //printfn "[Reply]%s" (string(response))
    //printfn "%s" ""
randomWatch.Stop()
let timeRandom = randomWatch.Elapsed.TotalMilliseconds

printfn "Time to register all users %f" timeRegister
printfn "Time to Zipf subscribe %f" timeZipfSubscribe
printfn "Time to Send Tweets %f" timeSend
printfn "Time to Query Tweets %f" queryTime
printfn "Time to Query HashTags %f" hashTime
printfn "Time to Query Mentions %f" mentionsTime
printfn "Time to perform Random Ops %f" timeRandom
*)

system.Terminate() |> ignore
0 