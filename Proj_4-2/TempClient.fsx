#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.FSharp
open System.Threading
open WebSocketSharp

// number of user
let args : string array = fsi.CommandLineArgs |> Array.tail
let numberOfClients= args.[0] |> int
let numberOfRandomOps = numberOfClients
let mutable i = 0
let logoutPercentage = 95


let system = ActorSystem.Create("TwitterSim")

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
let random = System.Random(1)
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
            let serverOp = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            //echoServer <! serverOp
            echoServer.Send serverOp
            //printfn "[command]%s" serverOp
            incrementCount <! 1
            return! loop()  
        else if operation = "SyncRegister" then
            userName <- result.[1]
            password <- result.[2]
            let serverOp = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            echoServer.Send serverOp
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            //incrementCount <! 1
            return! loop()
        else if operation = "RandomOp" then
            let mutable chooseOperation = Random( ).Next() % 7
            let mutable operation = "logout"           
            let mutable httpOperation = "POST"
            let mutable subscribeToUser = "user"+random.Next(numberOfClients) .ToString()
            let mutable queryhashtag = "#topic"+random.Next(numberOfClients) .ToString()
            let mutable at = "@user"+random.Next(numberOfClients) .ToString()
            let mutable tweetData = "tweet"+random.Next(numberOfClients) .ToString()+"... " + queryhashtag + "..." + at + " " 
            let mutable register = "register"
            if chooseOperation=0 then  operation <-"#"
            if chooseOperation=1 then  operation <-"retweet"
            if chooseOperation=2 then  operation <-"subscribe"
            if chooseOperation=3 then  operation <-"send"
            if chooseOperation=4 then  operation <-"@"
            if chooseOperation=5 then  operation <-"logout"
            if chooseOperation=6 then  operation <-"querying" 
            let serverOp = operation+","+httpOperation+","+userName+","+password+","+subscribeToUser+","+tweetData+","+queryhashtag+","+at+","+register
            echoServer.Send serverOp
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            sender <? "success" |> ignore 
        else if operation = "Subscribe" then
            let serverOp = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            echoServer.Send serverOp
            //echoServer <! serverOp
        else if operation = "SyncSubscribe" then
            let serverOp = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            //let task = echoServer <? serverOp
            echoServer.Send serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            lastUserSubscribed <- true
            sender <? "success" |> ignore 
        else if operation = "SendTweet" then
            let serverOp = "send, ,"+userName+","+password+", ,tweet from "+userName+"_"+result.[1]+"th @user"+(string (random.Next(numberOfClients)))+" #topic"+(string (random.Next(numberOfClients)))+" , , , "
            echoServer.Send serverOp
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore
        else if operation = "RecievedTweet" then
            printfn "[%s] : %s" userName result.[1]  
        else if operation = "Querying" then
            let serverOp = "querying, ,"+userName+","+password+", , , , , "
            echoServer.Send serverOp
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "Logout" then
            //echoServer <! "logout, ,"+userName+","+password+", , , , , "
            echoServer.Send ("logout, ,"+userName+","+password+", , , , , ")
        else if operation = "QueryHashtags" then
            let serverOp = "#, , , , , ,#topic"+(string (random.Next(numberOfClients)))+", ,"
            echoServer.Send serverOp
            //let task = echoServer <? serverOp
            //let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" serverOp
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "QueryMentions" then
            let serverOp = "@, , , , , , ,@user"+(string (random.Next(numberOfClients)))+","
            echoServer.Send serverOp
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
let clients = Array.zeroCreate (10)
for id in 0..2 do
    clients.[id] <- spawn system ("User"+(string id)) TwitterClient

let username = "User1" 
clients.[0] <! "Register,"+username+",password"
Thread.Sleep(100)
clients.[0] <! "SyncSubscribe,"+"User" + (string (0))
let mutable a = 0
while true do
    a <- a + 1
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