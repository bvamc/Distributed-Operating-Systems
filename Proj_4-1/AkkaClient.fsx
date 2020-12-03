#load "references.fsx"
#time "on"

open System
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

// number of user
let args : string array = fsi.CommandLineArgs |> Array.tail
let N= args.[0] |> int
let M = N
let mutable i = 0
let logoutPercentage = 95

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
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
                    port = 8123
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("RemoteFSharp", configuration)
let echoServer = system.ActorSelection(
                            "akka.tcp://RemoteFSharp@localhost:8777/user/EchoServer")
let rand = System.Random(1)

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
            let cmd = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            echoServer <! cmd
            //printfn "[command]%s" cmd
            incrementCount <! 1
            return! loop()  
        else if operation = "SyncRegister" then
            userName <- result.[1]
            password <- result.[2]
            let cmd = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            //incrementCount <! 1
            return! loop()
        else if operation = "RandomOp" then
            let mutable rand_num = Random( ).Next() % 7
            let mutable opt = "logout"           
            let mutable POST = "POST"
            let mutable target_username = "user"+rand.Next(N) .ToString()
            let mutable queryhashtag = "#topic"+rand.Next(N) .ToString()
            let mutable at = "@user"+rand.Next(N) .ToString()
            let mutable tweet_content = "tweet"+rand.Next(N) .ToString()+"... " + queryhashtag + "..." + at + " " 
            let mutable register = "register"
            if rand_num=0 then  opt <-"logout"
            if rand_num=1 then  opt <-"send"
            if rand_num=2 then  opt <-"subscribe"
            if rand_num=3 then  opt <-"retweet"
            if rand_num=4 then  opt <-"querying"
            if rand_num=5 then  opt <-"#"
            if rand_num=6 then  opt <-"@" 
            // msg can be anything like "start"
            let cmd = opt+","+POST+","+userName+","+password+","+target_username+","+tweet_content+","+queryhashtag+","+at+","+register
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
           
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "Subscribe" then
            let cmd = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            echoServer <! cmd
        else if operation = "SyncSubscribe" then
            let cmd = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            lastUserSubscribed <- true
            sender <? "success" |> ignore 
        else if operation = "SendTweet" then
            let cmd = "send, ,"+userName+","+password+", ,tweet from "+userName+"_"+result.[1]+"th @user"+(string (rand.Next(N)))+" #topic"+(string (rand.Next(N)))+" , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore
        else if operation = "RecievedTweet" then
            printfn "[%s] : %s" userName result.[1]  
        else if operation = "Querying" then
            let cmd = "querying, ,"+userName+","+password+", , , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "Logout" then
            echoServer <! "logout, ,"+userName+","+password+", , , , , "
        else if operation = "QueryHashtags" then
            let cmd = "#, , , , , ,#topic"+(string (rand.Next(N)))+", ,"
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "QueryMentions" then
            let cmd = "@, , , , , , ,@user"+(string (rand.Next(N)))+","
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 10000)
            //printfn "[command]%s" cmd
            //printfn "[Reply]%s" (string(response))
            //printfn "%s" ""
            sender <? "success" |> ignore 
        return! loop()     
    }
    loop ()
//
let clients = Array.zeroCreate (N + 1)
for id in 0..N do
    clients.[id] <- spawn system ("User"+(string id)) TwitterClient

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Register Account...   " 
printfn "-------------------------------------------------   "
let stopwatch = System.Diagnostics.Stopwatch.StartNew()
i<-0

while i<N-1 do
    let username = "User" + (string i)
    //clients.[i] <! Register(username,"password")
    clients.[i] <! "Register,"+username+",password"
    i<-i+1

while clientsCount<N-2 do
    Thread.Sleep(50)

//clients.[i] <! SyncRegister("User" + (string (N-1)),"password")
clients.[i] <! "SyncRegister,"+"User" + (string (N-1))+",password"
while clientsCount<N-1 do
    Thread.Sleep(50)
stopwatch.Stop()

let timeRegister = stopwatch.Elapsed.TotalMilliseconds


printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Zipf Subscribe...   " 
printfn "-------------------------------------------------   "
let mutable step = 1
let subsStopwatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    for j in 0..step..N-1 do
        if j<>i then
            //if i<> N-1 then
                //clients.[j] <! Subscribe("User" + (string (i)))
                //clients.[j] <! "Subscribe,"+"User" + (string (i))
            //else
                //clients.[j] <! SyncSubscribe("User" + (string (i)))
                //clients.[j] <! "SyncSubscribe,"+"User" + (string (i))
                let cmd = "SyncSubscribe,"+"User" + (string (i))
                let task = clients.[j] <? cmd
                Async.RunSynchronously (task, 10000) |> ignore
    step <- step+1

while not lastUserSubscribed do
    Thread.Sleep(50)
subsStopwatch.Stop()

let timeZipfSubscribe = subsStopwatch.Elapsed.TotalMilliseconds


let clientsLoggedOut = N - ((N*logoutPercentage)/100)
for i in 0..clientsLoggedOut do
    clients.[rand.Next(N)] <! "Logout"

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "send tweet...   " 
printfn "-------------------------------------------------   "
let sendStopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    for j in 0..1 do
        let cmd = "SendTweet,"+(string j)
        let task = clients.[i] <? cmd
        Async.RunSynchronously (task, 10000) |> ignore
        (*let response = Async.RunSynchronously (task, 10000)
        printfn "[Reply]%s" (string(response))
        printfn "%s" ""*)
sendStopWatch.Stop()
let timeSend = sendStopWatch.Elapsed.TotalMilliseconds


printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Query tweets...   " 
printfn "-------------------------------------------------   "
let queryStopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "Querying"
    let task = clients.[i] <? cmd
    Async.RunSynchronously (task, 10000) |> ignore
    (*let response = Async.RunSynchronously (task, 10000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""*)
sendStopWatch.Stop()
let queryTime = queryStopWatch.Elapsed.TotalMilliseconds


printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Query HashTags...   " 
printfn "-------------------------------------------------   "
let queryHash = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "QueryHashtags"
    let task = clients.[i] <? cmd
    Async.RunSynchronously (task, 10000) |> ignore
    (*let response = Async.RunSynchronously (task, 10000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""*)
sendStopWatch.Stop()
let hashTime = queryHash.Elapsed.TotalMilliseconds

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Query Mentions...   " 
printfn "-------------------------------------------------   "
let queryMentions = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "QueryMentions"
    let task = clients.[i] <? cmd
    Async.RunSynchronously (task, 10000) |> ignore
    (*let response = Async.RunSynchronously (task, 10000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""*)
sendStopWatch.Stop()
let mentionsTime = queryMentions.Elapsed.TotalMilliseconds

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn " %d Randon Ops ...   " 
printfn "-------------------------------------------------   "
let randomWatch = System.Diagnostics.Stopwatch.StartNew()

for i in 0..M-1 do
    let cmd = "RandomOp"
    let task = clients.[i] <? cmd
    Async.RunSynchronously (task, 10000) |> ignore
    //let response = Async.RunSynchronously (task, 10000)
    //printfn "[Reply]%s" (string(response))
    //printfn "%s" ""
randomWatch.Stop()
let timeRandom = randomWatch.Elapsed.TotalMilliseconds

printfn "Time to register %f" timeRegister
printfn "Time to Zipf subscribe %f" timeZipfSubscribe
printfn "Time to Send Tweets %f" timeSend
printfn "Time to Query Tweets %f" queryTime
printfn "Time to Query HashTags %f" hashTime
printfn "Time to Query Mentions %f" mentionsTime
printfn "Time to perform Random Ops %f" timeRandom

system.Terminate() |> ignore
0 