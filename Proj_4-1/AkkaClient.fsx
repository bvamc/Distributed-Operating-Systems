#load "references.fsx"
#time "on"

open System
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.FSharp


type MessagePack_processor = MessagePack8 of  string  * string * string* string* string * string* string* string * string

// number of user
let args : string array = fsi.CommandLineArgs |> Array.tail
let N= args.[0] |> int
let M = N
let mutable i = 0
let logoutPercentage = 70

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

(*let obj = new Object()
let addIIByOne() =
    Monitor.Enter obj
    ii<- ii+1
    Monitor.Exit obj
    

let actor_user_register (mailbox: Actor<_>) = 
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let idx = message
        let mutable opt = "reg"           
        let mutable POST = " "
        let mutable username = "user"+(string idx)
        let mutable password = "password" + (string idx)
        let mutable target_username = " "
        let mutable queryhashtag = " "
        let mutable at = " "
        let mutable tweet_content = " "
        let mutable register = " "
        let cmd = opt+","+POST+","+username+","+password+","+target_username+","+tweet_content+","+queryhashtag+","+at+","+register
        let task = echoServer <? cmd
        let response = Async.RunSynchronously (task, 1000)
        printfn "[command]%s" cmd
        printfn "[Reply]%s" (string(response))
        printfn "%s" ""
        addIIByOne()
        return! loop()
    }
    loop ()

let actor_client_simulator (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        let idx = message
        match box message with
        | :? string   ->
            let mutable rand_num = Random( ).Next() % 7
            let mutable opt = "reg"           
            let mutable POST = "POST"
            let mutable username = "user"+(string idx)
            let mutable password = "password" + (string idx)
            let mutable target_username = "user"+rand.Next(N) .ToString()
            let mutable queryhashtag = "#topic"+rand.Next(N) .ToString()
            let mutable at = "@user"+rand.Next(N) .ToString()
            let mutable tweet_content = "tweet"+rand.Next(N) .ToString()+"... " + queryhashtag + "..." + at + " " 
            let mutable register = "register"
            if rand_num=0 then  opt <-"reg"
            if rand_num=1 then  opt <-"send"
            if rand_num=2 then  opt <-"subscribe"
            if rand_num=3 then  opt <-"retweet"
            if rand_num=4 then  opt <-"querying"
            if rand_num=5 then  opt <-"#"
            if rand_num=6 then  opt <-"@" 
            // msg can be anything like "start"
            let cmd = opt+","+POST+","+username+","+password+","+target_username+","+tweet_content+","+queryhashtag+","+at+","+register
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            addIIByOne()
        return! loop()     
    }
    loop ()

let client_user_register = spawn system "client_user_register" actor_user_register    
let client_simulator = spawn system "client_simulator" actor_client_simulator



printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Register Account...   " 
printfn "-------------------------------------------------   "

let stopWatch = System.Diagnostics.Stopwatch.StartNew()
i<-0
ii<-0
while i<N do
    client_user_register <! string i |>ignore
    i<-i+1
while ii<N-1 do
    Thread.Sleep(50)
stopWatch.Stop()
let time_register = stopWatch.Elapsed.TotalMilliseconds
//    Thread.Sleep(5000)




printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "send tweet...   " 
printfn "-------------------------------------------------   "
stopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    for j in 0..10 do
        let cmd = "send, ,user"+(string i)+",password"+(string i)+", ,tweet+user"+(string i)+"_"+(string j)+"th @user"+(string (rand.Next(N)))+" #topic"+(string (rand.Next(N)))+" , , , "
//            let cmd = "send, ,user"+(string i)+",password"+(string i)+", ,@user"+(string (rand.Next(N)))+" #topic"+(string (rand.Next(N)))+" , , , "
//            let cmd = "send, ,user"+(string i)+",password"+(string i)+", ,t, , , "
        let task = echoServer <? cmd
        let response = Async.RunSynchronously (task, 3000)
        printfn "[command]%s" cmd
        printfn "[Reply]%s" (string(response))
        printfn "%s" ""
stopWatch.Stop()
let time_send = stopWatch.Elapsed.TotalMilliseconds





let mutable step = 1
stopWatch = System.Diagnostics.Stopwatch.StartNew()
printfn "Zipf Subscribe ----------------------------------"  
for i in 0..N-1 do
    for j in 0..step..N-1 do
        if not (j=i) then
            let cmd = "subscribe, ,user"+(string j)+",password"+(string j)+",user"+(string i)+", , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
        step <- step+1
stopWatch.Stop()
let time_zipf_subscribe = stopWatch.Elapsed.TotalMilliseconds
    


stopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "querying, ,user"+(string i)+",password"+(string i)+", , , , , "
    let task = echoServer <? cmd
    let response = Async.RunSynchronously (task, 5000)
    printfn "[command]%s" cmd
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
stopWatch.Stop()
let time_query = stopWatch.Elapsed.TotalMilliseconds



stopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "#, , , , , ,#topic"+(string (rand.Next(N)))+", ,"
    let task = echoServer <? cmd
    let response = Async.RunSynchronously (task, 3000)
    printfn "[command]%s" cmd
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
stopWatch.Stop()
let time_hashtag = stopWatch.Elapsed.TotalMilliseconds




stopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "@, , , , , , ,@user"+(string (rand.Next(N)))+","
    let task = echoServer <? cmd
    let response = Async.RunSynchronously (task, 3000)
    printfn "[command]%s" cmd
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
stopWatch.Stop()
let time_mention = stopWatch.Elapsed.TotalMilliseconds




printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn " %d Randon Ops and send tweet...   " M 
printfn "-------------------------------------------------   "
stopWatch = System.Diagnostics.Stopwatch.StartNew()
i<-0
ii<-0
while i<M do
    client_simulator<! string (rand.Next(N)) |>ignore
    i <- i+1
while ii<M-1 do
    Thread.Sleep(50)
stopWatch.Stop()
let time_random = stopWatch.Elapsed.TotalMilliseconds


printfn "The time of register %d users is %f" N time_register
printfn "The time of send 10 tweets is %f" time_send
printfn "The time of Zipf subscribe %d users is %f" N time_zipf_subscribe
printfn "The time of query %d users is %f" N time_query
printfn "The time of query %d hasgtag is %f" N time_hashtag
printfn "The time of query %d mention is %f" N time_mention
printfn "The time of %d random operations is %f" M time_random

printfn "Total Result: %f %f %f %f %f %f %f" time_register time_send time_zipf_subscribe time_query time_hashtag time_mention time_random
*)
//Client Re-Write

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

//
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
            printfn "[command]%s" cmd
            incrementCount <! 1
            return! loop()  
        else if operation = "SyncRegister" then
            userName <- result.[1]
            password <- result.[2]
            let cmd = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 1000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            incrementCount <! 1
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
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
           
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "Subscribe" then
            let cmd = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            echoServer <! cmd
        else if operation = "SyncSubscribe" then
            let cmd = "subscribe, ,"+userName+","+password+","+result.[1]+", , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            lastUserSubscribed <- true
        else if operation = "SendTweet" then
            let cmd = "send, ,"+userName+","+password+", ,tweet from "+userName+"_"+result.[1]+"th @user"+(string (rand.Next(N)))+" #topic"+(string (rand.Next(N)))+" , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            sender <? "success" |> ignore
        else if operation = "RecievedTweet" then
            printfn "[%s] : %s" userName result.[1]  
        else if operation = "Querying" then
            let cmd = "querying, ,"+userName+","+password+", , , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 5000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "Logout" then
            echoServer <! "logout, ,"+userName+","+password+", , , , , "
        else if operation = "QueryHashtags" then
            let cmd = "#, , , , , ,#topic"+(string (rand.Next(N)))+", ,"
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            sender <? "success" |> ignore 
        else if operation = "QueryMentions" then
            let cmd = "@, , , , , , ,@user"+(string (rand.Next(N)))+","
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
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

printfn "Time to register %f" timeRegister


printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Zipf Subscribe...   " 
printfn "-------------------------------------------------   "
let mutable step = 1
let subsStopwatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    for j in 0..step..N-1 do
        if j<>i then
            if i<> N-1 then
                //clients.[j] <! Subscribe("User" + (string (i)))
                clients.[j] <! "Subscribe,"+"User" + (string (i))
            else
                //clients.[j] <! SyncSubscribe("User" + (string (i)))
                clients.[j] <! "SyncSubscribe,"+"User" + (string (i))
    step <- step+1

while not lastUserSubscribed do
    Thread.Sleep(50)
subsStopwatch.Stop()

let timeZipfSubscribe = subsStopwatch.Elapsed.TotalMilliseconds
printfn "Time to Zipf subscribe %f" timeZipfSubscribe

let clientsLoggedOut = N - ((N*logoutPercentage)/100)
for i in 0..clientsLoggedOut do
    clients.[rand.Next(N)] <! "Logout"

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "send tweet...   " 
printfn "-------------------------------------------------   "
let sendStopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    for j in 0..10 do
        let cmd = "SendTweet,"+(string j)
        let task = clients.[i] <? cmd
        let response = Async.RunSynchronously (task, 3000)
        printfn "[Reply]%s" (string(response))
        printfn "%s" ""
sendStopWatch.Stop()
let timeSend = sendStopWatch.Elapsed.TotalMilliseconds
printfn "Time to Send Tweets %f" timeSend

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Query tweets...   " 
printfn "-------------------------------------------------   "
let queryStopWatch = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "Querying"
    let task = clients.[i] <? cmd
    let response = Async.RunSynchronously (task, 3000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
sendStopWatch.Stop()
let queryTime = queryStopWatch.Elapsed.TotalMilliseconds
printfn "Time to Query Tweets %f" queryTime

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Query HashTags...   " 
printfn "-------------------------------------------------   "
let queryHash = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "QueryHashtags"
    let task = clients.[i] <? cmd
    let response = Async.RunSynchronously (task, 3000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
sendStopWatch.Stop()
let hashTime = queryHash.Elapsed.TotalMilliseconds
printfn "Time to Query HashTags %f" hashTime

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Query Mentions...   " 
printfn "-------------------------------------------------   "
let queryMentions = System.Diagnostics.Stopwatch.StartNew()
for i in 0..N-1 do
    let cmd = "QueryMentions"
    let task = clients.[i] <? cmd
    let response = Async.RunSynchronously (task, 3000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
sendStopWatch.Stop()
let mentionsTime = queryMentions.Elapsed.TotalMilliseconds
printfn "Time to Query Mentions %f" mentionsTime

printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn " %d Randon Ops ...   " 
printfn "-------------------------------------------------   "
let randomWatch = System.Diagnostics.Stopwatch.StartNew()

for i in 0..M-1 do
    let cmd = "RandomOp"
    let task = clients.[i] <? cmd
    let response = Async.RunSynchronously (task, 3000)
    printfn "[Reply]%s" (string(response))
    printfn "%s" ""
randomWatch.Stop()
let timeRandom = randomWatch.Elapsed.TotalMilliseconds
printfn "Time to perform Random Ops %f" timeRandom

system.Terminate() |> ignore
0 // return an integer exit code

(*
let TwitterClient (mailbox: Actor<_>)=
    let mutable userName = ""
    let mutable password = ""

    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        printfn "mmmmmm %O" message
        match message with
        | Register (uname, pword) ->
            userName <- uname
            password <- pword
            let cmd = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            echoServer <! cmd
            printfn "[command]%s" cmd
            incrementCount <! 1
            return! loop()  
        | SyncRegister (uname, pword) ->
            userName <- uname
            password <- pword
            let cmd = "reg"+","+" "+","+userName+","+password+","+" "+","+" "+","+" "+","+" "+","+" "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 1000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            incrementCount <! 1
            return! loop()     
        | RandomOp ->
            let mutable rand_num = Random( ).Next() % 7
            let mutable opt = "reg"           
            let mutable POST = "POST"
            let mutable target_username = "user"+rand.Next(N) .ToString()
            let mutable queryhashtag = "#topic"+rand.Next(N) .ToString()
            let mutable at = "@user"+rand.Next(N) .ToString()
            let mutable tweet_content = "tweet"+rand.Next(N) .ToString()+"... " + queryhashtag + "..." + at + " " 
            let mutable register = "register"
            if rand_num=0 then  opt <-"reg"
            if rand_num=1 then  opt <-"send"
            if rand_num=2 then  opt <-"subscribe"
            if rand_num=3 then  opt <-"retweet"
            if rand_num=4 then  opt <-"querying"
            if rand_num=5 then  opt <-"#"
            if rand_num=6 then  opt <-"@" 
            // msg can be anything like "start"
            let cmd = opt+","+POST+","+userName+","+password+","+target_username+","+tweet_content+","+queryhashtag+","+at+","+register
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
        | Subscribe subUserName -> 
            let cmd = "subscribe, ,"+userName+","+password+","+subUserName+", , , , "
            echoServer <! cmd
        | SyncSubscribe subUserName -> 
            let cmd = "subscribe, ,"+userName+","+password+","+subUserName+", , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
            lastUserSubscribed <- true
        | SendTweet count-> 
            let cmd = "send, ,"+userName+","+password+", ,tweet from "+userName+"_"+(string count)+"th @user"+(string (rand.Next(N)))+" #topic"+(string (rand.Next(N)))+" , , , "
            let task = echoServer <? cmd
            let response = Async.RunSynchronously (task, 3000)
            printfn "[command]%s" cmd
            printfn "[Reply]%s" (string(response))
            printfn "%s" ""
        | RecieveTweet tweet ->
            printfn "Tweet %s" tweet

        return! loop()     
    }
    loop ()
*)