#load "references.fsx"
#time "on"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

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
                    port = 8777
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("RemoteFSharp", configuration)

type Tweet(tweetId:string, text:string, isRetweet:bool) =
    member this.TweetId = tweetId
    member this.Text = text
    member this.IsReTweet = isRetweet

    override this.ToString() =
      let mutable res = ""
      if isRetweet then
        res <- sprintf "[retweet][%s]%s" this.TweetId this.Text
      else
        res <- sprintf "[%s]%s" this.TweetId this.Text
      res

type User(userName:string, password:string) =
    let mutable subscribes = List.empty: User list
    let mutable subscribers = List.empty: User list
    let mutable tweets = List.empty: Tweet list
    let mutable loggedIn = true
    member this.UserName = userName
    member this.Password = password
    member this.AddSubscribe x =
        subscribes <- List.append subscribes [x]
    member this.AddSubscriber x =
        subscribers <- List.append subscribers [x]
    member this.GetSubscribes() =
        subscribes
    member this.GetSubscribers() =
        subscribers
    member this.AddTweet x =
        tweets <- List.append tweets [x]
    member this.GetTweets() =
        tweets
    member this.Logout() =
        loggedIn <- false
    member this.IsLoggedIn() = 
        loggedIn
    override this.ToString() = 
       this.UserName

type Twitter() =
    let mutable tweets = new Map<string,Tweet>([])
    let mutable users = new Map<string,User>([])
    let mutable hashtags = new Map<string, Tweet list>([])
    let mutable mentions = new Map<string, Tweet list>([])
    member this.AddTweet (tweet:Tweet) =
        tweets <- tweets.Add(tweet.TweetId,tweet)
    member this.AddUser (user:User) =
        users <- users.Add(user.UserName, user)
    member this.AddToHashTag hashtag tweet =
        let key = hashtag
        let mutable map = hashtags
        if not (map.ContainsKey(key)) then
            let l = List.empty: Tweet list
            map <- map.Add(key, l)
        let value = map.[key]
        map <- map.Add(key, List.append value [tweet])
        hashtags <- map
    member this.AddToMention mention tweet = 
        let key = mention
        let mutable map = mentions
        if not (map.ContainsKey(key)) then
            let l = List.empty: Tweet list
            map <- map.Add(key, l)
        let value = map.[key]
        map <- map.Add(key, List.append value [tweet])
        mentions <- map
    member this.register username password =
        let mutable res = ""
        if users.ContainsKey(username) then
            res <- "error, username already exist"
        else
            let user = User(username, password)
            this.AddUser user
            user.AddSubscribe user
            res <- "Register success username: " + username + "  password: " + password
        res
    member this.SendTweet username password text isRetweet =
        let mutable res = ""
        if not (this.Authentication username password) then
            res <- "error, authentication fail"
        else
            if not (users.ContainsKey(username))then
                res <-  "error, no this username"
            else
                let tweet = Tweet(System.DateTime.Now.ToFileTimeUtc() |> string, text, isRetweet)
                let user = users.[username]
                user.AddTweet tweet
                this.AddTweet tweet

                for subUser in user.GetSubscribers() do
                    if subUser.IsLoggedIn() then
                        let subUserActor = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8123/user/"+subUser.ToString())
                        subUserActor <! "RecievedTweet,"+tweet.Text
                let idx1 = text.IndexOf("#")
                if idx1 <> -1 then
                    let idx2 = text.IndexOf(" ",idx1)
                    let hashtag = text.[idx1..idx2-1]
                    this.AddToHashTag hashtag tweet
                let idx1 = text.IndexOf("@")
                if idx1 <> -1 then
                    let idx2 = text.IndexOf(" ",idx1)
                    let mention = text.[idx1..idx2-1]
                    this.AddToMention mention tweet
                res <-  "[success] sent twitter: " + tweet.ToString()
        res
    member this.Authentication username password =
            let mutable res = false
            if not (users.ContainsKey(username)) then
                printfn "%A" "error, no this username"
            else
                let user = users.[username]
                if user.Password = password then
                    res <- true
            res
    member this.GetUser username = 
        let mutable res = User("","")
        if not (users.ContainsKey(username)) then
            printfn "%A" "error, no this username"
        else
            res <- users.[username]
        res
    member this.Subscribe username1 password username2 =
        let mutable res = ""
        if not (this.Authentication username1 password) then
            res <- "error, authentication fail"
        else
            let user1 = this.GetUser username1
            let user2 = this.GetUser username2
            user1.AddSubscribe user2
            user2.AddSubscriber user1
            res <- "[success] " + username1 + " subscribe " + username2
        res
    member this.ReTweet username password text =
        let res = "[retweet]" + (this.SendTweet username password text true)
        res
    member this.QueryTweetsSubscribed username password =
        let mutable res = ""
        if not (this.Authentication username password) then
            res <- "error, authentication fail"
        else
            let user = this.GetUser username
            let res1 = user.GetSubscribes() |> List.map(fun x-> x.GetTweets()) |> List.concat |> List.map(fun x->x.ToString()) |> String.concat "\n"
            res <- "[success] queryTweetsSubscribed" + "\n" + res1
        res
    member this.QueryHashTag hashtag =
        let mutable res = ""
        if not (hashtags.ContainsKey(hashtag)) then
            res <- "error, no this hashtag"
        else
            let res1 = hashtags.[hashtag] |>  List.map(fun x->x.ToString()) |> String.concat "\n"
            res <- "[success] queryHashTag" + "\n" + res1
        res
    member this.QueryMention mention =
        let mutable res = ""
        if not (mentions.ContainsKey(mention)) then
            res <- "error, no this mention"
        else
            let res1 = mentions.[mention] |>  List.map(fun x->x.ToString()) |> String.concat "\n"
            res <-  "[success] queryMention" + "\n" + res1
        res
    member this.Logout username password =
        let mutable res = ""

        if not (this.Authentication username password) then
            res <- "error, authentication fail"
        else
            let user = this.GetUser username
            user.Logout()  
        res
    override this.ToString() =
        "print the entire Twitter"+ "\n" + tweets.ToString() + "\n" + users.ToString() + "\n" + hashtags.ToString() + "\n" + mentions.ToString()
        
    
let twitter =  Twitter()

type MessagePackReg = MessagePack1 of  string  * string  * string* string
type MessagePackSend = MessagePack2 of  string  * string  * string* string* bool
type MessagePackSubscribe = MessagePack3 of  string  * string  * string* string 
type MessagePackRetweets = MessagePack4 of  string  * string  * string * string
type MessagePackQueryTweetsSubscribed = MessagePack5 of  string  * string  * string 
type MessagePackHashtag = MessagePack6 of  string  * string   
type MessagePackAt = MessagePack7 of  string  * string  
type MessagePackLogout = MessagePack9 of  string* string

let ActorReg (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack1(POST,register,username,password) ->
            if username = "" then
                return! loop()
            let res = twitter.register username password
            printfn "%s" res
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActorSend (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        let sender_path = mailbox.Sender().Path.ToStringWithAddress()
        match message  with
        |   MessagePack2(POST,username,password,tweet_content,false) -> 
            let res = twitter.SendTweet username password tweet_content false
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActorSubscribe (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack3(POST,username,password,target_username) -> 
            let res = twitter.Subscribe username password target_username
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActorRetweet (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack4(POST,username,password,tweet_content) -> 
            let res = twitter.ReTweet  username password tweet_content
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActorQuerying (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack5(POST,username,password ) -> 
            let res = twitter.QueryTweetsSubscribed  username password
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActoryQueryHashtag (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack6(POST,queryhashtag) -> 
            let res = twitter.QueryHashTag  queryhashtag
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActorAt (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack7(POST,at) -> 
            let res = twitter.QueryMention  at
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let ActorLogout (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match message  with
        |   MessagePack9(username,password) ->
            let res = twitter.Logout username password
            sender <? res |> ignore
        | _ ->  failwith "unknown message"
        return! loop()     
    }
    loop ()

let mutable opt= "reg" 
let mutable POST="POST"
let mutable username="user2"
let mutable password="123456"
let mutable register="register"
let mutable targetUsername="user1"
let mutable tweetContent="Today is a good day!"
let mutable queryhashtag="#Trump"
let mutable at="@Biden"

type MessagePackProcessor = MessagePack8 of  string  * string * string* string* string * string* string* string * string

let actorReg = spawn system "processor1" ActorReg
let actorSend = spawn system "processor2" ActorSend
let actorSubscribe = spawn system "processor3" ActorSubscribe
let actorRetweet = spawn system "processor4" ActorRetweet
let actorQuerying = spawn system "processor5" ActorQuerying 
let actorQueryHashtag = spawn system "processor6" ActoryQueryHashtag
let actorAt = spawn system "processor7" ActorAt
let actorLogout = spawn system "processor8" ActorLogout

let ApiActor (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender()
        match box message with
        | :? string   ->
            if message="" then
                return! loop() 
            printfn "%s" ""
            printfn "[message received] %s" message
            let result = message.Split ','
            let mutable opt= result.[0]
            let mutable POST=result.[1]
            let mutable username=result.[2]
            let mutable password=result.[3]
            let mutable target_username=result.[4]
            let mutable tweet_content=result.[5]
            let mutable queryhashtag=result.[6]
            let mutable at=result.[7]
            let mutable register=result.[8]
            let mutable task = actorReg <? MessagePack1("","","","")
            if opt= "reg" then
                printfn "[Register] username:%s password: %s" username password
                task <- actorReg <? MessagePack1(POST,register,username,password)
            if opt= "send" then
                printfn "[send] username:%s password: %s tweet_content: %s" username password tweet_content
                task <- actorSend <? MessagePack2(POST,username,password,tweet_content,false)
            if opt= "subscribe" then
                printfn "[subscribe] username:%s password: %s subscribes username: %s" username password target_username
                task <- actorSubscribe <? MessagePack3(POST,username,password,target_username )
            if opt= "retweet" then
                printfn "[retweet] username:%s password: %s tweet_content: %s" username password tweet_content
                task <- actorRetweet <? MessagePack4(POST,username,password,tweet_content)
            if opt= "querying" then
                printfn "[querying] username:%s password: %s" username password
                task <- actorQuerying <? MessagePack5(POST,username,password )
            if opt= "#" then
                printfn "[#Hashtag] %s: " queryhashtag
                task <- actorQueryHashtag <? MessagePack6(POST,queryhashtag )
            if opt= "@" then
                printfn "[@mention] %s" at
                task <- actorAt <? MessagePack7(POST,at )
            if opt= "logout" then
                task <- actorLogout <? MessagePack9(username,password)
            let response = Async.RunSynchronously (task, 1000)
            sender <? response |> ignore
            printfn "[Result]: %s" response
        return! loop()     
    }
    loop ()
let apiActor = spawn system "EchoServer" ApiActor

apiActor <? "" |> ignore
printfn "------------------------------------------------- \n " 
printfn "-------------------------------------------------   " 
printfn "Twitter Server is running...   " 
printfn "-------------------------------------------------   "

Console.ReadLine() |> ignore

printfn "-----------------------------------------------------------\n" 
0