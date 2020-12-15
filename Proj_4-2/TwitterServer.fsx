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
                    port = 8555
                    hostname = localhost
                }
            }
        }")

type ServerOps =
    | Register of string* string
    | Send of  string  * string* string* bool
    | Subscribe of   string  * string* string 
    | ReTweet of  string  * string * string
    | Querying of  string  * string 
    | QueryHashTag of   string   
    | QueryAt of   string  
    | Logout of  string* string

let system = ActorSystem.Create("TwitterServer", configuration)

type Tweet(tweetId:string, text:string, isRetweet:bool) =
    member this.Text = text
    member this.TweetId = tweetId
    member this.IsReTweet = isRetweet

    override this.ToString() =
      let mutable res = ""
      if isRetweet then
        res <- sprintf "[retweet][%s]%s" this.TweetId this.Text
      else
        res <- sprintf "[%s]%s" this.TweetId this.Text
      res

type User(userName:string, password:string) =
    let mutable following = List.empty: User list
    let mutable followers = List.empty: User list
    let mutable tweets = List.empty: Tweet list
    let mutable loggedIn = true
    member this.UserName = userName
    member this.Password = password
    member this.GetFollowing() =
        following
    member this.GetFollowers() =
        followers
    member this.AddToFollowing user =
        following <- List.append following [user]
    member this.AddToFollowers user =
        followers <- List.append followers [user]
    member this.AddTweet x =
        tweets <- List.append tweets [x]
    member this.GetTweets() =
        tweets
    member this.IsLoggedIn() = 
        loggedIn
    member this.Logout() =
        loggedIn <- false
    override this.ToString() = 
       this.UserName

type Twitter() =
    let mutable tweetIdToTweetMap = new Map<string,Tweet>([])
    let mutable usernameToUserObjMap = new Map<string,User>([])
    let mutable hashtagToTweetMap = new Map<string, Tweet list>([])
    let mutable mentionsToTweetMap = new Map<string, Tweet list>([])
    member this.AddUser (user:User) =
        usernameToUserObjMap <- usernameToUserObjMap.Add(user.UserName, user)
    member this.AddTweet (tweet:Tweet) =
        tweetIdToTweetMap <- tweetIdToTweetMap.Add(tweet.TweetId,tweet)
    member this.AddToHashTag hashtag tweet =
        let key = hashtag
        let mutable map = hashtagToTweetMap
        if not (map.ContainsKey(key)) then
            let l = List.empty: Tweet list
            map <- map.Add(key, l)
        let value = map.[key]
        map <- map.Add(key, List.append value [tweet])
        hashtagToTweetMap <- map
    member this.AddToMention mention tweet = 
        let key = mention
        let mutable map = mentionsToTweetMap
        if not (map.ContainsKey(key)) then
            let l = List.empty: Tweet list
            map <- map.Add(key, l)
        let value = map.[key]
        map <- map.Add(key, List.append value [tweet])
        mentionsToTweetMap <- map
    member this.Register username password =
        let mutable res = ""
        if usernameToUserObjMap.ContainsKey(username) then
            res <- "[Register][Error]: Username already exists!"
        else
            let user = User(username, password)
            this.AddUser user
            user.AddToFollowing user
            res <- "[Register][Sucess]: " + username + "  Added successfully! "
        res
    member this.SendTweet username password text isRetweet =
        let mutable res = ""
        if not (this.Authentication username password) then
            res <- "[Sendtweet][Error]: Username & password do not match"
        else
            if not (usernameToUserObjMap.ContainsKey(username))then
                res <-  "[Sendtweet][Error]: Username not found"
            else
                let user = usernameToUserObjMap.[username]
                let tweet = Tweet(DateTime.Now.ToFileTimeUtc() |> string, text, isRetweet)
                user.AddTweet tweet
                this.AddTweet tweet

                for subUser in user.GetFollowers() do
                    if subUser.IsLoggedIn() then
                        let subUserActor = system.ActorSelection("akka.tcp://TwitterSim@localhost:8666/user/"+subUser.ToString())
                        subUserActor <! "RecievedTweet,"+tweet.Text
                
                let mentionStart = text.IndexOf("@")
                if mentionStart <> -1 then
                    let mentionEnd = text.IndexOf(" ",mentionStart)
                    let mention = text.[mentionStart..mentionEnd-1]
                    this.AddToMention mention tweet
                
                let hashStart = text.IndexOf("#")
                if hashStart <> -1 then
                    let hashEnd = text.IndexOf(" ",hashStart)
                    let hashtag = text.[hashStart..hashEnd-1]
                    this.AddToHashTag hashtag tweet
                
                res <-  "[Sendtweet][Success]: Sent "+tweet.ToString()
        res
    member this.Authentication username password =
            let mutable res = false
            if not (usernameToUserObjMap.ContainsKey(username)) then
                printfn "%s" "[Authentication][Error]: Username not found"
            else
                let user = usernameToUserObjMap.[username]
                if user.Password = password then
                    res <- true
            res
    member this.GetUser username = 
        let mutable res = User("","")
        if not (usernameToUserObjMap.ContainsKey(username)) then
            printfn "%s" "[FetchUserObject][Error]: Username not found"
        else
            res <- usernameToUserObjMap.[username]
        res
    member this.Subscribe username1 password username2 =
        let mutable res = ""
        if not (this.Authentication username1 password) then
            res <- "[Subscribe][Error]: Username & password do not match"
        else
            let user1 = this.GetUser username1
            let user2 = this.GetUser username2
            user1.AddToFollowing user2
            user2.AddToFollowers user1
            res <- "[Subscribe][Success]: " + username1 + " now following " + username2
        res
    member this.ReTweet username password text =
        let res = "[retweet]" + (this.SendTweet username password text true)
        res
    member this.QueryTweetsSubscribed username password =
        let mutable res = ""
        if not (this.Authentication username password) then
            res <- "[QueryTweets][Error]: Username & password do not match"
        else
            let user = this.GetUser username
            let res1 = user.GetFollowing() |> List.map(fun x-> x.GetTweets()) |> List.concat |> List.map(fun x->x.ToString()) |> String.concat "\n"
            res <- "[QueryTweets][Success] " + "\n" + res1
        res
    member this.QueryHashTag hashtag =
        let mutable res = ""
        if not (hashtagToTweetMap.ContainsKey(hashtag)) then
            res <- "[QueryHashTags][Error]: No Hashtag with given String found"
        else
            let res1 = hashtagToTweetMap.[hashtag] |>  List.map(fun x->x.ToString()) |> String.concat "\n"
            res <- "[QueryHashTags][Success] " + "\n" + res1
        res
    member this.QueryMention mention =
        let mutable res = ""
        if not (mentionsToTweetMap.ContainsKey(mention)) then
            res <- "[QueryMentions][Error]: No mentions are found for the given user"
        else
            let res1 = mentionsToTweetMap.[mention] |>  List.map(fun x->x.ToString()) |> String.concat "\n"
            res <-  "[QueryMentions][Success]:" + "\n" + res1
        res
    member this.Logout username password =
        let mutable res = ""

        if not (this.Authentication username password) then
            res <- "[Logout][Error]: Username & password do not match"
        else
            let user = this.GetUser username
            user.Logout()  
        res
    override this.ToString() =
        "Snapshot of Twitter"+ "\n" + tweetIdToTweetMap.ToString() + "\n" + usernameToUserObjMap.ToString() + "\n" + hashtagToTweetMap.ToString() + "\n" + mentionsToTweetMap.ToString()
        
    
let twitter =  Twitter()


let ActorReg (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   Register(username,password) ->
            if username = "" then
                return! loop()
            mailbox.Sender() <? twitter.Register username password |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorReg = spawn system "actorReg" ActorReg

let ActorSend (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   Send(username,password,tweetData,false) -> 
            mailbox.Sender() <? twitter.SendTweet username password tweetData false |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorSend = spawn system "actorSend" ActorSend

let ActorSubscribe (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   Subscribe(username,password,subsribeUsername) -> 
            mailbox.Sender() <? twitter.Subscribe username password subsribeUsername |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorSubscribe = spawn system "actorSubscribe" ActorSubscribe

let ActorRetweet (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   ReTweet(username,password,tweetData) -> 
            mailbox.Sender() <? twitter.ReTweet  username password tweetData |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorRetweet = spawn system "actorRetweet" ActorRetweet

let ActorQuerying (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   Querying(username,password ) -> 
            mailbox.Sender() <? twitter.QueryTweetsSubscribed  username password |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorQuerying = spawn system "actorQuerying" ActorQuerying 

let ActoryQueryHashtag (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   QueryHashTag(queryhashtag) -> 
            mailbox.Sender() <? twitter.QueryHashTag  queryhashtag |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorQueryHashtag = spawn system "actorQueryHashtag" ActoryQueryHashtag

let ActorAt (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   QueryAt(at) -> 
            mailbox.Sender() <? twitter.QueryMention  at |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorAt = spawn system "actorAt" ActorAt

let ActorLogout (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        match msg  with
        |   Logout(username,password) ->
            mailbox.Sender() <? twitter.Logout username password |> ignore
        | _ ->  failwith "Invalid Operation "
        return! loop()     
    }
    loop ()

let actorLogout = spawn system "actorLogout" ActorLogout

let ApiActor (mailbox: Actor<_>) = 
    let rec loop () = actor {        
        let! msg = mailbox.Receive ()
        let sender = mailbox.Sender()
        if msg="" then
            return! loop() 
        //Parse Message
        let inpMessage = msg.Split ','
        let mutable serverOperation= inpMessage.[0]
        let mutable username=inpMessage.[2]
        let mutable password=inpMessage.[3]
        let mutable subsribeUsername=inpMessage.[4]
        let mutable tweetData=inpMessage.[5]
        let mutable queryhashtag=inpMessage.[6]
        let mutable at=inpMessage.[7]
        let mutable task = actorReg <? Register("","")
        if serverOperation= "reg" then
            printfn "[Register] username:%s password: %s" username password
            task <- actorReg <? Register(username,password)
        else if serverOperation= "send" then
            printfn "[send] username:%s password: %s tweetData: %s" username password tweetData
            task <- actorSend <? Send(username,password,tweetData,false)
        else if serverOperation= "subscribe" then
            printfn "[subscribe] username:%s password: %s following username: %s" username password subsribeUsername
            task <- actorSubscribe <? Subscribe(username,password,subsribeUsername )
        else if serverOperation= "querying" then
            printfn "[querying] username:%s password: %s" username password
            task <- actorQuerying <? Querying(username,password )
        else if serverOperation= "retweet" then
            printfn "[retweet] username:%s password: %s tweetData: %s" username password tweetData
            task <- actorRetweet <? ReTweet(username,password,tweetData)
        else if serverOperation= "@" then
            printfn "[@mention] %s" at
            task <- actorAt <? QueryAt(at )
        else if serverOperation= "#" then
            printfn "[#Hashtag] %s: " queryhashtag
            task <- actorQueryHashtag <? QueryHashTag(queryhashtag )
        else if serverOperation= "logout" then
            task <- actorLogout <? Logout(username,password)
        let response = Async.RunSynchronously (task, 1000)
        sender <? response |> ignore
        printfn "[Result]: %s" response
        return! loop()     
    }
    loop ()
let apiActor = spawn system "ApiActor" ApiActor

apiActor <? "" |> ignore
printfn "*****************************************************" 
printfn "Starting Twitter Server!! ...  " 
printfn "*****************************************************"

Console.ReadLine() |> ignore
0