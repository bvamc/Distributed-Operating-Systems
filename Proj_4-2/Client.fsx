#load "references.fsx"

open WebSocketSharp

let ws = new WebSocket("ws://localhost:8080/websocket")

ws.OnOpen.Add(fun args -> System.Console.WriteLine("Open"))
ws.OnClose.Add(fun args -> System.Console.WriteLine("Close"))
ws.OnMessage.Add(fun args -> System.Console.WriteLine("Msg: {0}", args.Data))
ws.OnError.Add(fun args -> System.Console.WriteLine("Error: {0}", args.Message))

ws.Connect()


ws.Send("User1")
//ws.Send("""{"ref":"1", "event":"phx_join", "topic":"rooms:lobby", "payload":{"user":"boo"}}""")
//ws.Send("""{"ref":"2", "event":"new:msg", "topic":"rooms:lobby", "payload":{"user":"boo","body":"howdy"}}""")
//ws.Send("""{"ref":"3", "event":"new:msg", "topic":"rooms:lobby", "payload":{"user":"boo","body":"Woohoo!"}}""")
let mutable a = 0

while true do
    printfn "%A" ws.IsAlive
    a <- a+1

//Newton Json soft dll
