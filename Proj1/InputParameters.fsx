for arg in fsi.CommandLineArgs do
    printf "%s " arg

printfn ""
printfn "param1 a string %s" fsi.CommandLineArgs.[1]
printfn "param2 a string %s" fsi.CommandLineArgs.[2]
let param1 = fsi.CommandLineArgs.[1] |> int
let param2 = fsi.CommandLineArgs.[2] |> int
printfn "param1 an Int %i" param1
printfn "param2 an Int %i" param2