usage:
    dotnet fsi proj2.fsx numNodes topology algorithm	
    for bonus
    dotnet fsi proj2Bonus.fsx numNodes topology algorithm


What is working:
    Convergence of Gossip and Push-Sum algorithms for all 4 topologies.
	Bonus part: Implementation of node failure for both algorithms in all 4 topologies.

Largest network dealed with:

	1. For Gossip algorithm:
		a) Full network topology: 1000000 nodes 
		b) 2D network topology: 1000000 nodes
		c) Imperfect 2D topology: 1000000 nodes
		d) Line topology: 10000 nodes

	2. For Push-Sum algorithm:
		a) Full network topology: 1000000 nodes 
		b) 2D network topology: 100000 nodes
		c) Imperfect 2D topology: 1000000 nodes
		d) Line topology: 1000 nodes
