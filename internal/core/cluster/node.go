package cluster

type Node struct {
	Role        string
	Port        string
	JoinAddress string
}

func NewNode(role string, port string, joinAddress string) *Node {
	return &Node{
		Role:        role,
		Port:        port,
		JoinAddress: joinAddress,
	}
}
