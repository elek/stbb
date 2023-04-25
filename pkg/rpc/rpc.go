package rpc

type RPC struct {
	TcpClient TCPClient `cmd:""`
	TcpServer TCPServer `cmd:""`
}
