package tls

import (
	"fmt"
	"log"
	"net"
)

func client(address string, size int, open func() (net.Conn, func(), error)) (err error) {
	log.SetFlags(log.Lshortfile)
	var n int
	conn, cleanup, err := open()
	if err != nil {
		return err
	}
	defer cleanup()

	_, err = conn.Write([]byte(fmt.Sprintf("%d\n", size)))
	if err != nil {
		return
	}

	buf := make([]byte, size)
	read := 0
	for read < size {
		n, err = conn.Read(buf)
		if err != nil {
			return err
		}
		read += n
	}
	return nil
}
