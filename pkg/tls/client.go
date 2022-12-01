package tls

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"time"
)

func client() (err error) {
	log.SetFlags(log.Lshortfile)

	conf := &tls.Config{
		InsecureSkipVerify: true,
	}

	start := time.Now()
	samples := 1000
	for i := 0; i < samples; i++ {
		var conn net.Conn
		conn, err = tls.Dial("tcp", "127.0.0.1:1443", conf)
		if err != nil {
			return
		}

		_, err = conn.Write([]byte("hello\n"))
		if err != nil {
			return
		}

		buf := make([]byte, 100)
		_, err = conn.Read(buf)
		if err != nil {
			return
		}

		conn.Close()
	}
	fmt.Printf("%d", time.Since(start).Milliseconds()/int64(samples))

	return nil
}
