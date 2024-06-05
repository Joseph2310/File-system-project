package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func handle_query(conn net.Conn) {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		scanned := scanner.Scan()
		if !scanned {
			fmt.Println("Error reading input: ", scanner.Err())
			return
		}

		cmd_line := scanner.Text()
		_, err := conn.Write([]byte(cmd_line))
		if err != nil {
			fmt.Println("Error writing input: ", err)
			return
		}

		if cmd_line != "EOP!!" {
			buffer := make([]byte, 1)
			n, err := conn.Read(buffer)
			if err != nil {
				fmt.Println("Error reading command execution state: ", err)
				return
			}

			exec_state := string(buffer[:n])
			if exec_state == "0" {
				fmt.Println("Query failed")
			} else if exec_state == "1" {
				fmt.Println("Query success")
				if cmd_line[0] == 'S' || cmd_line[0] == 's' {
					for {
						buffer = make([]byte, 4)
						buffer2 := new(bytes.Buffer)
						_, err := conn.Read(buffer)
						if err != nil {
							fmt.Println("Error reading length:", err)
							break
						}

						_, err = buffer2.Write(buffer)
						if err != nil {
							fmt.Println("Error buffer casting:", err)
							break
						}

						var wordLength int32
						err = binary.Read(buffer2, binary.LittleEndian, &wordLength)
						if err != nil {
							fmt.Println("Error buffer casting:", err)
						}
						//fmt.Println(wordLength)

						buffer = make([]byte, wordLength)
						_, err = conn.Read(buffer)
						if err != nil {
							fmt.Println("Error reading query data:", err)
						}

						word := string(buffer)
						if word == "EOR!!" {
							fmt.Println()
						} else if word == "EOF!!" {
							fmt.Println("Query finished")
							break
						} else {
							if len(word) >= 10 {
								fmt.Print(word, "\t")
							} else {
								fmt.Print(word, "\t\t")
							}
						}
					}
				}
			}
		} else {
			break
		}
	}
}

func connect_to_server(ip_port string) net.Conn {
	conn, err := net.Dial("tcp", ip_port)
	if err != nil {
		panic(err)
	} else {
		return conn
	}
}

func main() {
	const ip_port = "192.168.0.105:9055"
	conn := connect_to_server(ip_port)
	fmt.Println("Connected successfully")
	defer conn.Close()
	handle_query(conn)
}
