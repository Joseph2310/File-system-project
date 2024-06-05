package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"

	_ "github.com/go-sql-driver/mysql" // Import MySQL driver package
)

func createAndInsertTables(db *sql.DB) error {

	_, err := db.Exec(`DROP TABLE IF EXISTS Advisor, Enrollment, Subject, Teacher, Student`)
	if err != nil {
		fmt.Println("Error dropping tables:", err)
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS Student (
        id_student INT AUTO_INCREMENT PRIMARY KEY ,
        name VARCHAR(255),
        age INT,
        class VARCHAR(255)
    )`)

	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS Teacher (
        id_teacher INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255)
    )`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS Subject (
        id_Sub INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        teacher_id INT,
        FOREIGN KEY(teacher_id) REFERENCES Teacher(id_teacher)
    )`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS Enrollment (
        student_id INT,
        subject_id INT,
        FOREIGN KEY(student_id) REFERENCES Student(id_student),
        FOREIGN KEY(subject_id) REFERENCES Subject(id_Sub),
        PRIMARY KEY(student_id, subject_id)
    )`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS Advisor (
        student_id INTEGER,
        teacher_id INTEGER,
        FOREIGN KEY(student_id) REFERENCES Student(id_student),
        FOREIGN KEY(teacher_id) REFERENCES Teacher(id_teacher),
        PRIMARY KEY(student_id)
    )`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}
	//Insert data
	// Insert data into the Student table
	_, err = db.Exec("INSERT IGNORE INTO Student (name, age, class) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		"Alice", 20, "Math",
		"Bob", 22, "Physics",
		"Charlie", 21, "Biology",
		"David", 23, "Chemistry",
		"Emma", 19, "Literature",
		"Frank", 24, "History",
		"Grace", 20, "Geography",
		"Henry", 22, "Computer Science",
		"Ivy", 21, "Physics",
		"Jack", 23, "Math")
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	// Insert data into the Teacher table
	_, err = db.Exec("INSERT IGNORE INTO Teacher (name) VALUES (?), (?), (?), (?), (?), (?), (?), (?), (?), (?)",
		"Smith", "Johnson", "Brown", "White", "Lee", "Garcia", "Martinez", "Davis", "Taylor", "Clark")
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	// Insert data into the Subject table
	_, err = db.Exec("INSERT IGNORE INTO Subject (name) VALUES (?), (?), (?), (?), (?), (?), (?), (?), (?), (?)",
		"Math",
		"Physics",
		"Biology",
		"Chemistry",
		"Literature",
		"History",
		"Geography",
		"Computer Science",
		"Physics",
		"Science")
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	// Insert data into the Enrollment table
	_, err = db.Exec("INSERT IGNORE INTO Enrollment (student_id, subject_id) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)",
		1, 1,
		2, 2,
		3, 3,
		4, 4,
		5, 5,
		6, 6,
		7, 7,
		8, 8,
		9, 9,
		10, 10)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	// Insert data into the Advisor table
	_, err = db.Exec("INSERT IGNORE INTO Advisor (student_id, teacher_id) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)",
		1, 1,
		2, 2,
		3, 3,
		4, 4,
		5, 5,
		6, 6,
		7, 7,
		8, 8,
		9, 9,
		10, 10)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	fmt.Println("Data inserted successfully!")
	return nil
}

func send_data(msg string, conn net.Conn) {
	// Create a buffer to store binary data
	buffer := new(bytes.Buffer)

	// Writing binary data to the buffer
	var intValue int32 = int32(len(msg))
	err := binary.Write(buffer, binary.LittleEndian, intValue)
	if err != nil {
		fmt.Println("Error writing integer:", err)
		return
	}

	temp_buffer := buffer.Bytes()
	_, err2 := conn.Write(temp_buffer)
	if err2 != nil {
		fmt.Println("Error sending the length:", err)
	}
}

func handle_connection(conn net.Conn, db *sql.DB) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	for {
		length, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading: ", err)
		}

		sql_cmd := string(buffer[:length])

		fmt.Println("command:", sql_cmd)
		if sql_cmd == "EOP!!" {
			break
		}
		// select statement
		if sql_cmd[0] == 'S' || sql_cmd[0] == 's' {

			rows, err := db.Query(sql_cmd)
			if err != nil {
				fmt.Println("Error selecting data:", err)

				// send unsuccessful signal
				_, err2 := conn.Write([]byte("0"))
				if err2 != nil {
					fmt.Println("Error sending unsuccessfull signal:", err)
				}
				continue
			}

			// send successful signal
			_, err2 := conn.Write([]byte("1"))
			if err2 != nil {
				fmt.Println("Error sending successfull signal:", err)
			}

			cols, err := rows.Columns()
			if err != nil {
				fmt.Println("Error getting columns:", err)
				break
			}

			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				fmt.Println("Error getting columns:", err)
				break
			}

			// get the number of columns
			num_of_columns := len(cols)

			values := make([]interface{}, num_of_columns)
			valuePtrs := make([]interface{}, num_of_columns)
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			for i := 0; i < num_of_columns; i++ {
				send_data(cols[i], conn)
				_, err4 := conn.Write([]byte(cols[i]))
				if err4 != nil {
					fmt.Println("Error sending signal (EOF!!):", err)
				}

			}

			send_data("EOR!!", conn)
			//send end row signal
			_, err4 := conn.Write([]byte("EOR!!"))
			if err4 != nil {
				fmt.Println("Error sending signal (EOF!!):", err)
			}

			for rows.Next() {

				if err := rows.Scan(valuePtrs...); err != nil {
					fmt.Println("Error scanning row:", err)
					break
				}

				// Process each column value
				for i, data := range values {
					// Convert data to the appropriate type and handle it as needed
					// Example: send the length of the string

					str := ""
					colType := columnTypes[i].DatabaseTypeName()
					if colType == "VARCHAR" || colType == "TEXT" || colType == "CHAR" || colType == "LONGTEXT" {
						nameSlice := data.([]byte)
						for _, b := range nameSlice {
							str += string(b)
						}
					} else {
						str = string(fmt.Sprint(data))
					}

					send_data(str, conn)

					// Convert data to string and send it over the connection
					_, err := conn.Write([]byte(str))
					if err != nil {
						fmt.Println("Error sending current data:", err)
					}
				}

				send_data("EOR!!", conn)
				//send end row signal
				_, err4 := conn.Write([]byte("EOR!!"))
				if err4 != nil {
					fmt.Println("Error sending signal (EOF!!):", err)
				}
			}

			send_data("EOF!!", conn)
			//send end data signal
			_, err4 = conn.Write([]byte("EOF!!"))
			if err4 != nil {
				fmt.Println("Error sending signal (EOF!!):", err)
			}
		} else {
			// other statement
			_, err := db.Exec(sql_cmd)
			if err != nil {
				fmt.Println("Error finishing the operation:", err)

				// The operation didn't done
				_, err2 := conn.Write([]byte("0"))
				if err2 != nil {
				}
			} else {
				// the operation done successfully
				_, err2 := conn.Write([]byte("1"))
				if err2 != nil {
				}
			}
		}
	}
	fmt.Println("Slave disconnected: ", conn.RemoteAddr())
}

func main() {

	// start listening
	listener, err := net.Listen("tcp", ":9055")
	if err != nil {
		fmt.Println("Error: Can't listen \n", err)
	}

	// Open a database connection
	curr_db, err := sql.Open("mysql", "root:1234@tcp(localhost:3306)/test_database")
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}
	defer curr_db.Close() // Defer closing the database connection

	// Ping the database to check if the connection is successful
	err = curr_db.Ping()
	if err != nil {
		fmt.Println("Error pinging the database:", err)
		return
	}

	fmt.Println("Connected to the database successfully!")

	// create database tables and insert data
	createAndInsertTables(curr_db)

	for {
		// try accepting new client
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting new connection:", err, "\n")
			return
		}

		fmt.Println("connected with client:", conn.RemoteAddr())
		go handle_connection(conn, curr_db)
	}

}
