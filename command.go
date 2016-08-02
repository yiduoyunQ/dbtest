package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	_ "github.com/go-sql-driver/mysql"
)

var (
	rootUser     = "check"
	rootPassword = "123.com"
	ip           = ""
	port         = 0
	defaultFile  = "/DBAASDAT/my.cnf"
	defaultDb    = "DBaaS_check"
	defaultTable = "chk"
	timeout      = 5 * time.Second
	readTimeout  = 5 * time.Second

	commands = []cli.Command{
		// health check
		{
			Name:        "dbHealthCheck",
			ShortName:   "dhc",
			Usage:       "upsql health check",
			Description: "upsql health check with insert,select,delete",
			Flags:       flags,
			Action: func(c *cli.Context) {
				if c.IsSet("default-file") {
					defaultFile = c.String("default-file")
				}
				if c.Bool("version") {
					cli.ShowVersion(c)
					return
				}
				if c.IsSet("root-user") {
					rootUser = c.String("root-user")
				}
				if c.IsSet("root-password") {
					rootPassword = c.String("root-password")
				}
				if c.IsSet("default-db") {
					defaultDb = c.String("default-db")
				}
				if c.IsSet("default-table") {
					defaultTable = c.String("default-table")
				}
				if c.IsSet("time-out") {
					timeout = c.Duration("time-out")
				}
				if c.IsSet("read-time-out") {
					readTimeout = c.Duration("read-time-out")
				}

				f, err := os.Open(defaultFile)
				if err != nil {
					panic(err)
				}

				r := bufio.NewReader(f)
				for {
					b, _, err := r.ReadLine()
					if err != nil {
						if err == io.EOF {
							break
						}
						panic(err)
					}

					s := strings.TrimSpace(string(b))
					if strings.Index(s, "#") == 0 {
						continue
					}
					index := strings.Index(s, "=")
					if index < 0 {
						continue
					}
					key := strings.TrimSpace(s[:index])
					if len(key) == 0 {
						continue
					}
					val := strings.TrimSpace(s[index+1:])
					if len(val) == 0 {
						continue
					}
					index = strings.Index(val, "#")
					if index > 0 {
						val = strings.TrimSpace(val[:index])
					}
					switch strings.ToLower(key) {
					default:
						continue
					case "bind_address":
						ip = val
					case "port":
						port, err = strconv.Atoi(val)
						if err != nil {
							panic(err)
						}
					}
				}

				err = check(timeout, readTimeout)
				if err != nil {
					fmt.Println(2)
					os.Exit(2)
				}
				fmt.Println(0)
			},
		},
	}
	flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "version, v",
			Usage: "print app version",
		},
		cli.StringFlag{
			Name:  "root-user, u",
			Usage: "root user",
		},
		cli.StringFlag{
			Name:  "root-password, p",
			Usage: "root password",
		},
		cli.StringFlag{
			Name:  "default-file, 0",
			Usage: "default config file",
		},
		cli.StringFlag{
			Name:  "default-db, D",
			Usage: "health check database",
		},
		cli.StringFlag{
			Name:  "default-table, T",
			Usage: "health check table",
		},
		cli.DurationFlag{
			Name:  "time-out",
			Value: 5 * time.Second,
			Usage: "db connect time out",
		},
		cli.DurationFlag{
			Name:  "read-time-out",
			Value: 5 * time.Second,
			Usage: "insert, select, delete time out",
		},
	}
)

func check(t, rt time.Duration) error {
	tEnd := time.Now().Add(t)
	rtEnd := time.Now().Add(rt)
	_t := t
	_rt := rt
	// insert
	db1, err := sql.Open("mysql", rootUser+":"+rootPassword+"@tcp("+ip+":"+strconv.Itoa(port)+")/"+defaultDb+"?timeout="+_t.String()+"&readTimeout="+_rt.String())
	log.Println(rootUser + ":" + rootPassword + "@tcp(" + ip + ":" + strconv.Itoa(port) + ")/" + defaultDb + "?timeout=" + _t.String() + "&readTimeout=" + _rt.String())
	if err != nil {
		log.Println("insert sql.Open error")
		return err
	}
	defer db1.Close()
	err = db1.Ping()
	if err != nil {
		log.Println("insert db.Ping error")
		return err
	}
	_t = tEnd.Sub(time.Now())
	rows1, err := db1.Query("insert into " + defaultTable + " values(1,'a');")
	if err != nil {
		log.Println("insert db.Query error")
		return err
	}
	defer rows1.Close()
	err = rows1.Err()
	if err != nil {
		log.Println("insert rows error")
		return err
	}
	_rt = rtEnd.Sub(time.Now())

	// select
	if _t <= 0 || _rt <= 0 {
		return fmt.Errorf("time out when excute select")
	}
	db2, err := sql.Open("mysql", rootUser+":"+rootPassword+"@tcp("+ip+":"+strconv.Itoa(port)+")/"+defaultDb+"?timeout="+_t.String()+"&readTimeout="+_rt.String())
	if err != nil {
		log.Println("select sql.Open error")
		return err
	}
	defer db2.Close()
	err = db2.Ping()
	if err != nil {
		log.Println("select db.Ping error")
		return err
	}
	_t = tEnd.Sub(time.Now())
	rows2, err := db2.Query("select * from " + defaultTable + ";")
	if err != nil {
		log.Println("select db.Query error")
		return err
	}
	defer rows2.Close()
	err = rows2.Err()
	if err != nil {
		log.Println("select rows error")
		return err
	}
	_rt = rtEnd.Sub(time.Now())

	// delete
	if _t <= 0 || _rt <= 0 {
		return fmt.Errorf("time out when excute delete")
	}
	db3, err := sql.Open("mysql", rootUser+":"+rootPassword+"@tcp("+ip+":"+strconv.Itoa(port)+")/"+defaultDb+"?timeout="+_t.String()+"&readTimeout="+_rt.String())
	if err != nil {
		log.Println("delete sql.Open error")
		return err
	}
	defer db3.Close()
	err = db3.Ping()
	if err != nil {
		log.Println("delete db.Ping error")
		return err
	}
	rows3, err := db3.Query("delete from " + defaultTable + ";")
	if err != nil {
		log.Println("delete db.Query error")
		return err
	}
	defer rows3.Close()
	err = rows3.Err()
	if err != nil {
		log.Println("delete rows error")
		return err
	}

	return nil
}
