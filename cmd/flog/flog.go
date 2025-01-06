package main

import (
	"database/sql"
	"io/fs"
	"log/slog"
	"os"
	"path"

	"github.com/hellojonas/flog/pkg/applog"
	"github.com/hellojonas/flog/pkg/flog"
	"github.com/hellojonas/flog/pkg/flog/flogdb"
	"github.com/hellojonas/flog/pkg/tcp"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	logger := applog.Logger()
	userDir := os.Getenv("HOME")

	if os.PathSeparator == '\\' {
		userDir = os.Getenv("USERPROFILE")
	}

	flogRoot := path.Join(userDir, ".flog")

	if err := os.MkdirAll(flogRoot, fs.ModePerm); err != nil {
		panic(err)
	}

	dbpath := "file:" + path.Join(flogRoot, "flog.db")

	logger.Info("opening db...", slog.String("db", dbpath))
	db, err := sql.Open("sqlite3", dbpath)

	if err != nil {
		panic(err)
	}

	defer db.Close()

	flogdb.InitSchema(db)

	addr := ":8008"
	server, err := tcp.NewTCPServer(addr, flog.New())

	if err != nil {
		panic(err)
	}

	defer server.Close()

	server.StartAccept()
}
