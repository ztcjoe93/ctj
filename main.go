package main

import (
	"flag"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	trades = []*Trade{}
)

func main() {
	logFile, err := os.OpenFile("ctj.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatal("Error opening log file:", err)
	}
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	logger.SetOutput(multiWriter)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	filePath := flag.String("p", "", "CSV file path to consume")
	flag.Parse()

	logger.Debug("Ingesting csv...")
	trades = ingestCSV(*filePath)
	SortTradesByStatusUpdateTime(trades)

	DetermineTradeLeg(trades)
}
