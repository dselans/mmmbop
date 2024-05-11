package migrator

import (
	"context"

	"github.com/sirupsen/logrus"
)

func (m *Migrator) runReader(shutdownCtx context.Context, workCh chan<- *Job) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runReader",
	})

	// TODO: Read file
	//// Open the file
	//file, err := os.Open("mongodb_dump.gz")
	//if err != nil {
	//	return errors.Wrap(err, "unable to open file")
	//}
	//defer file.Close()
	//
	//// Create a gzip reader
	//gzipReader, err := gzip.NewReader(file)
	//if err != nil {
	//	log.Fatalf("Failed to create gzip reader: %v", err)
	//}
	//defer gzipReader.Close()
	//
	//// Create a buffered scanner to read line by line
	//scanner := bufio.NewScanner(gzipReader)
	//for scanner.Scan() {
	//	fmt.Println(string(scanner.Bytes()))
	//
	//	var doc bson.M
	//	// Unmarshal BSON data from each line
	//	err := bson.Unmarshal(scanner.Bytes(), &doc)
	//	if err != nil {
	//		log.Fatalf("Failed to unmarshal BSON: %v", err)
	//	}
	//
	//	// Print the document or handle it
	//	fmt.Println(doc)
	//	os.Exit(0)
	//}
	//
	//if err := scanner.Err(); err != nil {
	//	log.Fatalf("Error during scanning: %v", err)
	//}
	//
	//return nil

	llog.Debug("start")
	defer llog.Debug("exit")

	return nil
}
