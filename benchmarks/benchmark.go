package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/reesporte/bugfruit"
)

func bench(iter, nops, keysz, valsz int, config *bugfruit.Config, name string, keys [][]byte) {
	s, err := bugfruit.NewStorage(name, 0777, config)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	for i := 0; i < nops; i++ {
		k := keys[i]
		v := make([]byte, valsz)
		for i := 0; i < valsz; i++ {
			v[i] = k[i%keysz]
		}
		err := s.Set(string(k), v)
		if err != nil {
			panic(err)
		}
	}
	end := time.Now()
	total := end.Sub(start).Seconds()
	fmt.Printf("set, %d, %f, %d, %f\n", iter, total, nops, float64(nops)/total)

	start = time.Now()
	for i := 0; i < nops; i++ {
		k := string(keys[i])
		val, ok := s.Get(k)
		if !ok {
			log.Fatalf("%v %v", val, ok)
		}
	}

	end = time.Now()
	total = end.Sub(start).Seconds()
	fmt.Printf("get, %d, %f, %d, %f\n", iter, total, nops, float64(nops)/total)

	start = time.Now()
	for i := 0; i < nops; i++ {
		k := string(keys[i])
		err := s.Delete(k)
		if err != nil {
			panic(err)
		}
	}

	end = time.Now()
	total = end.Sub(start).Seconds()
	fmt.Printf("delete, %d, %f, %d, %f\n", iter, total, nops, float64(nops)/total)

	err = s.Close()
	if err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(name); err != nil {
		log.Fatal(err)
	}
}

func main() {
	nopsPtr := flag.Int("nops", 10000000, "how many operations to complete")
	iterPtr := flag.Int("iter", 1000, "how many iterations to benchmark")
	keyszPtr := flag.Int("keysz", 16, "how large keys should be (in bytes)")
	valszPtr := flag.Int("valsz", 100, "how large vals should be (in bytes)")

	flag.Parse()

	iter := *iterPtr
	nops := *nopsPtr
	keysz := *keyszPtr
	valsz := *valszPtr

	config := &bugfruit.Config{VacuumBatch: 0, FsyncBatch: 0}

	keys := make([][]byte, nops)
	for i := 0; i < nops; i++ {
		k := make([]byte, keysz)
		rand.Read(k)
		keys[i] = k
	}

	fmt.Println("type, iteration, s, nops, opsps")
	for i := 0; i < iter; i++ {
		bench(i, nops, keysz, valsz, config, "benchmarks.db", keys)
	}
}
