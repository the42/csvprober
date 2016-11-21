package main

import (
	"fmt"
	"log"
	"os"

	"github.com/the42/csvprober"
)

func main() {
	prober := csvprober.NewProber()
	r, err := prober.Probe(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range r.CSVprobability {
		fmt.Printf("Delimiter: %c Min: %d, Mean: %f, Max: %d, Stddev: %f\n", v.Delimiter, v.Min, v.Mean, v.Max, v.Stddev)
	}
}
