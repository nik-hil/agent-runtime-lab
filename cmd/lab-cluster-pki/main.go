package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"agent-runtime-lab/src/clustertls"
)

func main() {
	dir := flag.String("dir", ".state/m5-pki", "new private PKI directory")
	hosts := flag.String("hosts", "localhost,127.0.0.1", "comma-separated endpoint host names/IPs")
	flag.Parse()
	if err := clustertls.Generate(*dir, strings.Split(*hosts, ",")); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	fmt.Println("Created private development certificates in", *dir)
}
