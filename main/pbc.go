package main



import "pbservice"
import "os"
import "fmt"

func usage() {
	fmt.Printf("Usage: pbc viewport key\n")
	fmt.Printf("       pbc viewport key value\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) == 3 {
		// get
		ck := pbservice.MakeClerk(os.Args[1], "")
		v := ck.Get(os.Args[2])
		fmt.Printf("%v\n", v)
	} else if len(os.Args) == 4 {
		// put
		ck := pbservice.MakeClerk(os.Args[1], "")
		ck.Put(os.Args[2], os.Args[3])
	} else {
		usage()
	}
}
