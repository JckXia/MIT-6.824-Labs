package main

import "fmt"
 
// import "io/ioutil"

import "log"
import "os"
import "io/ioutil"
import "bufio"
import "strings"
 
func collecKvPairs(fileName string, kvPairs map [string] [] string) {
	file, err := os.Open(fileName)
	fmt.Println(fileName)
	if err != nil {
		fmt.Println("Err")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		 
		words := strings.Fields(scanner.Text())
		 
		kvPairs[words[0]] = append(kvPairs[words[0]], words[1])
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {


	files, err := ioutil.ReadDir("./reduce-tasks-0")

	if err != nil {
   		 //log.Fatal(err)
		fmt.Println("Error! ")
	}
	// t := make(map[string][]string)
	// t["abc"] = append(t["abc"], "abde")
	// t["abc"] = append(t["abc"], "erg")
	// fmt.Println(t["abc"][0])
    // fmt.Println(t["abc"][1])
	// s := append(t["abc"], "abc") 
	// fmt.Println(s["abc"])
	kvPairs := make(map[string][]string)
	for _, f := range files {

    	// fmt.Println(f.Name())
		pathName := "./reduce-tasks-0/"+f.Name()
		// fmt.Println(pathName)
		collecKvPairs(pathName, kvPairs)
	}
	fmt.Println(kvPairs)
}
