package main


 
import "os"
import "bufio"
import "log"
import "io"
import "fmt"
import "strings"
func readFileLines() {
    f, err := os.OpenFile("imr-2-0", os.O_RDONLY, os.ModePerm)
    if err != nil {
        log.Fatalf("open file error: %v", err)
        return
    }
    defer f.Close()

    rd := bufio.NewReader(f)
    for {
        line, err := rd.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                break
            }

            log.Fatalf("read file line error: %v", err)
            return
        }
		// fmt.Println(line, "    ", len(line))
		words := strings.Fields(line)
		fmt.Println(words[0], " ", words[1])
        _ = line  // GET the line string
    }
}

func main() {
	readFileLines()
}