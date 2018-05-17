package chansmr

import (
	"io"
)
type ChanReader struct {
	ch chan byte
	err error
}

//buffer size make(chan byte, 1024)

func NewChanReader(reader chan byte) *ChanReader {
	return &ChanReader{reader, nil}
}


func (r *ChanReader) Chan() <-chan byte {
	return r.ch
}

func (r *ChanReader)  Read(p []byte) (n int, err error) {
	n = 0
    loop := true
	if(r.err == io.EOF){
	    return 0, io.EOF
    }
	for r.err != io.EOF && loop && n < len(p) {
		select {
		case b, ok := <-r.ch:
			if ok {
				p[n] = b
                n++

            } else {
                r.err = io.EOF
                break;
			}
        default:
            loop = false
            break
        }
	}
	//dlog.Printf("Read %d bytes", n)
	return n, err
}

func (r *ChanReader) Close() error {
    _, ok := (<-r.ch)
	if ok {
        close(r.ch)
        return nil
    } else{
        return nil
    }
}

//func main() {
//    fmt.Printf("Started\n")
//
//    testChan := make(chan byte, 100)
//
//    testChan <- 65
//    testChan <- 66
//    testChan <- 67
//    close(testChan)
//
//    reader := NewChanReader(testChan)
//    defer reader.Close()
//
//    buf := make([]byte, 32)
//    ioCount, _ := io.ReadAtLeast(reader, buf, 2)
//    s := string(buf[:ioCount])
//    fmt.Printf("%v\n", s)
//
//    _, eof := reader.Read(buf)
//    if eof == io.EOF{
//        fmt.Println("Success\n")
//    } else{
//        fmt.Println("Error\n")
//    }
//
//
//
//}
