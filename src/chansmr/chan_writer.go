package chansmr

type ChanWriter struct {
	ch chan byte
}

//buffer size make(chan byte, 1024)

func NewChanWriter(writer chan byte) *ChanWriter {
	return &ChanWriter{writer}
}


func (w *ChanWriter) Chan() <-chan byte {
	return w.ch
}

func (w *ChanWriter) Write(p []byte) (int, error) {
	n := 0
	for _, b := range p {
		w.ch <- b
		n++
	}
	//dlog.Printf("wrote %d bytes", n)
	return n, nil
}

func (w *ChanWriter) Close() error {
	close(w.ch)
	return nil
}

//func main() {
//	testChan := make(chan byte)
//	writer := NewChanWriter(testChan)
//	go func() {
//		defer writer.Close()
//		writer.Write([]byte("Stream "))
//		writer.Write([]byte("me!"))
//	}()
//	for c := range writer.Chan() {
//		fmt.Printf("%c", c)
//	}
//	fmt.Println()
//}
