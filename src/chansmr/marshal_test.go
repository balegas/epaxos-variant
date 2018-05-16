package chansmr

import (
    "state"
    "bufio"
    "testing"
    "fmt"
)

func TestMarshal(t *testing.T){
    testChan := make(chan byte, 16)
    writer := NewChanWriter(testChan)
    reader := NewChanReader(testChan)

    msg := state.Key(123)
    w := bufio.NewWriter(writer)
    msg.Marshal(w)
    msg = state.Key(124)
    w.Flush()
    r := bufio.NewReader(reader)
    msg.Unmarshal(r)

    fmt.Printf("%v", msg)


}
