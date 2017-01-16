package goriak

import (
	"testing"
	"time"
)

func TestTSCreate(t *testing.T) {
	TsQuery(`CREATE TABLE UserLog
(
   id      SINT64 NOT NULL,
   action       VARCHAR   NOT NULL,
   time         TIMESTAMP NOT NULL,
   PRIMARY KEY (
     (id, QUANTUM(time, 15, 'm')),
      id, time
   )
)`, con())
}

func TestTSWrite(t *testing.T) {
	type testType struct {
		ID     int64     `goriakts:"0,id"`
		Action string    `goriakts:"2,action"`
		Time   time.Time `goriakts:"1,time"`
	}

	TsWrite("UserLog", testType{
		ID:     1337,
		Time:   time.Now(),
		Action: "yolo",
	}, con())
}

func TestTSRead(t *testing.T) {
	//TSCreate(con())
	//TSWrite(con())
	//TSRead(con())
}

func TestTS(t *testing.T) {

}
