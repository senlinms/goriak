package goriak

import (
	"fmt"
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
		Time   time.Time `goriakts:"2,time"`
		Action string    `goriakts:"1,action"`
	}

	err := TsWrite("UserLog", testType{
		ID:     1337,
		Time:   time.Now(),
		Action: "yolo",
	}, con())

	if err != nil {
		t.Error(err)
	}
}

func TestTSRead(t *testing.T) {
	type testType struct {
		ID     int64     `goriakts:"0,id"`
		Action string    `goriakts:"1,action"`
		Time   time.Time `goriakts:"2,time"`
	}

	var res []testType

	query := fmt.Sprintf(
		"SELECT * FROM UserLog WHERE id = %d AND time >= %d AND time < %d",
		1337,
		TsTimeFormat(time.Now().Add(-time.Second*10)),
		TsTimeFormat(time.Now().Add(time.Second)),
	)

	t.Log(query)

	err := TsRead(query, &res, con())

	if err != nil {
		t.Error(err)
	}

	t.Logf("%+v", res)
}

func TestTsInsert10(t *testing.T) {
	err := TsQuery(`CREATE TABLE TestTsInsert10
(
   id      SINT64 NOT NULL,
   action       VARCHAR   NOT NULL,
   time         TIMESTAMP NOT NULL,
   PRIMARY KEY (
     (id, QUANTUM(time, 15, 'm')),
      id, time
   )
)`, con())

	if err != nil {
		t.Fatal(err)
	}

	type testType struct {
		ID     int64     `goriakts:"0,id"`
		Action string    `goriakts:"1,action"`
		Time   time.Time `goriakts:"2,time"`
	}

	for i := 0; i < 10; i++ {
		err = TsWrite("TestTsInsert10", testType{
			ID:     400,
			Action: "TestSuite",
			Time:   time.Now(),
		}, con())
		if err != nil {
			t.Error(err)
		}
	}
}
