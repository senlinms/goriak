package goriak

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	riak "github.com/basho/riak-go-client"
)

func TsQuery(query string, session *Session) error {
	cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(``).Build()
	if err != nil {
		return err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return err
	}

	if !cmd.Success() {
		return errors.New("TsQuery failed")
	}

	return nil
}

func TsWrite(table string, object interface{}, session *Session) error {
	// Map TsCell ID to Riak struct Field
	cellIDs := make(map[int]int)

	var maxID int

	r := reflect.TypeOf(object)

	num := r.NumField()
	for i := 0; i < num; i++ {
		tag := r.Field(i).Tag.Get("goriakts")
		log.Println(tag)
		tagPiece := strings.Split(tag, ",")

		if len(tagPiece) != 2 {
			return errors.New("Unexpected goriakts tag")
		}

		tagNum, err := strconv.Atoi(tagPiece[0])
		if err != nil {
			return errors.New("Unexpected goriakts tag")
		}

		cellIDs[tagNum] = i

		// Keep track of the largest num found
		if tagNum > maxID {
			maxID = tagNum
		}
	}

	row := make([]riak.TsCell, maxID+1)

	rVal := reflect.ValueOf(object)

	for tsCellID, fieldID := range cellIDs {
		f := rVal.Field(fieldID)

		switch r.Field(fieldID).Type.Kind() {
		case reflect.String:
			row[tsCellID] = riak.NewStringTsCell(f.String())
		case reflect.Int64:
			row[tsCellID] = riak.NewSint64TsCell(f.Int())
		case reflect.Struct:
			if ts, ok := f.Interface().(time.Time); ok {
				row[tsCellID] = riak.NewTimestampTsCell(ts)
			} else {
				log.Println("Unknown Type:", r.Field(fieldID).Type.Kind())
			}
		default:
			log.Println("Unknown Type:", r.Field(fieldID).Type.Kind())
		}
	}

	cmd, err := riak.NewTsStoreRowsCommandBuilder().
		WithTable(table).
		WithRows([][]riak.TsCell{row}).
		Build()
	if err != nil {
		return err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return err
	}

	if !cmd.Success() {
		return errors.New("TsWrite command execute failed")
	}

	return nil
}

func TsRead(query string, objects []interface{}, session *Session) error {
	return nil
}

/*func TSWrite(session *Session) {
	row := []riak.TsCell{
		riak.NewSint64TsCell(100),
		riak.NewStringTsCell("Foo"),
		riak.NewTimestampTsCell(time.Now()),
	}

	cmd, err := riak.NewTsStoreRowsCommandBuilder().
		WithTable("UserLog").
		WithRows([][]riak.TsCell{row}).
		Build()
	if err != nil {
		panic(err)
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		panic(err)
	}

	log.Printf("%+v", cmd)
	log.Printf("%+v", cmd.(*riak.TsStoreRowsCommand))
}

func TSRead(session *Session) {
	q := fmt.Sprintf(`SELECT id, action, time
        FROM UserLog
        WHERE id = 100
        AND time > %d
        AND time <= %d`,
		riak.ToUnixMillis(time.Now().Add(-time.Second*10)),
		riak.ToUnixMillis(time.Now()))

	log.Println(q)

	cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(q).Build()
	if err != nil {
		panic(err)
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		panic(err)
	}

	res := cmd.(*riak.TsQueryCommand).Response

	log.Printf("%+v", cmd)
	log.Printf("%+v", res)
	log.Printf("%+v", cmd.(*riak.TsQueryCommand).Success())

	for _, c := range res.Columns {
		log.Printf("%+v %+v", c.GetName(), c.GetType())
	}

	for _, row := range res.Rows {
		log.Printf("%+v", row)
		for _, r := range row {
			switch r.GetDataType() {
			case "SINT64":
				log.Printf("%+v %+v", r.GetDataType(), r.GetSint64Value())
			case "VARCHAR":
				log.Printf("%+v %+v", r.GetDataType(), r.GetStringValue())
			case "TIMESTAMP":
				log.Printf("%+v %+v %+v", r.GetDataType(), r.GetTimeValue(), r.GetTimeValue().UnixNano())
				log.Printf("%+v %+v", r.GetDataType(), r.GetTimestampValue())
			default:
				log.Printf("%+v", r.GetDataType())
			}

		}
	}
}
*/
