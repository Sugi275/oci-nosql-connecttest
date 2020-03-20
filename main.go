package main

import (
	"fmt"
	"os"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func main() {
	fmt.Println("Process Start")

	provider, err := iam.NewSignatureProviderFromFile("~/.oci/config", "", "", "ocid1.compartment.oc1..your ocid")
	if err != nil {
		fmt.Printf("failed to create new SignatureProvider: %v\n", err)
		return
	}

	cfg := nosqldb.Config{
		Region:                "us-ashburn-1",
		AuthorizationProvider: provider,
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}

	defer client.Close()

	// テーブル名を指定
	tableName := "test"

	// データの格納ｓ
	mapVals := types.ToMapValue("pk1", 333)
	mapVals.Put("cl1", 444)
	putReq := &nosqldb.PutRequest{
		TableName: tableName,
		Value:     mapVals,
	}
	putRes, err := client.Put(putReq)
	ExitOnError(err, "Can't put single row")
	fmt.Printf("Put row: %v\nresult: %v\n", putReq.Value.Map(), putRes)

	// データの取得
	key := &types.MapValue{}
	key.Put("pk1", 333)
	getReq := &nosqldb.GetRequest{
		TableName: tableName,
		Key:       key,
	}
	getRes, err := client.Get(getReq)
	ExitOnError(err, "Can't get single row")
	fmt.Printf("Got row: %v\n", getRes.ValueAsJSON())

	// Delete the row
	// delReq := &nosqldb.DeleteRequest{
	// 	TableName: tableName,
	// 	Key:       key,
	// }
	// delRes, err := client.Delete(delReq)
	// ExitOnError(err, "Can't delete single row")
	// fmt.Printf("Deleted key: %v\nresult: %v\n", jsonutil.AsJSON(delReq.Key.Map()), delRes)
}

// ExitOnError ExitOnError
func ExitOnError(err error, msg string) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, msg, err)
	os.Exit(1)
}
