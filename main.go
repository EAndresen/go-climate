package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"os"
	"time"
)

const TableName = "climate"

type ClimateNode struct {
	Id          string `json:"id"`
	Date        string `json:"date"`
	SensorId    string `json:"sensorId"`
	Temperature string `json:"temperature"`
	Humidity    string `json:"humidity"`
	Location    string `json:"location"`
}

func putNode(node ClimateNode) string {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	av, err := dynamodbattribute.MarshalMap(node)
	if err != nil {
		fmt.Println("Got error marshalling new movie item:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(TableName),
	}

	_, err = svc.PutItem(input)
	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return "Successfully added '" + node.SensorId + "' to table " + TableName
}

func getNode(id string) []ClimateNode {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	filt := expression.Name("id").Equal(expression.Value(id))
	proj := expression.NamesList(expression.Name("id"), expression.Name("date"), expression.Name("temperature"))

	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(TableName),
	}

	result, err := svc.Scan(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var returnSlice []ClimateNode

	for _, i := range result.Items {
		item := ClimateNode{}

		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			fmt.Println("Got error unmarshalling:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		returnSlice = append(returnSlice, item)
	}

	return returnSlice
}

func main() {
	node := ClimateNode{
		Id:          "3",
		Date:        time.Now().String(),
		SensorId:    "3",
		Temperature: "25",
		Humidity:    "50",
		Location:    "AWS",
	}

	putNode(node)
	climateNodes := getNode("3")

	fmt.Printf("%+v\n", climateNodes)
}
