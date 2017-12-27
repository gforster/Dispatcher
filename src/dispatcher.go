package main

import (
	"os"
	"fmt"
	"bytes"
	"strconv"
        "io/ioutil"
        //"encoding/json"
	"github.com/jeffail/gabs"
	"net/http"
	"net/smtp"
	"github.com/gorilla/mux"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sns"
)

var sqsname string
var sqsregion string
var snstopic string
var snstopicarn string

var useaws bool

type ActionMessage struct {
	Host string
	Action string
	Contact string
}

func handleSetUseAWS(w http.ResponseWriter, r *http.Request) {
	tmpuseaws := r.URL.Query().Get("useaws")
	if tmpuseaws == "" {
		fmt.Fprintf(w, "failed to set useaws")
		Log("failed to set useaws: left blank", 0)
		return
	}

	flag, err := strconv.ParseBool(tmpuseaws)
	if err != nil {
		fmt.Fprintf(w, "failed to set useaws: value is not true or false")
		Log("failed to set useaws: " + err.Error(), 0)
		return
	}

	useaws = flag 

	if flag {
		fmt.Fprintf(w, "now using aws")
	} else {
		fmt.Fprintf(w, "aws not in use any more")
	}
}

func handleSetSNSTopic(w http.ResponseWriter, r *http.Request) {
	tmpsnstopic := r.URL.Query().Get("snstopic")
	if tmpsnstopic == "" {
		fmt.Fprintf(w, "failed to set sns topic")
		Log("failed to set sns topic: left blank", 0)
		return
	}

	snstopic = tmpsnstopic
	fmt.Fprintf(w, "topic set to: " + snstopic)
}

func handleSetSNSTopicARN(w http.ResponseWriter, r *http.Request) {
	tmpsnstopicarn := r.URL.Query().Get("snstopicarn")
	if tmpsnstopicarn == "" {
		fmt.Fprintf(w, "failed to set sns topic arn")
		Log("failed to set sns topic arn: left blank", 0)
		return
	}

	snstopicarn = tmpsnstopicarn
	fmt.Fprintf(w, "topic set to: " + snstopicarn)
}

func handleSetSQSName(w http.ResponseWriter, r *http.Request) {
        tmpsqsname := r.URL.Query().Get("sqsname")
        if tmpsqsname == "" {
		fmt.Fprintf(w, "failed to set queue name")
		Log("failed to set queue name: left blank", 0)
		return
	}

	sqsname = tmpsqsname
	fmt.Fprintf(w, "queue name set to: " + sqsname)
}

func handleSetSQSRegion(w http.ResponseWriter, r *http.Request) {
        tmpsqsregion := r.URL.Query().Get("sqsregion")
	if tmpsqsregion == "" {
		fmt.Fprintf(w, "failed to set region")
		Log("failed to set queue region: left blank", 0)
		return
	}

	sqsregion = tmpsqsregion
	fmt.Fprintf(w, "region set to: " + sqsregion)
}

func handleWhoAreYou(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "dispatcher")
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "pong")
}

func handleTrigger(w http.ResponseWriter, r *http.Request) {
	var err error

	Host := r.URL.Query().Get("host")
	Action := r.URL.Query().Get("action")
	Contact := r.URL.Query().Get("contact")

	if Host == "" {
		Log("failed to trigger: missing host data", 0)
		fmt.Fprintf(w, "failed to trigger: missing host data")
		return
	}

	if Action == "" {
		Log("failed to trigger: missing action data", 0)
		fmt.Fprintf(w, "failed to trigger: missing action data")
		return
	}

	if Contact == "" {
		Log("failed to trigger: missing contact data", 0)
		fmt.Fprintf(w, "failed to trigger: missing contact data")
		return
	}

	queue, _ := GetSQSUrl(sqsname)

	if queue == "" {
		queue, err = CreateSQS(sqsname)
		if err != nil {
			fmt.Fprintf(w, "failed to handle event")
			Log("failed to create Queue on AWS: " + err.Error(), 0)
			return
		}
	}

	action := ActionMessage{}
	action.Host = Host
	action.Action = Action
	action.Contact = Contact

	messageid, err := SendSQSMessage(queue, &action)
	if err != nil {
		fmt.Fprintf(w, "failed to insert message into sqs: " + err.Error())
		return
	}

	fmt.Fprintf(w, "successfully added message to sqs: " + messageid)

}

func handleSplunkTrigger(w http.ResponseWriter, r *http.Request) {
	bsplunkmsg, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		fmt.Fprintf(w, err.Error(), 500)
		Log("couldn't read JSON Post From Splunk: " + err.Error(), 0)
		return
	}

	jsonParsed, err := gabs.ParseJSON(bsplunkmsg)

	dsthost, ok := jsonParsed.Path("result.dsthost").Data().(string)
	if ! ok {
		fmt.Fprintf(w, "failed to parse result from splunk json: missing dsthost")
		Log("failed to parse result from splunk json: missing dsthost", 0)
		return
	}

	dstfn, ok := jsonParsed.Path("result.dstfn").Data().(string)
	if ! ok {
		fmt.Fprintf(w, "failed to parse result from splunk json: missing dstfn")
		Log("failed to parse result from splunk json: missing dstfn", 0)
		return
	}

	dstcontact, ok := jsonParsed.Path("result.dstcontact").Data().(string)
	if ! ok {
		Log("dstcontact missing: setting to unixadmins@creditacceptance.com", 0)
		dstcontact = "unixadmins@creditacceptance.com"
	}

	queue, _ := GetSQSUrl(sqsname)

	if queue == "" {
		queue, err = CreateSQS(sqsname)
		if err != nil {
			fmt.Fprintf(w, "failed to handle event")
			Log("failed to create Queue on AWS: " + err.Error(), 0)
			return
		}
	}

	action := ActionMessage{}
	action.Host = dsthost 
	action.Action = dstfn
	action.Contact = dstcontact 

	messageid, err := SendSQSMessage(queue, &action)
	if err != nil {
		fmt.Fprintf(w, "failed to insert message into sqs: " + err.Error())
		return
	}

	Log("inserted message into SQS: " + messageid, 0)
	fmt.Fprintf(w, "successfully added message to sqs: " + messageid)
}

func SendSQSMessage(sqsname string, actionmsg *ActionMessage) (string, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(sqsregion)},
	)

	// Create a SQS service client.
	svc := sqs.New(sess)

	result, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageAttributes: map[string]*sqs.MessageAttributeValue {
			"Host": &sqs.MessageAttributeValue {
				DataType: aws.String("String"),
				StringValue: aws.String(actionmsg.Host),
			},
			"Action": &sqs.MessageAttributeValue {
				DataType: aws.String("String"),
				StringValue: aws.String(actionmsg.Action),
			},
			"Contact": &sqs.MessageAttributeValue {
				DataType: aws.String("String"),
				StringValue: aws.String(actionmsg.Contact),
			},
		},

		MessageBody: aws.String("stuff"),
		QueueUrl: &sqsname,
	})

	if err != nil {
		Log("failed to send sqs message: " + err.Error(), 0)
		return "", err
	}

	return *result.MessageId, nil

}

func GetSQSUrl(sqsname string) (string, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(sqsregion)},
	)

	// Create a SQS service client.
	svc := sqs.New(sess)

	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(sqsname),
	})

	if err != nil {
		Log("failed to get queue on aws, probably doesn't exist: " + err.Error(), 1)
		return "", err
	}

	return *result.QueueUrl, nil
}


func CreateSQS(sqsname string) (string, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(sqsregion)},
	)

	// Create a SQS service client.
	svc := sqs.New(sess)

	result, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(sqsname),
		Attributes: map[string]*string{
			"DelaySeconds":           aws.String("10"),
			"MessageRetentionPeriod": aws.String("86400"),
		},
	})

	if err != nil {
		Log("failed to create queue on aws... check creds file? : " + err.Error(), 0)
		return "", err
	}	

	return *result.QueueUrl, nil
}

func Log(message string, level int) error {
	file, err := os.OpenFile("./dispatcher.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		_, err := SendSNSMessage("Dispatcher: Failed To Write To Log", snstopicarn)
		if err != nil {
			err := SendSMTPMessage("smtp.cac.com", "dispatcher@creditacceptance.com", "unixadmins@creditacceptance.com", "Can't Write To Log", err.Error())
			if err != nil {
				fmt.Println("All Forms Of Communication Are Down")
				return err
			}
		}

		return err	
	}
	defer file.Close()


	_, err = file.WriteString(message)
	if err != nil {
                _, err := SendSNSMessage("Dispatcher: Failed To Write To Log", snstopicarn)
                if err != nil {
                        err := SendSMTPMessage("smtp.cac.com", "dispatcher@creditacceptance.com", "unixadmins@creditacceptance.com", "Can't Write To Log", err.Error())
			if err != nil {
				fmt.Println("All Forms Of Communication Are Down")
				return err
			}
                }

                return err  
	}

	return nil
}

func SendSMTPMessage(mailserver string, from string, to string, subject string, body string) error {
	connection, err := smtp.Dial(mailserver)
	if err != nil {
		return err
	}
	defer connection.Close()

	connection.Mail(from)
	connection.Rcpt(to)

	wc, err := connection.Data()
	if err != nil {
		return err
	}
	defer wc.Close()

	body = "To: " + to + "\r\nFrom: " + from + "\r\nSubject: " + subject + "\r\n\r\n" + body

	buf := bytes.NewBufferString(body)
	_, err = buf.WriteTo(wc)
	if err != nil {
		return err
	}

	return nil
}

func SendSNSMessage(message string, snstpcarn string) (string, error) {
        svc := sns.New(session.New(&aws.Config{Region: aws.String(sqsregion)}))

        params := &sns.PublishInput{
                Message: aws.String(message),
                TopicArn: aws.String(snstpcarn),
        }

        resp, err := svc.Publish(params)  

        if err != nil {                    
                return "", err
        }

	return *resp.MessageId, nil
}

func main() {
	
	sqsname = "dispatcher"
	sqsregion = "us-east-2"
	snstopic = "dispatcher"
	snstopicarn = "arn:aws:sns:us-east-2:326315424881:dispatcher"

        router := mux.NewRouter()
        router.HandleFunc("/whoareyou", handleWhoAreYou)
        router.HandleFunc("/ping", handlePing)
	router.HandleFunc("/setsqsname", handleSetSQSName)
	router.HandleFunc("/setsqsregion", handleSetSQSRegion)
	router.HandleFunc("/setsnstopic", handleSetSNSTopic)
	router.HandleFunc("/setuseaws", handleSetUseAWS)
        router.HandleFunc("/trigger", handleTrigger)
        router.HandleFunc("/splunktrigger", handleSplunkTrigger)

        err := http.ListenAndServe(":80", router)
        if err != nil {
                fmt.Println("ListenAndServe: ", err)
        }
}
