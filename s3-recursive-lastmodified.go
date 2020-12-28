package main

import (
	"flag"
	"fmt"
	//"sync"

	//"runtime/debug"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	//debug.SetMaxThreads(1000)
	var bucket, delim, root_prefix string
	var worker_count int
	//var maxKeys int64

	// TODO create a worker group instead of waitgroup (prevent dead stop from single request timing out)
	//var counter int64

	// TODO these should be outside
	//flag.StringVar(&region, "region", "us-east-1", "AWS region")
	flag.StringVar(&bucket, "bucket", "s3-bucket", "Bucket name")
	flag.StringVar(&delim, "delimiter", "/", "Delim")
	flag.StringVar(&root_prefix, "root_prefix", "", "Root prefix")
	flag.IntVar(&worker_count, "worker_count", 5000, "Worker count")
	flag.Parse()

	// TODO I believe regions are auto-parsed from config?
	//svc := s3.New(session.New(), &aws.Config{
	//	Region: aws.String(region),
	//})

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := s3.New(sess)

	var latestObj *s3.Object

	tickets := make(chan bool, worker_count)
	for i := 1; i <= worker_count; i++ {
		tickets <- true
	}
	// TODO add context object
	latestObj, _ = listObjectsPrefix(svc, bucket, delim, root_prefix, tickets)

	fmt.Println(fmt.Sprintf("BUCKET %s", bucket))
	if latestObj != nil {
		fmt.Println(fmt.Sprintf("KEY %s", *latestObj.Key))
		fmt.Println(fmt.Sprintf("MODIFIED %s", *latestObj.LastModified))
	} else {
		fmt.Println("NO OBJECTS")
	}
}

func listObjectsPrefix(svc *s3.S3, bucket string, delim string, prefix string, tickets chan bool) (oldestObject *s3.Object, err error) {
	var latestObj *s3.Object
	//var wg sync.WaitGroup

	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String(delim),
		Prefix:    aws.String(prefix),
		// TODO func (c *S3) ListObjectsV2PagesWithContext(ctx aws.Context, input *ListObjectsV2Input, fn func(*ListObjectsV2Output, bool) bool, opts ...request.Option) error
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		//fmt.Println(fmt.Sprintf("Starting '%s'", *page.Contents[0].Key))
		for _, object := range page.Contents {
			int64_zero := new(int64)
			if object.Size == int64_zero {
				continue
			}

			if latestObj == nil || object.LastModified.After(*latestObj.LastModified) {
				latestObj = object
			}
			//fmt.Println(fmt.Sprintf("%s", *object.LastModified))
			//fmt.Println(fmt.Sprintf("%s", *object.Key))
		}

		latestObjs := make(chan *s3.Object)
		var prefixCount = 0
		for _, cp := range page.CommonPrefixes {
			prefixCount += 1
			//go func(*sync.WaitGroup) {
			//fmt.Println("GOROUTINE")
			go func(c chan bool) {
				<-c
				latestObj2, err := listObjectsPrefix(svc, bucket, delim, *cp.Prefix, tickets)
				if err != nil {
					fmt.Println(fmt.Sprintf("ERROR '%v'", err))
					panic(fmt.Sprintf("Failed to check object permissions in '%s', %v", bucket, err))
					// TODO here?
					latestObjs <- nil
				}

				latestObjs <- latestObj2
				c <- true
			}(tickets)
		}

		var l *s3.Object
		for i := 0; i < prefixCount; i++ {
			l = <-latestObjs
			// TODO lock
			if latestObj == nil || l.LastModified.After(*latestObj.LastModified) {
				latestObj = l
			}
		}
		close(latestObjs)

		if latestObj != nil {
			fmt.Println(fmt.Sprintf("KEY %s", *latestObj.Key))
			fmt.Println(fmt.Sprintf("MODIFIED %s", *latestObj.LastModified))
		}

		//fmt.Println(fmt.Sprintf("%d", counter))
		return !lastPage
	})

	return latestObj, err
}
