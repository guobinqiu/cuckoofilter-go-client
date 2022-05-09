package main

import (
	"context"
	"cuckoofilter-go-client/cuckoofilter"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
)

func main() {
	db := NewClient(&Option{
		Addr:    "localhost:50051", //change to remote server ip
		Timeout: 1 * time.Second,
	})
	defer db.Close()

	if err := db.Ping().Err(); err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	filterName := "fff"
	capacity := 1000000
	ele := "guobin"

	if _, err := db.CreateFilter(filterName, uint64(capacity)); err != nil {
		fmt.Println(err)
	}

	if _, err := db.InsertElement(filterName, ele); err != nil {
		fmt.Println(err)
	}

	exist, err := db.LookupElement(filterName, ele)
	if err != nil {
		fmt.Println(err)
	}
	if exist {
		fmt.Println("do something")
	}
}

type Client struct {
	conn *grpc.ClientConn
	cfc  cuckoofilter.CuckooFilterClient
	opt  *Option
}

type Option struct {
	Addr    string
	Timeout time.Duration
	Dialer  func() (*grpc.ClientConn, error)
}

func NewClient(opt *Option) *Client {
	if opt.Dialer == nil {
		opt.Dialer = func() (*grpc.ClientConn, error) {
			ctx, _ := context.WithTimeout(context.Background(), opt.Timeout)
			return grpc.DialContext(ctx, opt.Addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
				grpc.FailOnNonTempDialError(true),
			)
		}
	}
	c := Client{opt: opt}
	c.conn, _ = opt.Dialer()
	c.cfc = cuckoofilter.NewCuckooFilterClient(c.conn)
	return &c
}

type PingErr struct {
	err error
}

func (e *PingErr) Err() error {
	return e.err
}

func (c *Client) Ping() *PingErr {
	_, err := c.opt.Dialer()
	return &PingErr{err}
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) LookupElements(filterName string, elements []string) ([]string, []string, error) {
	var res *cuckoofilter.LookupElementsResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.LookupElements(ctx, &cuckoofilter.LookupElementsRequest{FilterName: filterName, Elements: elements})
	if err != nil {
		return nil, nil, err
	}
	return res.MatchedElements, res.UnmatchedElements, nil
}

func (c *Client) DeleteFilter(filterName string) (bool, error) {
	var res *cuckoofilter.DeleteFilterResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.DeleteFilter(ctx, &cuckoofilter.DeleteFilterRequest{FilterName: filterName})
	if err != nil {
		return false, err
	}
	return res.Status.Code == 0, nil
}

func (c *Client) ListFilters() ([]string, error) {
	var res *cuckoofilter.ListFiltersResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.ListFilters(ctx, new(empty.Empty))
	if err != nil {
		return nil, err
	}
	return res.Filters, nil
}

func (c *Client) InsertElements(filterName string, elements []string) ([]string, error) {
	var res *cuckoofilter.InsertElementsResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.InsertElements(ctx, &cuckoofilter.InsertElementsRequest{FilterName: filterName, Elements: elements})
	if err != nil {
		return nil, err
	}
	return res.FailedElements, nil
}

func (c *Client) ResetFilter(filterName string) (bool, error) {
	var res *cuckoofilter.ResetFilterResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.ResetFilter(ctx, &cuckoofilter.ResetFilterRequest{FilterName: filterName})
	if err != nil {
		return false, err
	}
	return res.Status.Code == 0, nil
}

func (c *Client) DeleteElement(filterName string, element string) (bool, error) {
	var res *cuckoofilter.DeleteElementResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.DeleteElement(ctx, &cuckoofilter.DeleteElementRequest{FilterName: filterName, Element: element})
	if err != nil {
		return false, err
	}
	return res.Status.Code == 0, nil
}

func (c *Client) LookupElement(filterName string, element string) (bool, error) {
	var res *cuckoofilter.LookupElementResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.LookupElement(ctx, &cuckoofilter.LookupElementRequest{FilterName: filterName, Element: element})
	if err != nil {
		return false, err
	}
	return res.Status.Code == 0, nil
}

func (c *Client) CountElements(filterName string) (uint64, error) {
	var res *cuckoofilter.CountElementsResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.CountElements(ctx, &cuckoofilter.CountElementsRequest{FilterName: filterName})
	if err != nil {
		return 0, err
	}
	return res.Len, err
}

func (c *Client) CreateFilter(filterName string, capacity uint64) (bool, error) {
	var res *cuckoofilter.CreateFilterResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.CreateFilter(ctx, &cuckoofilter.CreateFilterRequest{FilterName: filterName, Capacity: capacity})
	if err != nil {
		return false, err
	}
	return res.Status.Code == 0, nil
}

func (c *Client) InsertElement(filterName string, element string) (bool, error) {
	var res *cuckoofilter.InsertElementResponse
	ctx, _ := context.WithTimeout(context.Background(), c.opt.Timeout)
	res, err := c.cfc.InsertElement(ctx, &cuckoofilter.InsertElementRequest{FilterName: filterName, Element: element})
	if err != nil {
		return false, err
	}
	return res.Status.Code == 0, nil
}
