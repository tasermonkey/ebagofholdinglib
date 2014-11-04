package ebagofholdinglib

import (
	"fmt"
	"github.com/emicklei/go-restful"
	"io"
	"os"
)

func hello(req *restful.Request, resp *restful.Response) {
	io.WriteString(resp, "world")
}

type StoreResult struct {
	err string
}

func CheckClose(c io.Closer, errp *error) {
	err := c.Close()
	if err != nil && *errp == nil {
		*errp = err
	}
}

// /{fileUUID}
// POST Body:
//   blob
func store(req *restful.Request, resp *restful.Response) {
	var err error
	src := req.Request.Body
	defer CheckClose(src, &err)
	out, err := os.Create(req.PathParameter("fileUUID"))
	if err != nil {
		fmt.Print(err)
	}
	defer CheckClose(out, &err)
	io.Copy(out, src)
	resp.WriteEntity(StoreResult{err.Error()})
}

func read(req *restful.Request, resp *restful.Response) {

}
