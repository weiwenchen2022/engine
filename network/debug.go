package network

import (
	"fmt"
	"net/http"
	"sort"
	"text/template"
)

/*
	Some HTML presented at http://machine:port/debug/rpc
	Lists services, their methods, and some statistics, still rudimentary.
*/

const debugText = `
<html>
	<body>
	<title>Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range .Method}}
			<tr>
			<td align=left font=fixed>{{.Name}}({{.Type.ArgType}}) ({{.Type.ReplyType}}, error)</td>
			<td align=center>{{.Type.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
</html>`

var debug = template.Must(template.New("Network debug").Parse(debugText[1:]))

// If set, print log statements for internal and I/O errors.
var debugLog = false

type debugMethod struct {
	Type *methodType
	Name string
}

type debugService struct {
	Service *service
	Name    string
	Method  []debugMethod
}

type debugHTTP struct {
	*Server
}

// Runs at /debug/network
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	// Build a sorted version of the data.
	var services []debugService
	server.serviceMap.Range(func(snamei, svci any) bool {
		svc := svci.(*service)
		ds := debugService{svc, snamei.(string), make([]debugMethod, 0, len(svc.method))}
		for mname, method := range svc.method {
			ds.Method = append(ds.Method, debugMethod{method, mname})
		}
		sort.Slice(ds.Method, func(i, j int) bool {
			return ds.Method[i].Name < ds.Method[j].Name
		})
		services = append(services, ds)
		return true
	})
	sort.Slice(services, func(i, j int) bool {
		return services[i].Name < services[j].Name
	})

	if err := debug.Execute(w, services); err != nil {
		fmt.Fprintln(w, "network: error executing template:", err)
	}
}
