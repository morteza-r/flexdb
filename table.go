package flexdb

import "sync"

type Table struct {
	Docs    sync.Map //map[uint]Doc
	Indexes sync.Map //map[string]Index
}
