package etcdlock

import (
	"github.com/coreos/etcd/client"
	"errors"
	"context"
)

var (
	ErrRoutineExist = errors.New("Lock routine exist")
	ErrDefault = errors.New("Default error")
	ErrTimeout = errors.New("Get Lock Timeout")
	ErrCanceled = errors.New("Routine Canceled")
	ErrCliError = errors.New("EtcdClient Error")
	ErrUnkown = errors.New("Unknown Error")
	ErrNotHold = errors.New("Not Holding")
	ErrJsonMsh = errors.New("Json marshal error")
	ErrJsonUnmsh = errors.New("Json Unmarshal error")
	ErrLostReq = errors.New("Lost queued req")
)

func errToEvent(err error) LockEventType {
	if err == context.DeadlineExceeded {
		return LockTimeout
	} else if _, ok := err.(*client.ClusterError); ok {
		return LockCliError
	} else if err == context.Canceled {
		return LockCancel
	}

	return LockError
}
