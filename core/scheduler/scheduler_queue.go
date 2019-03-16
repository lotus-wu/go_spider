// Copyright 2014 Hu Cong. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//
package scheduler

import (
	"container/list"
	"crypto/md5"
	"sync"

	"github.com/hu17889/go_spider/core/common/request"
	//"fmt"
)

type Elem struct {
	Ele    *list.Element
	Polled bool
}

type QueueScheduler struct {
	locker *sync.Mutex
	rm     bool
	rmKey  map[[md5.Size]byte]*Elem
	queue  *list.List
}

func NewQueueScheduler(rmDuplicate bool) *QueueScheduler {
	queue := list.New()
	rmKey := make(map[[md5.Size]byte]*Elem)
	locker := new(sync.Mutex)
	return &QueueScheduler{rm: rmDuplicate, queue: queue, rmKey: rmKey, locker: locker}
}

func (this *QueueScheduler) Push(requ *request.Request) {
	this.locker.Lock()
	var key [md5.Size]byte
	if this.rm {
		key = md5.Sum([]byte(requ.GetUrl()))
		if _, ok := this.rmKey[key]; ok {
			//if elem.Polled == true {
			this.locker.Unlock()
			return
			//}
		}
	}
	e := this.queue.PushBack(requ)
	if this.rm {
		this.rmKey[key] = &Elem{Ele: e, Polled: false}
	}
	this.locker.Unlock()
}

func (this *QueueScheduler) Poll() *request.Request {
	this.locker.Lock()
	if this.queue.Len() <= 0 {
		this.locker.Unlock()
		return nil
	}
	e := this.queue.Front()
	requ := e.Value.(*request.Request)
	key := md5.Sum([]byte(requ.GetUrl()))
	this.queue.Remove(e)
	if this.rm {
		delete(this.rmKey, key)
		this.rmKey[key] = &Elem{Ele: nil, Polled: true}
		//本quene的特色是使用过的key都不可以再次插入，key测试的map不删除数据即可，为了减少内存使用，这里删除掉elem数据
	}
	this.locker.Unlock()
	return requ
}

func (this *QueueScheduler) Count() int {
	this.locker.Lock()
	len := this.queue.Len()
	this.locker.Unlock()
	return len
}
