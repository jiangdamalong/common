package sttparser

import (
	"sync"
)

type StackItemGroup struct {
	v         []IndexStackSt
	groupSize int
	usedIndex int

	next *StackItemGroup
}

type StackItemManager struct {
	groupSize int
	lock      sync.Mutex
	allocator *StackItemAllocator
	objGroup  *StackItemGroup

	allocatorNum int
	groupNum     int

	maxAllocNum int
	maxGroupNum int
	freeSeq     int
}

type StackItemAllocator struct {
	objs   *StackItemGroup
	objmgr *StackItemManager

	next *StackItemAllocator
}

func (o *StackItemGroup) init(groupSize int) {
	o.v = make([]IndexStackSt, groupSize)
	o.groupSize = groupSize
	o.next = nil
	o.usedIndex = 0
}

func (m *StackItemAllocator) Alloc() *IndexStackSt {
	if m.objs == nil {
		m.objs = m.objmgr.GetStackItemGroup()
	}
	if m.objs.usedIndex >= m.objs.groupSize {
		objs := m.objmgr.GetStackItemGroup()
		objs.next = m.objs
		m.objs = objs
	}

	m.objs.usedIndex++
	return &(m.objs.v[m.objs.usedIndex-1])
}

func (m *StackItemManager) Init(groupSize int, maxAllocatorNum int, maxGroupNum int) {
	m.groupSize = groupSize
	m.maxAllocNum = maxAllocatorNum
	m.maxGroupNum = maxGroupNum
}

func (m *StackItemManager) GetStackItemGroup() *StackItemGroup {
	m.lock.Lock()
	if m.objGroup == nil {
		m.lock.Unlock()
		objs := new(StackItemGroup)
		objs.init(m.groupSize)
		return objs
	}
	e1 := m.objGroup.next
	e := m.objGroup
	e.next = nil
	e.usedIndex = 0
	m.objGroup = e1
	m.groupNum--
	m.lock.Unlock()
	return e
}

func (m *StackItemManager) GetAllocator() *StackItemAllocator {
	m.lock.Lock()
	if m.allocator == nil {
		m.lock.Unlock()
		e := new(StackItemAllocator)
		e.objs = nil
		e.next = nil
		e.objmgr = m
		//fmt.Printf("new allocator\n")
		return e
	} else {
		e := m.allocator
		m.allocator = m.allocator.next
		m.allocatorNum--
		e.next = nil
		m.lock.Unlock()
		//fmt.Printf("reuse\n")
		return e
	}
}

func (m *StackItemManager) CleanAllocator(obja *StackItemAllocator) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.freeSeq++
	if m.freeSeq%1024 == 0 {
		for e1 := obja.objs; e1 != nil; {
			if e1.usedIndex > 0 {
				e1.v[e1.usedIndex-1].next = nil
			}
			e1 = e1.next
		}
		return
	}

	for e := obja.objs; e != nil; {
		e1 := e
		e = e.next

		if m.groupNum < m.maxGroupNum {
			e1.usedIndex = 0
			e1.next = m.objGroup
			m.objGroup = e1
			m.groupNum++
		} else {
			if e1.usedIndex > 0 {
				e1.v[e1.usedIndex-1].next = nil
			}
		}
	}
	obja.objs = nil

	if m.allocatorNum < m.maxAllocNum {
		obja.next = m.allocator
		m.allocator = obja
		m.allocatorNum++
	}

	obja = nil
}
