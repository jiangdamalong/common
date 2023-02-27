package sttparser

import (
	"sync"
)

type ItemGroup struct {
	v         []StSimbolItem
	groupSize int
	usedIndex int

	next *ItemGroup
}

type ItemManager struct {
	groupSize int
	lock      sync.Mutex
	allocator *ItemAllocator
	objGroup  *ItemGroup

	allocatorNum int
	groupNum     int

	maxAllocNum int
	maxGroupNum int
	freeSeq     int
}

type ItemAllocator struct {
	objs   *ItemGroup
	objmgr *ItemManager

	next *ItemAllocator
}

func (o *ItemGroup) init(groupSize int) {
	o.v = make([]StSimbolItem, groupSize)
	o.groupSize = groupSize
	o.next = nil
	o.usedIndex = 0
}

func (m *ItemAllocator) Alloc() *StSimbolItem {
	if m.objs == nil {
		m.objs = m.objmgr.GetItemGroup()
	}
	if m.objs.usedIndex >= m.objs.groupSize {
		objs := m.objmgr.GetItemGroup()
		objs.next = m.objs
		m.objs = objs
	}

	m.objs.usedIndex++
	return &(m.objs.v[m.objs.usedIndex-1])
}

func (m *ItemManager) Init(groupSize int, maxAllocatorNum int, maxGroupNum int) {
	m.groupSize = groupSize
	m.maxAllocNum = maxAllocatorNum
	m.maxGroupNum = maxGroupNum
}

func (m *ItemManager) GetItemGroup() *ItemGroup {
	m.lock.Lock()
	if m.objGroup == nil {
		m.lock.Unlock()
		objs := new(ItemGroup)
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

func (m *ItemManager) GetAllocator() *ItemAllocator {
	m.lock.Lock()
	if m.allocator == nil {
		m.lock.Unlock()
		e := new(ItemAllocator)
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

func (m *ItemManager) CleanAllocator(obja *ItemAllocator) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.freeSeq++
	if m.freeSeq%512 == 0 {
		for e1 := obja.objs; e1 != nil; {
			if e1.v[0].prev != nil {
				e1.v[0].prev.next = nil
			}

			if e1.usedIndex > 0 && e1.v[e1.usedIndex-1].next != nil {
				e1.v[e1.usedIndex-1].prev = nil
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
			if e1.v[0].prev != nil {
				e1.v[0].prev.next = nil
			}

			if e1.usedIndex > 0 && e1.v[e1.usedIndex-1].next != nil {
				e1.v[e1.usedIndex-1].prev = nil
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
