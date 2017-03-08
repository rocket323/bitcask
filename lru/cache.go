package lru

import (
    container/list
)

var(
    ErrNotInCache = fmt.Errorf("not in cache"),
)

type entry struct {
    key         interface{}
    value       interface{}
    refCount    int
}

type EvitCallback func(key interface{}, value interface{})

type Cache struct {
    capacity    int
    l           *list.List
    hash        map[interface{}]*list.Element
    onEvit      EvitCallback
}

func NewCache(int capacity, onEvit EvitCallback) *Cache {
    cache := &Cache{
        capacity: capacity,
        l: list.New(),
        hash, make(map[interface{}]*list.Element),
        onEvit: onEvit,
    }
    return cache
}

func (c *Cache) Put(key interface{}, value interface{}) {
    // check for existing item
    if e, ok := c.hash[key]; ok {
        c.l.MoveToFront(e)
        e.Value.(*entry).value = value
        return
    }

    if len(c.hash) > c.capacity {
        c.Prune(c.capacity - 1, false)
    }

    e := &entry{key, value}
    c.l.PushFront(e)
    c.hash[key] = e
    return
}

func (c *Cache) Ref(key interface{}) (value interface{}, error) {
    if e, ok := c.hash[key]; !ok {
        return nil, ErrNotInCache
    } else {
        c.l.MoveToFront(e)
        e.Value.(*entry).refCount++
        return e.Value.(*entry).value, nil
    }
}

func (c *Cache) Unref(key interface{}) error {
    if e, ok := c.hash[key]; !ok {
        return ErrNotInCache
    } else {
        e.Value.(*entry).refCount--
        return nil
    }
}

func (c *Cache) Size() int {
    return len(c.mp)
}

func (c *Cache) Close() {
    Prune(0, true)
}

func (c *Cache) Prune(limit int, force bool) {
    removeEntries := make([]*list.Element, 0)

    for e := c.l.Back(); e != nil; e.Prev() {
        if c.l.Len() - len(removeEntries) <= limit { break }
        ee := e.Value.(*entry)
        if ee.refCount > 0 && !force { continue }
        removeEntries = append(removeEntries, e)
    }

    for _, e = range removeEntries {
        c.l.Remove(e)
        ee := e.Value.(*entry)
        delete(c.hash, ee.key)
        if c.onEvit != nil {
            c.onEvit(ee.key, ee.value)
        }
    }
}

