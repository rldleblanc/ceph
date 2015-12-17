// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef WP_QUEUE_H
#define WP_QUEUE_H

#include "common/OpQueue.h"

#include <map>
#include <list>

/**
 * Weighted Priority queue with strict priority queue
 *
 * This queue attempts to be fair to all classes of
 * operations but is also weighted so that higher classes
 * get more share of the operations.
 */

template <typename T, typename K>
class WeightedPriorityQueue : public OpQueue <T, K> {
  int64_t total_priority;

  typedef std::list<std::pair<unsigned, T>> ListPairs;
  static unsigned filter_list_pairs(
    ListPairs *l, std::function<bool (T)> f,
    std::list<T> *out) {
    unsigned ret = 0;
    if (out) {
      for (typename ListPairs::reverse_iterator i = l->rbegin();
	   i != l->rend();
	   ++i) {
	if (f(i->second)) {
	  out->push_front(i->second);
	}
      }
    }
    for (typename ListPairs::iterator i = l->begin();
	 i != l->end(); ) {
      if (f(i->second)) {
	l->erase(i++);
	++ret;
      } else {
	++i;
      }
    }
    return ret;
  }

  struct SubQueue {
  private:
    typedef std::map<K, ListPairs> Classes;
    Classes q;
    typename Classes::iterator cur;
    unsigned q_size;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	cur(q.begin()),
	q_size(0) {}
    SubQueue()
      :	cur(q.begin()),
	q_size(0) {}
    void enqueue(K cl, unsigned cost, T item,
		 bool front = CEPH_OP_QUEUE_BACK) {
      if (front == CEPH_OP_QUEUE_FRONT) {
	q[cl].push_front(std::make_pair(cost, item));
      } else {
	q[cl].push_back(std::make_pair(cost, item));
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      ++q_size;
    }
    std::pair<unsigned, T> front() const {
      assert(!q.empty());
      assert(cur != q.end());
      assert(!cur->second.empty());
      return cur->second.front();
    }
    void pop_front() {
      assert(!q.empty());
      assert(cur != q.end());
      assert(!cur->second.empty());
      cur->second.pop_front();
      if (cur->second.empty()) {
	cur = q.erase(cur);
      } else {
	++cur;
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      --q_size;
    }
    unsigned size() const {
      return q_size;
    }
    bool empty() const {
      return (q_size == 0);
    }
    unsigned remove_by_filter(std::function<bool (T)> f, std::list<T> *out) {
      unsigned count = 0;
      for (typename Classes::iterator i = q.begin();
	   i != q.end(); ) {
	count += filter_list_pairs(&(i->second), f, out);
	if (i->second.empty()) {
	  if (cur == i) {
	    ++cur;
	  }
	  q.erase(i++);
	} else {
	  ++i;
	}
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      q_size -= count;
      return count;
    }
    unsigned remove_by_class(K k, std::list<T> *out) {
      typename Classes::iterator i = q.find(k);
      if (i == q.end()) {
	return 0;
      }
      unsigned count = i->second.size();
      q_size -= count;
      if (out) {
	for (typename ListPairs::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
	  out->push_front(j->second);
	}
      }
      if (i == cur) {
	++cur;
      }
      q.erase(i);
      if (cur == q.end()) {
	cur = q.begin();
      }
      return count;
    }

    void dump(Formatter *f) const {
      f->dump_int("num_keys", q.size());
      if (!empty()) {
	f->dump_int("first_item_cost", front().first);
      }
    }
  };

  unsigned high_size, wrr_size;
  unsigned max_cost;

  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  SubQueues queue;
  typename SubQueues::reverse_iterator dq;

  SubQueue *create_queue(unsigned priority) {
    typename SubQueues::iterator p = queue.find(priority);
    if (p != queue.end()) {
      return &p->second;
    }
    total_priority += priority;
    SubQueue *sq = &queue[priority];
    return sq;
  }

  void remove_queue(unsigned priority) {
    assert(queue.count(priority));
    dq = (typename SubQueues::reverse_iterator) queue.erase(queue.find(priority));
    if (dq == queue.rend()) {
      dq = queue.rbegin();
    }
    total_priority -= priority;
    assert(total_priority >= 0);
  }

public:
  WeightedPriorityQueue(unsigned max_per, unsigned min_c)
    : total_priority(0),
      high_size(0),
      wrr_size(0),
      max_cost(0),
      dq(queue.rbegin())
  {
    srand(time(0));
  }

  unsigned length() const override final {
    return high_size + wrr_size;
  }

  void remove_by_filter(
      std::function<bool (T)> f, std::list<T> *removed = 0) override final {
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end(); ++i) {
      wrr_size -= i->second.remove_by_filter(f, removed);
      unsigned priority = i->first;
      if (i->second.empty()) {
	remove_queue(priority);
      }
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      high_size -= i->second.remove_by_filter(f, removed);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void remove_by_class(K k, std::list<T> *out = 0) override final {
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end(); ++i) {
      wrr_size -= i->second.remove_by_class(k, out);
      unsigned priority = i->first;
      if (i->second.empty()) {
	remove_queue(priority);
      }
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      high_size -= i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void enqueue (K cl, T item, unsigned priority, unsigned cost = 0,
		bool front = CEPH_OP_QUEUE_BACK,
		unsigned opclass = CEPH_OP_CLASS_NORMAL) override final {
    switch (opclass){
      case CEPH_OP_CLASS_NORMAL :
	if (cost > max_cost) {
	  max_cost = cost;
	}
	create_queue(priority)->enqueue(cl, cost, item, front);
	++wrr_size;
	break;
      case CEPH_OP_CLASS_STRICT :
	high_queue[priority].enqueue(cl, 0, item, front);
	++high_size;
	break;
      default :
	assert(1);
    }
  }

  bool empty() const override final {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !queue.empty());
    return (high_size + wrr_size  == 0) ? true : false;
  }

  void inc_dq() {
    ++dq;
    if (dq == queue.rend()) {
      dq = queue.rbegin();
    }
  }

  T dequeue() override final {
    assert(!empty());

    if (!high_queue.empty()) {
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty()) {
	high_queue.erase(high_queue.rbegin()->first);
      }
      --high_size;
      return ret;
    }
    while (true) {
      // If there is only one priority, run it.
      if (dq->second.size() == wrr_size) {
	break;
      } else {
	// Pick a new priority out of the total priority.
	unsigned prio = rand() % total_priority;
	typename SubQueues::iterator i = queue.begin();
	unsigned tp = i->first;
	// Find the priority coresponding to the picked number.
	// Add low priorities to high priorities until the picked number
	// is less than the total and try to dequeue that priority.
	while (prio > tp) {
	  ++i;
	  tp += i->first;
	}
	dq = (typename SubQueues::reverse_iterator) ++i;
	// Flip a coin to see if this priority gets to run based on cost.
	// The next op's cost is multiplied by .9 and subtracted from the
	// max cost seen. Ops with lower costs will have a larger value
	// and allow them to be selected easier than ops with high costs.
	if (max_cost == 0 || rand() % max_cost <=
	    (max_cost - ((dq->second.front().first * 9) / 10))){
	  break;
	}
      }
      inc_dq();
    }
    T ret = dq->second.front().second;
    dq->second.pop_front();
    if (dq->second.empty()) {
      remove_queue(dq->first);
    }
    --wrr_size;
    return ret;
  }

  void dump(Formatter *f) const {
    f->dump_int("total_priority", total_priority);
    f->open_array_section("high_queues");
    for (typename SubQueues::const_iterator p = high_queue.begin();
	 p != high_queue.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("queues");
    for (typename SubQueues::const_iterator p = queue.begin();
	 p != queue.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
  }
};

#endif
