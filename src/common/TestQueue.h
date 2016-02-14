// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef T_QUEUE_H
#define T_QUEUE_H

#include "OpQueue.h"

#include <functional>
#include <list>
#include <iostream>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/rbtree.hpp>
#include <boost/intrusive/avl_set.hpp>

namespace bi = boost::intrusive;

using namespace std;

template <typename T>
class MapKey
{
  public:
  bool operator()(const int i, const T &k) const
  {
    return i < k.key;
  }
  bool operator()(const T &k, const int i) const
  {
    return k.key < i;
  }
};

template <typename T>
class DelItem
{
  public:
  void operator()(T* delete_this)
    { delete delete_this; }
};

template <typename T, typename K>
class TestQueue :  public OpQueue <T, K>
{
  private:
    class ListPair : public bi::list_base_hook<>
    {
      public:
        unsigned cost;
        T item;
        ListPair(unsigned& c, T& i) :
          cost(c),
          item(i)
          {}
    };
    //class Klass : public bi::avl_set_base_hook<>
    class Klass : public bi::set_base_hook<>
    {
      typedef bi::list<ListPair> ListPairs;
      typedef typename ListPairs::iterator Lit;
      public:
        K key;		// klass
        ListPairs lp;
        Klass(K& k) :
          key(k)
          {}
	//friend bool operator< (const Klass &a, const Klass &b)
	//  { return a.key < b.key; }
	//friend bool operator> (const Klass &a, const Klass &b)
	//  { return a.key > b.key; }
	//friend bool operator== (const Klass &a, const Klass &b)
	//  { return a.key == b.key; }
	bool insert(unsigned cost, T& item, bool front) {
	  if (front) {
	    lp.push_front(*new ListPair(cost, item));
	  } else {
	    lp.push_back(*new ListPair(cost, item));
	  }
	}
	//Get the cost of the next item to dequeue
	unsigned get_cost() const {
	  typename ListPairs::const_iterator i = lp.begin();
	  //std::cout << "Get lp->cost: " << std::hex << &i << std::dec << std::endl;
	  //return lp.begin()->cost;
	  return i->cost;
	}
	T& pop() {
	  Lit i = lp.begin();
	  T& ret = i->item;
	  lp.erase_and_dispose(i, DelItem<ListPair>());
	  return ret;
	}
	bool empty() const {
	  return lp.empty();
	}
	unsigned filter_list_pairs(std::function<bool (T)>& f,
	  std::list<T>* out) {
	  unsigned count = 0;
	  // intrusive containers can't erase with a reverse_iterator
	  // so we have to walk backwards on our own. Since there is
	  // no iterator before begin, we have to test at the end.
	  for (Lit i = --lp.end();; --i) {
	    //std::cout << "testing: " << i->cost << ", " << i->item << std::endl;
	    if (f(i->item)) {
	      //std::cout << "Deleting: " << std::endl;
	      if (out) {
		out->push_front(i->item);
	      }
	      i = lp.erase_and_dispose(i, DelItem<ListPair>());
	      ++count;
	    }
	    if (i == lp.begin()) {
	      break;
	    }
	  }
	  return count;
	}
	unsigned filter_class(std::list<T>* out) {
	  unsigned count = 0;
	  for (Lit i = --lp.end();; --i) {
	    if (out) {
	      out->push_front(i->item);
	    }
	    i = lp.erase_and_dispose(i, DelItem<ListPair>());
	    ++count;
	    if (i == lp.begin()) {
	      break;
	    }
	  }
	  return count;
	}
	void print() const {
    	  typename ListPairs::const_iterator it(lp.begin()), ite(lp.end());
    	  for (; it != ite; ++it) {
    	    //std::cout << "      L: " << it->cost << ", " << it->item << std::endl;
    	  }
    	}
    };
    //class SubQueue : public bi::avl_set_base_hook<>
    class SubQueue : public bi::set_base_hook<>
    {
      //typedef bi::avl_set<Klass> Klasses;
      typedef bi::rbtree<Klass> Klasses;
      typedef typename Klasses::iterator Kit;
      void check_end() {
	if (next == klasses.end()) {
	  next = klasses.begin();
	}
      }
      public:
	unsigned key;	// priority
	Klasses klasses;
	Kit next;
	SubQueue(unsigned& p) :
	  key(p),
	  next(klasses.begin())
	  {}
      bool empty() const {
	return klasses.empty();
      }
      bool insert(K& cl, unsigned cost, T& item, bool front = false) {
	typename Klasses::insert_commit_data insert_data;
      	std::pair<Kit, bool> ret =
	  //klasses.insert_check(cl, MapKey<Klass>(), insert_data);
	  klasses.insert_unique_check(cl, MapKey<Klass>(), insert_data);
      	if (ret.second) {
      	  //ret.first = klasses.insert_commit(*new Klass(cl), insert_data);
      	  ret.first = klasses.insert_unique_commit(*new Klass(cl), insert_data);
	  check_end();
      	}
      	ret.first->insert(cost, item, front);
      }
      // Get the cost of the next item to be dequeued
      unsigned get_cost() const {
	return next->get_cost();
      }
      T& pop() {
	//std::cout << "Next pointer: " << std::hex << &next << std::dec << std::endl;
	//if (next == klasses.end()) {
	  //std::cout << "Next is at the end." << std::endl;
	//}
	//if (next == klasses.end()) {
	//  next = klasses.begin();
	//}
	T& ret = next->pop();
	if (next->empty()) {
	  next = klasses.erase_and_dispose(next, DelItem<Klass>());
	}
	//if (next == klasses.end()) {
	//  next = klasses.begin();
	//}
	check_end();
	//if (next != klasses.end()) {
	//  std::cout << "pop" << std::endl;
	//  std::cout << "klasses.size(): " << klasses.size() << ", next pointer " << std::hex << &next << std::dec << std::endl;
	//  std::cout << "test next: " << std::endl;
	//  std::cout << "next->get_cost(): " << next->get_cost() << std::endl;
	//  std::cout << "next test complete." << std::endl;
	//}
	return ret;
      }
      unsigned filter_list_pairs(std::function<bool (T)>& f, std::list<T>* out) {
	unsigned count = 0;
	for (Kit i = klasses.begin(); i != klasses.end();) {
	  //cout << "going to test klass: " << i->key << std::endl;
	  count += i->filter_list_pairs(f, out);
	  if (i->empty()) {
	    i = klasses.erase_and_dispose(i, DelItem<Klass>());
	  } else {
	    ++i;
	  }
	}
	check_end();
	//if (next == klasses.end()) {
	//  next = klasses.begin();
	//}
	//std::cout << "filter_list_pairs" << std::endl;
	//std::cout << "klasses.size(): " << klasses.size() << ", next pointer " << std::hex << &next << std::dec << std::endl;
	//std::cout << "test next: " << std::endl;
	//std::cout << "next->get_cost(): " << next->get_cost() << std::endl;
	//std::cout << "next test complete." << std::endl;
	return count;
      }
      unsigned filter_class(K& cl, std::list<T>* out) {
	unsigned count = 0;
	Kit i = klasses.find(cl, MapKey<Klass>());
	//std::cout << "filter_class" << std::endl;
	//std::cout << "next: " << std::hex << &next << " <-> " << &i << std::dec << std::endl;
	if (i != klasses.end()) {
	  count = i->filter_class(out);
	}
	Kit tmp = klasses.erase_and_dispose(i, DelItem<Klass>());
	if (next == i) {
	  next = tmp;
	}
	check_end();
	//if (next == klasses.end()) {
	//  next = klasses.begin();
	//}
	//std::cout << "klasses.size(): " << klasses.size() << ", next pointer " << std::hex << &next << std::dec << std::endl;
	//std::cout << "test next: " << std::endl;
	//std::cout << "next->get_cost(): " << next->get_cost() << std::endl;
	//std::cout << "next test complete." << std::endl;
	//std::cout << "Removed " << count << " items." << std::endl;
	return count;
      }
      void print() const {
        typename Klasses::const_iterator it(klasses.begin()), ite(klasses.end());
        for (; it != ite; ++it) {
          std::cout << "   K: " << it->key << std::endl;
          it->print();
        }
      }
    };
    class Queue {
      //typedef bi::avl_set<SubQueue> SubQueues;
      typedef bi::rbtree<SubQueue> SubQueues;
      typedef typename SubQueues::iterator Sit;
      SubQueues queues;
      unsigned total_prio;
      unsigned max_cost;
      public:
	unsigned size;
	Queue() :
	  total_prio(0),
	  max_cost(0),
	  size(0)
	  {}
	bool empty() const {
	  return !size;
	}
	void insert(unsigned p, K& cl, unsigned cost, T& item, bool front = false) {
	  typename SubQueues::insert_commit_data insert_data;
      	  std::pair<typename SubQueues::iterator, bool> ret =
      	    //queues.insert_check(p, MapKey<SubQueue>(), insert_data);
      	    queues.insert_unique_check(p, MapKey<SubQueue>(), insert_data);
      	  if (ret.second) {
      	    //ret.first = queues.insert_commit(*new SubQueue(p), insert_data);
      	    ret.first = queues.insert_unique_commit(*new SubQueue(p), insert_data);
	    total_prio += p;
      	  }
      	  ret.first->insert(cl, cost, item, front);
	  if (cost > max_cost) {
	    max_cost = cost;
	  }
	  ++size;
	}
	T& pop(bool strict = false) {
	  --size;
	  Sit i = --queues.end();
	  if (strict) {
	    //std::cout << "Strict: got iterator. " << std::hex << &i << std::dec << std::endl;
	    T& ret = i->pop();
	    //std::cout << "Got item, going to try to clean up." << std::endl;
	    if (i->empty()) {
	      //std::cout << "Empty priority, going to clean up." << std::endl;
	      queues.erase_and_dispose(i, DelItem<SubQueue>());
	      //std::cout << "Clean up successful." << std::endl;
	    }
	    return ret;
	  }
	  //std::cout << "Queue size: " << queues.size() << ", total_prio: " << total_prio << std::endl;
	  if (queues.size() > 1) {
	    while (true) {
	      // Pick a new priority out of the total priority.
	      unsigned prio = rand() % total_prio + 1;
	      unsigned tp = total_prio - i->key;
	      // Find the priority coresponding to the picked number.
	      // Subtract high priorities to low priorities until the picked number
	      // is more than the total and try to dequeue that priority.
	      // Reverse the direction from previous because there is a higher
	      // chance of dequeuing a high priority op so spend less time spinning.
	      //std::cout << "prio: " << prio << ", tp: " << tp << "/" << total_prio << std::endl;
	      while (prio <= tp) {
		//if (tp > 1000) {
		//  print();
		//}
		//assert(tp < 1000);
		//std::cout << "decrement iterator" << std::endl;
		--i;
		//std::cout << "update tp, subtracting " << i->key << std::endl;
		//if (i->key < prio) {
		//  std::cout << "key < prio, stopping the vicious cycle." << std::endl;
		//  break;
		//}
		tp -= i->key;
		//std::cout << "prio: " << prio << ", tp: " << tp << "/" << total_prio << std::endl;
	      }
	      // Flip a coin to see if this priority gets to run based on cost.
	      // The next op's cost is multiplied by .9 and subtracted from the
	      // max cost seen. Ops with lower costs will have a larger value
	      // and allow them to be selected easier than ops with high costs.
	      //std::cout << "Try to dequeue prio: " << i->key << std::endl;
	      //std::cout << "Next klass in prio to dequeue: " << i->next->key << std::endl;
	      //std::cout << "Check the cost" << std::endl;
	      if (max_cost == 0 || rand() % max_cost <=
		  (max_cost - ((i->get_cost() * 9) / 10))) {
		break;
	      }
	      //std::cout << "Could not dequeue based on cost" << std::endl;
	      i = --queues.end();
	    }
	  }
	  T& ret = i->pop();
	  if (i->empty()) {
	    total_prio -= i->key;
	    queues.erase_and_dispose(i, DelItem<SubQueue>());
	  }
	  return ret;
	}
	void print() const {
	  typename SubQueues::const_iterator it(queues.begin()), ite(queues.end());
      	  for (; it != ite; ++it) {
      	    std::cout << "P: " << it->key << std::endl;
      	    it->print();
      	  }
	}
	void filter_list_pairs(std::function<bool (T)>& f, std::list<T>* out) {
	  for (Sit i = queues.begin(); i != queues.end();) {
      	    size -= i->filter_list_pairs(f, out);
	    if (i->empty()) {
	      total_prio -= i->key;
	      i = queues.erase_and_dispose(i, DelItem<SubQueue>());
	    } else {
	      ++i;
	    }
      	  }
	}
	void filter_class(K& cl, std::list<T>* out) {
	  for (Sit i = queues.begin(); i != queues.end();) {
	    size -= i->filter_class(cl, out);
	    if (i->empty()) {
	      total_prio -= i->key;
	      i = queues.erase_and_dispose(i, DelItem<SubQueue>());
	    } else {
	      ++i;
	    }
	  }
	}
    };

    Queue strict;
    Queue normal;
  public:
    TestQueue(unsigned max_per, unsigned min_c) :
      strict(),
      normal()
      {
	std::srand(time(0));
      }
    unsigned length() const override final {
      return strict.size + normal.size;
    }
    void remove_by_filter(std::function<bool (T)> f, std::list<T>* removed = 0) override final {
      normal.filter_list_pairs(f, removed);
    }
    void remove_by_class(K cl, std::list<T>* removed = 0) override final {
      normal.filter_class(cl, removed);
    }
    bool empty() const override final {
      return !(strict.size + normal.size);
    }
    void enqueue_strict(K cl, unsigned p, T item) override final {
      strict.insert(p, cl, 0, item);
    }
    void enqueue_strict_front(K cl, unsigned p, T item) override final {
      strict.insert(p, cl, 0, item, true);
    }
    void enqueue(K cl, unsigned p, unsigned cost, T item) override final {
      normal.insert(p, cl, cost, item);
    }
    void enqueue_front(K cl, unsigned p, unsigned cost, T item) override final {
      normal.insert(p, cl, cost, item, true);
    }
    T dequeue() {
      if (!strict.empty()) {
	//std::cout << "Dequeueing from strict." << std::endl;
	return strict.pop(true);
      }
      //std::cout << "Dequeueing from normal." << std::endl;
      return normal.pop();
    }
    void dump(ceph::Formatter *f) const {
    }
};

#endif
