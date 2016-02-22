// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "common/PrioritizedQueue.h"
//#include "common/TestQueue.h"
#include "common/WeightedPriorityQueue.h"
#include "common/WeightedPriorityQueue2.h"
#include "include/assert.h"
#include <iostream>
#include <chrono>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/variant.hpp>

#include "RunningStat.h"

static bool printstrict = false;
typedef std::map<unsigned long long int, unsigned long long int> PrioStat;
typedef unsigned Klass;
typedef unsigned Prio;
typedef unsigned Kost;
typedef unsigned Strick;
typedef std::tuple<Prio, Klass, Kost, unsigned, Strick> Op;
typedef std::chrono::time_point<std::chrono::system_clock> SWatch;
namespace bi = boost::intrusive;

class PGRef : public boost::intrusive_ref_counter<PGRef> {
  unsigned PG;
  public:
    PGRef(unsigned p) :
      PG(p) {};
};

class PGQueuable {
  typedef boost::variant <
    unsigned,
    int,
    double
      > QVariant;
  QVariant qvariant;
  int cost;
  unsigned priority;
  time_t start_time;
  double owner;
  struct RunVis : public boost::static_visitor<> {
    unsigned *osd;
    PGRef &pg;
    double &handle;
    RunVis(unsigned *osd, PGRef &pg, double &handle)
      : osd(osd), pg(pg), handle(handle) {}
    void operator()(unsigned &op);
    void operator()(int &op);
    void operator()(double &op);
  };
public:
  // cppcheck-suppress noExplicitConstructor 
  //PGQueuable()
  //  : qvariant(0), cost(1), priority(2), start_time(time(0)), owner(2.0) {}
  PGQueuable(unsigned &op, unsigned &c, unsigned &s)
    : qvariant(op), cost((int) c), priority(op), start_time(time(0)), owner((double) s) {}
  PGQueuable(int &op, int &c, unsigned &s)
    : qvariant(op), cost(c), priority((unsigned) op), start_time(time(0)), owner((double) op) {}
  PGQueuable(double &op, int &c, unsigned &s)
    : qvariant(op), cost(c), priority((unsigned) op), start_time(time(0)), owner(op) {}
  boost::optional<unsigned> maybe_get_op() {
    unsigned *op = boost::get<unsigned>(&qvariant);
    return op ? *op : boost::optional<unsigned>();
  }
  void run(unsigned *osd, PGRef &pg, double &handle) {
    RunVis v(osd, pg, handle);
    boost::apply_visitor(v, qvariant);
  }
  unsigned get_priority() const { return priority; }
  int get_cost() const { return cost; }
  time_t get_start_time() const { return start_time; }
  double get_owner() const { return owner; }
};

typedef std::pair<boost::intrusive_ptr<PGRef>, PGQueuable> OpPair;

template <typename T>
class Queue {
  T q;
  RunningStat eqtime, misseddq, eqrtime, missedrdq;
  SWatch start, end;

  const static unsigned max_prios = 5; // (0-4) * 64
  const static unsigned klasses = 7;  // Make prime to help get good coverage

  struct Stats {
    PrioStat opdist, sizedist, totalopdist, totalsizedist;
    unsigned long long totalops, totalcost, totalweightops, totalweightcost;
    RunningStat dqtime;

    Stats() :
      totalops(0),
      totalcost(0),
      totalweightops(0),
      totalweightcost(0),
      dqtime()
    {}

    void resetStats() {
      for (PrioStat::iterator i = opdist.begin(); i != opdist.end(); ++i) {
	totalopdist[i->first] -= i->second;
	totalweightops -= (i->first + 1) * i->second;
      }
      for (PrioStat::iterator i = sizedist.begin(); i != sizedist.end(); ++i) {
	totalsizedist[i->first] -= i->second;
	totalweightcost -= (i->first + 1) * i->second;
      }
      opdist.clear();
      sizedist.clear();
      //totalopdist.clear();
      //totalsizedist.clear();
      totalops = 0;
      totalcost = 0;
      //totalweightops = 0;
      //totalweightcost = 0;
      dqtime.Clear();
    }
  };

  // stats for each test and long running stats
  Stats sqstat, nqstat, sqrstat, nqrstat;

  public:
    static Op gen_op(unsigned i, bool randomize = false,
       	unsigned skew = 0) {
      // Skew (0-100) will perform level random cost allocation
      // where 100 will cause low priority to have much larger ops.
      // Choose priority, class, cost and 'op' for this op.
      unsigned p, k, c, o, s;
      unsigned long long int range, start, ratio;
      if (randomize) {
        p = (rand() % max_prios) * 64;
        k = rand() % klasses;
	if (rand() % 20 == 7) {
	  skew = skew > 100 ? 100 : skew;
	  ratio = (max_prios * 64 * 100 - (p * skew))/(max_prios * 64);
	  range = (((1<<22) - 4096) * (100 - skew)) / 100;
	  start = (((1<<22) - 4096) * skew) / 100 + 4096;
          c = rand() % ((range * ratio)/100) + (start * ratio) / 100;
	} else {
	  // Make some of the costs 0, but make sure small costs
	  // still work ok.
          c = 0;
	}
        s = rand() % 10;
      } else {
        p = (i % max_prios) * 64;
        k = i % klasses;
        c = (i % 8 == 0 || i % 16 == 0) ? 0 : 1 << (i % 23);
	s = i % 7; // Use prime numbers to
      }
      o = rand() % (1<<16);
      return Op(p, k, c, o, s);
    }
    void enqueue_op(Op &op, bool front = false,
       	unsigned strict = false) {
      //{
      //  PGQueuable pg = PGQueuable(std::get<0>(op), std::get<2>(op), std::get<4>(op));
      //  unsigned t = pg.get_priority();
      //}
      OpPair oppair = std::make_pair(boost::intrusive_ptr<PGRef>(new PGRef(std::get<1>(op))),
	    PGQueuable(std::get<0>(op), std::get<2>(op), std::get<4>(op)));
      //PGQueuable pg = oppair.second;
      //unsigned test = pg.get_priority();
      start = std::chrono::system_clock::now();
      if (strict) {
	if (front) {
          q.enqueue_strict_front(std::get<1>(op), std::get<0>(op), oppair);
	} else {
          q.enqueue_strict(std::get<1>(op), std::get<0>(op), oppair);
	}
      } else {
	if (front) {
	  q.enqueue_front(std::get<1>(op), std::get<0>(op),
	            std::get<2>(op), oppair);
	} else {
	  q.enqueue(std::get<1>(op), std::get<0>(op),
	            std::get<2>(op), oppair);
	}
      }
      end = std::chrono::system_clock::now();
      double ticks = std::chrono::duration_cast
	<std::chrono::nanoseconds>(end - start).count();
      eqtime.Push(ticks);
      eqrtime.Push(ticks);
      switch (std::get<4>(op)) {
      case 6:
	// Strict queue
	if (printstrict) {
	  eq_add_stats(sqstat, oppair);
	  eq_add_stats(sqrstat, oppair);
	}
	break;
      default:
	//Normal queue
	eq_add_stats(nqstat, oppair);
	eq_add_stats(nqrstat, oppair);
	break;
      }
    }
    void eq_add_stats(Stats &s, OpPair &r) {
	++s.totalopdist[r.second.get_priority()];
	s.totalsizedist[r.second.get_priority()] += r.second.get_cost();
	s.totalweightops += (r.second.get_priority() + 1);
	s.totalweightcost += (r.second.get_priority() + 1) * r.second.get_cost();
    }
    OpPair dequeue_op() {
      typedef std::pair<boost::intrusive_ptr<PGRef>, PGQueuable> OpPair;
      //OpPair r;
      unsigned missed;
      start = std::chrono::system_clock::now();
      //r = q.dequeue(missed);
      OpPair r = q.dequeue();
      end = std::chrono::system_clock::now();
      // Keep track of strict and normal queues seperatly
      switch ((unsigned) r.second.get_owner()) {
      case 6:
	// Strict queue
	if (printstrict) {
	  dq_add_stats(sqstat, r, end - start);
	  dq_add_stats(sqrstat, r, end - start);
	}
	break;
      default:
	// Normal queue
	// misseddq only makes sense in the normal queue.
	misseddq.Push(missed);
	missedrdq.Push(missed);
	dq_add_stats(nqstat, r, end - start);
	dq_add_stats(nqrstat, r, end - start);
	break;
      }
      return r;
    }
    void dq_add_stats(Stats &s, OpPair &r,
       	std::chrono::duration<double> t) {
      s.dqtime.Push(std::chrono::duration_cast<std::chrono::nanoseconds>(t).count());
      ++s.opdist[r.second.get_priority()];
      ++s.totalops;
      s.sizedist[r.second.get_priority()] += r.second.get_cost();
      s.totalcost += r.second.get_cost();
    }

    Queue(unsigned max_per = 0, unsigned min_c =0) :
      q(max_per, min_c),
      eqtime(),
      misseddq(),
      sqstat(),
      nqstat(),
      eqrtime(),
      missedrdq(),
      sqrstat(),
      nqrstat()
	{}

    void gen_enqueue(unsigned i, bool randomize = false) {
      unsigned op_queue, fob;
      Op tmpop = gen_op(i, randomize);
      enqueue(i, tmpop, randomize);
    }

    void enqueue(unsigned i, Op tmpop, bool randomize = false,
	int fob = -1) {
      // Choose how to enqueue this op.
      if (randomize) {
        //op_queue = rand() % 10;
        fob = rand() % 10;
      } else {
	if (fob == -1) {
	  //op_queue = i % 7; // Use prime numbers to
          fob = i % 11;     // get better coverage
	}
      }
      switch (std::get<4>(tmpop)) {
      case 6 :
        // Strict Queue
        if (fob == 4) {
          // Queue to the front.
          enqueue_op(tmpop, true, true);
        } else {
          //Queue to the back.
          enqueue_op(tmpop, false, true);
        }
        break;
      default:
        // Normal queue
        if (fob == 4) {
          // Queue to the front.
          enqueue_op(tmpop, true);
        } else {
          //Queue to the back.
          enqueue_op(tmpop);
        }
        break;
      }
    }
    void dequeue() {
      if (!q.empty()) {
        OpPair op = dequeue_op();
      }
    }

    void test_queue(unsigned item_size,
       	unsigned eratio, bool randomize = false) {
      for (unsigned i = 1; i <= item_size; ++i) {
	if (rand() % 100 + 1 < eratio) {
	  gen_enqueue(i, randomize);
	} else {
	  if (!q.empty()) {
	    dequeue();
	  } else {
	    gen_enqueue(i, randomize);
	  }
	}
      }
    }

    void print_substat_summary(Stats s, string n) {
      std::cout << ">" << n << " " << std::setw(6) << q.length() <<
        "/" << std::setw(7) << s.totalops <<
        " (" << std::setw(14) << s.totalcost << ") " <<
        ": " << std::setw(7) << eqtime.Mean() <<
        "," << std::setw(7) << eqtime.StandardDeviation() <<
        "," << std::setw(7) << s.dqtime.Mean() <<
        "," << std::setw(7) << s.dqtime.StandardDeviation();
      if (n.compare("S") != 0) {
	std::cout << "," << std::setw(7) << misseddq.Mean() <<
	  "," << std::setw(7) << misseddq.StandardDeviation();
      }
      std::cout << std::endl;
    }

    string get_terse() {
      return get_terse_summary(nqrstat);
    }

    string get_terse_summary(Stats s) {
      return std::to_string(eqtime.Mean()) + "," + std::to_string(eqtime.StandardDeviation()) + "," +
	     std::to_string(s.dqtime.Mean()) + "," + std::to_string(s.dqtime.StandardDeviation());
    }

    void print_substat_io(Stats s) {
      unsigned totprio = 0;
      for (PrioStat::iterator i = s.totalopdist.begin(); i != s.totalopdist.end(); ++i) {
	totprio += 1 + i->first;
      }
      double shares = (double) s.totalops / totprio;
      for (PrioStat::reverse_iterator i = s.totalopdist.rbegin(); i != s.totalopdist.rend(); ++i) {
	unsigned long long dqops, prio;
	prio = i->first;
	PrioStat::iterator it = s.opdist.find(prio);
	if (it != s.opdist.end()) {
	  dqops = it->second;
	} else {
	  dqops = 0;
	}
	double availops = (prio + 1) * shares;
	unsigned long long int ops = s.totalopdist[prio];
	//std::cout << totprio << "," << shares << "," << availops <<
	//  "," << ops << std::endl;
	totprio -= (prio + 1);
	if (availops > ops) {
	  shares += (availops - ops) / totprio;
	  availops = ops;
	}
	//std::cout << availops << " / " << s.totalops << std::endl; 
        std::cout << ">>" << std::setw(3) << prio <<
          ":" << std::setw(15) << dqops <<
          "/" << std::setw(15) << s.totalopdist[prio] <<
          " " << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double) dqops / s.totalopdist[prio]) * 100 << " % " <<
          " (" << std::setw(6) << std::fixed << std::setprecision(2);
	if (s.totalops != 0) {
	  std::cout << ((double) dqops / s.totalops) * 100 << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	    (availops / s.totalops) * 100 << " %/";
	} else {
	  std::cout << "0.00" << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	    "0.00" << " %/";
	}
	  std::cout << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double) ((unsigned long long int)(prio + 1) * s.totalopdist[prio]) / s.totalweightops) * 100 << " %)" <<
          std::endl;
      }
    }

    void print_substat_cost(Stats s) {
      unsigned totprio = 0;
      for (PrioStat::iterator i = s.totalopdist.begin(); i != s.totalopdist.end(); ++i) {
	totprio += 1 + i->first;
      }
      double shares = (double) s.totalcost / totprio;
      for (PrioStat::reverse_iterator i = s.totalsizedist.rbegin(); i != s.totalsizedist.rend(); ++i) {
	unsigned long long dqcost, prio;
	prio = i->first;
	PrioStat::iterator it = s.sizedist.find(prio);
	if (it != s.sizedist.end()) {
	  dqcost = it->second;
	} else {
	  dqcost = 0;
	}
	double availcost = (prio + 1) * shares;
	unsigned long long int cost = s.totalsizedist[prio];
	totprio -= (prio + 1);
	if (availcost > cost) {
	  shares += (availcost - cost) / totprio;
	  availcost = cost;
	}
        std::cout << ">>" << std::setw(3) << prio <<
          ":" << std::setw(15) << dqcost <<
          "/" << std::setw(15) << s.totalsizedist[prio] <<
          " " << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double) dqcost / s.totalsizedist[prio]) * 100 << " % " <<
          " (" << std::setw(6) << std::fixed << std::setprecision(2);
	if (s.totalcost != 0) {
	  std::cout << ((double) dqcost / s.totalcost) * 100 << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	    (availcost / s.totalcost) * 100 << " %/";
	} else {
	  std::cout << "0.00" << " %/" <<
	    std::setw(6) << std::fixed << std::setprecision(2) <<
	  "0.00" << " %/";
	}
	std::cout << std::setw(6) << std::fixed << std::setprecision(2) <<
          ((double) ((unsigned long long int)(i->first + 1) * i->second) / s.totalweightcost) * 100 << " %)" <<
          std::endl;
      }
    }

    void print_stats() {
      std::cout << ">" << "Q " << std::setw(6) << "len" <<
        "/" << std::setw(7) << "DQ ops" <<
        " (" << std::setw(14) << "T. cost" << ") " <<
        ": " << std::setw(7) << "E. Mn" <<
        "," << std::setw(7) << "E. SD" <<
        "," << std::setw(7) << "D. Mn" <<
        "," << std::setw(7) << "D. SD" <<
        "," << std::setw(7) << "M. Mn" <<
        "," << std::setw(7) << "M. SD" << std::endl;
      if (printstrict) {
	print_substat_summary(sqstat, "S");
	print_substat_summary(sqrstat, "R");
      }
      print_substat_summary(nqstat, "N");
      print_substat_summary(nqrstat, "R");

      std::cout << ">>" << "  P:" <<
       	std::setw(15) << "DQ OPs/Cost" <<
        "/" << std::setw(15) << "T. OPs/Cost" <<
        " " << std::setw(6) << "DQ" << " % " <<
        " (" << std::setw(6) << "A Dist" << " %/" <<
	std::setw(6) << "C Dist" << " %/" <<
        std::setw(6) << "P Dist" << " %)" <<
	std::endl;
      if (printstrict) {
	print_substat_io(sqstat);
	print_substat_io(sqrstat);
	print_substat_cost(sqstat);
	print_substat_cost(sqrstat);
      }
      print_substat_io(nqstat);
      std::cout << ">>" << std::setfill('-') << std::setw(74) <<
	"-" << std::setfill(' ') << std::endl;
      print_substat_io(nqrstat);
      std::cout << ">>" << std::setfill('=') << std::setw(74) <<
	"=" << std::setfill(' ') << std::endl;
      print_substat_cost(nqstat);
      std::cout << ">>" << std::setfill('-') << std::setw(74) <<
	"-" << std::setfill(' ') << std::endl;
      print_substat_cost(nqrstat);
    }
    void reset_round_stats() {
      eqtime.Clear();
      misseddq.Clear();
      sqstat.resetStats();
      nqstat.resetStats();
    }
    bool empty() const {
      return q.empty();
    }
};

typedef Queue<PrioritizedQueue<OpPair, unsigned>> PQ;
typedef Queue<WeightedPriorityQueue2<OpPair, unsigned>> TQ;
typedef Queue<WeightedPriorityQueue<OpPair, unsigned>> WQ;

void work_queues(PQ &pq, WQ &wq, TQ &tq, int count, int eratio,
    string name, string* csv = 0, unsigned skew = 0) {
  wq.reset_round_stats();
  pq.reset_round_stats();
  tq.reset_round_stats();
  unsigned long long enqueue = 0, dequeue = 0;
  for (unsigned i = 0; i < count; ++i) {
    if (eratio <= 10 && pq.empty()) {
      std::cout << "Nothing left to do, breaking early." << std::endl;
      break;
    }
    if (rand() % 101 < eratio) {
      ++enqueue;
      unsigned op_queue, fob;
      Op tmpop = Queue<int>::gen_op(i, true, skew);
      // Choose how to enqueue this op.
      op_queue = rand() % 10;
      fob = rand() % 10;
      pq.enqueue(i, tmpop, false, fob);
      wq.enqueue(i, tmpop, false, fob);
      tq.enqueue(i, tmpop, false, fob);
    } else {
      ++dequeue;
      pq.dequeue();
      wq.dequeue();
      tq.dequeue();
    }
  }
  if (enqueue + dequeue > 0) {
    std::cout << std::endl;
    std::cout << enqueue << "/" << dequeue << " (" <<std::setw(6) <<
      std::fixed << std::setprecision(2) << (double) enqueue / (enqueue + dequeue) * 100 <<
      " % / " << std::setw(6) << std::fixed << std::setprecision(2) <<
      (double) dequeue / (enqueue + dequeue) * 100 << " %)" << std::endl;
    std::cout << "===Prio Queue stats (" << name << "):" << std::endl;
    pq.print_stats();
    std::cout << "===WP Queue stats (" << name << "):" << std::endl;
    wq.print_stats();
    std::cout << "===Test Queue stats (" << name << "):" << std::endl;
    tq.print_stats();
  }
  if (csv) {
    *csv += pq.get_terse() + "," + wq.get_terse() + "," + tq.get_terse();
  }
}

int main() {
  srand(time(0));
  string csv;
  PQ pq;
  WQ wq;
  TQ tq;
  std::cout << "Running.." << std::endl;
  work_queues(pq, wq, tq, 10000, 100, "Warm-up (100/0)");
  work_queues(pq, wq, tq, 10000, 70, "Stress (70/30)");
  work_queues(pq, wq, tq, 90000, 70, "Stress (70/30)");
  work_queues(pq, wq, tq, 10000, 50, "Balanced (50/50)");
  work_queues(pq, wq, tq, 990000, 50, "Balanced (50/50)");
  work_queues(pq, wq, tq, 10000, 30, "Cool-down (30/70)");
  work_queues(pq, wq, tq, 90000, 30, "Cool-down (30/70)");
  work_queues(pq, wq, tq, 1000000, 0, "Drain (0/100)");
  work_queues(pq, wq, tq, 10000, 50, "Balanced (50/50)", &csv);
  csv += "\n";
  PQ spq;
  WQ swq;
  TQ stq;
  work_queues(spq, swq, stq, 10000, 100, "Skew (90): Warm-up (100/0)", 0, 90);
  work_queues(spq, swq, stq, 10000, 70, "Skew (90): Stress (70/30)", 0, 90);
  work_queues(spq, swq, stq, 90000, 70, "Skew (90): Stress (70/30)", 0, 90);
  work_queues(spq, swq, stq, 10000, 50, "Skew (90): Balanced (50/50)", 0, 90);
  work_queues(spq, swq, stq, 990000, 50, "Skew (90): Balanced (50/50)", 0, 90);
  work_queues(spq, swq, stq, 10000, 30, "Skew (90): Cool-down (30/70)", 0, 90);
  work_queues(spq, swq, stq, 90000, 30, "Skew (90): Cool-down (30/70)", 0, 90);
  work_queues(spq, swq, stq, 1000000, 0, "Skew (90): Drain (0/100)", 0, 90);
  work_queues(spq, swq, stq, 10000, 50, "Skew (90): Balanced (50/50)", &csv, 90);
  std::cout << csv << std::endl;
}
