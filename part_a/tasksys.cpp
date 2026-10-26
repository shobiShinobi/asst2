#include "tasksys.h"
#include <algorithm>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

namespace {

inline void cpuPause() {
#if defined(__x86_64__) || defined(__i386__)
    asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

// Give the core up quickly. Idle workers otherwise sit in a pause loop
// and steal time from the one thread that has a task (the reduce steps,
// and spin_between_run_calls).
inline void idleSpin() {
    std::this_thread::yield();
}

// Assignment policy for the sleeping pool.
// Dynamic is what we submit. Flip this to measure the static variants
// used in the writeup (imbalance ratio R, and the skewed-cost test).
enum AssignMode {
    ASSIGN_DYNAMIC = 0,
    ASSIGN_STATIC_CONTIGUOUS = 1,
    ASSIGN_STATIC_INTERLEAVED = 2
};

const AssignMode kAssignMode = ASSIGN_DYNAMIC;

void staticRange(int tid, int nthreads, int total, int* start, int* end) {
    int base = total / nthreads;
    int rem = total % nthreads;
    if (tid < rem) {
        *start = tid * (base + 1);
        *end = *start + base + 1;
    } else {
        *start = rem * (base + 1) + (tid - rem) * base;
        *end = *start + base;
    }
}

}  // namespace

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads < 1 ? 1 : num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    // Caller is one of the workers, so we never go above num_threads_.
    int workers = num_threads_;
    if (workers > num_total_tasks) {
        workers = num_total_tasks;
    }

    // Dynamic assignment: whoever finishes a task grabs the next id.
    std::atomic<int> next(0);
    auto work = [&]() {
        while (true) {
            int id = next.fetch_add(1, std::memory_order_relaxed);
            if (id >= num_total_tasks) {
                break;
            }
            runnable->runTask(id, num_total_tasks);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(workers - 1);
    for (int i = 0; i < workers - 1; i++) {
        threads.push_back(std::thread(work));
    }
    work();
    for (int i = 0; i < (int)threads.size(); i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads < 1 ? 1 : num_threads),
      pool_size_(0),
      shutdown_(false),
      runnable_(NULL),
      total_(0),
      next_(0),
      finished_(0),
      generation_(0),
      workers_idle_(0) {
    // Leave one slot for the calling thread so a launch uses at most
    // num_threads_ threads.
    pool_size_ = num_threads_ > 1 ? num_threads_ - 1 : 0;
    workers_.reserve(pool_size_);
    for (int i = 0; i < pool_size_; i++) {
        workers_.push_back(std::thread(
            &TaskSystemParallelThreadPoolSpinning::workerLoop, this, i));
    }
    while (pool_size_ > 0 &&
           workers_idle_.load(std::memory_order_acquire) != pool_size_) {
        idleSpin();
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdown_.store(true, std::memory_order_release);
    for (int i = 0; i < (int)workers_.size(); i++) {
        workers_[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop(int tid) {
    (void)tid;
    int my_gen = 0;
    while (true) {
        // Counted idle before looking at the generation, so run() will
        // not publish the next launch (and will not reset next_) until
        // this worker has left the claim loop.
        workers_idle_.fetch_add(1, std::memory_order_acq_rel);
        while (!shutdown_.load(std::memory_order_acquire) &&
               generation_.load(std::memory_order_acquire) == my_gen) {
            idleSpin();
        }
        if (shutdown_.load(std::memory_order_acquire)) {
            return;
        }

        IRunnable* runnable = NULL;
        int total = 0;
        {
            // Snapshot under the same mutex run() holds while it publishes.
            // Otherwise a worker can observe generation G and then load the
            // runnable/limit that already belong to G+1.
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdown_.load(std::memory_order_acquire)) {
                return;
            }
            my_gen = generation_.load(std::memory_order_acquire);
            runnable = runnable_;
            total = total_;
            workers_idle_.fetch_sub(1, std::memory_order_acq_rel);
        }

        while (true) {
            int id = next_.fetch_add(1, std::memory_order_relaxed);
            if (id >= total) {
                break;
            }
            runnable->runTask(id, total);
            // acq_rel chains task writes across workers so the caller
            // that observes finished_ == total sees every runTask.
            finished_.fetch_add(1, std::memory_order_acq_rel);
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }
    if (pool_size_ == 0) {
        for (int i = 0; i < num_total_tasks; i++) {
            runnable->runTask(i, num_total_tasks);
        }
        return;
    }

    while (true) {
        while (workers_idle_.load(std::memory_order_acquire) != pool_size_) {
            idleSpin();
        }
        std::lock_guard<std::mutex> lock(mutex_);
        // A worker may have taken the snapshot lock and dropped out of
        // the idle set between the check above and this lock.
        if (workers_idle_.load(std::memory_order_acquire) != pool_size_) {
            continue;
        }
        runnable_ = runnable;
        total_ = num_total_tasks;
        finished_.store(0, std::memory_order_relaxed);
        next_.store(0, std::memory_order_relaxed);
        generation_.fetch_add(1, std::memory_order_release);
        break;
    }

    while (true) {
        int id = next_.fetch_add(1, std::memory_order_relaxed);
        if (id >= num_total_tasks) {
            break;
        }
        runnable->runTask(id, num_total_tasks);
        finished_.fetch_add(1, std::memory_order_acq_rel);
    }
    while (finished_.load(std::memory_order_acquire) < num_total_tasks) {
        idleSpin();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads < 1 ? 1 : num_threads),
      pool_size_(0),
      shutdown_(false),
      generation_(0),
      workers_idle_(0),
      sleepers_(0),
      runnable_(NULL),
      total_(0),
      next_(0),
      finished_(0) {
    pool_size_ = num_threads_ > 1 ? num_threads_ - 1 : 0;
    workers_.reserve(pool_size_);
    for (int i = 0; i < pool_size_; i++) {
        workers_.push_back(std::thread(
            &TaskSystemParallelThreadPoolSleeping::workerLoop, this, i));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
        work_cv_.notify_all();
    }
    for (int i = 0; i < (int)workers_.size(); i++) {
        workers_[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::noteFinished(int count) {
    if (count <= 0) {
        return;
    }
    int prev = finished_.fetch_add(count, std::memory_order_acq_rel);
    if (prev + count == total_) {
        std::lock_guard<std::mutex> lock(mutex_);
        done_cv_.notify_one();
    }
}

void TaskSystemParallelThreadPoolSleeping::executeRange(int tid, IRunnable* runnable, int total) {
    if (kAssignMode == ASSIGN_DYNAMIC) {
        while (true) {
            int id = next_.fetch_add(1, std::memory_order_relaxed);
            if (id >= total) {
                break;
            }
            runnable->runTask(id, total);
            noteFinished(1);
        }
        return;
    }

    if (kAssignMode == ASSIGN_STATIC_INTERLEAVED) {
        int count = 0;
        for (int id = tid; id < total; id += num_threads_) {
            runnable->runTask(id, total);
            count++;
        }
        noteFinished(count);
        return;
    }

    int start = 0;
    int end = 0;
    staticRange(tid, num_threads_, total, &start, &end);
    for (int id = start; id < end; id++) {
        runnable->runTask(id, total);
    }
    noteFinished(end - start);
}

// Pauses before a worker gives up and sleeps. Back-to-back bulk launches
// hit the new generation during this window. A longer wait would keep
// idle workers on the core for the whole fib in spin_between_run_calls.
const int kSpinsBeforeSleep = 4000;

bool TaskSystemParallelThreadPoolSleeping::waitForLaunch(int my_gen) {
    for (int spins = 0; spins < kSpinsBeforeSleep; spins++) {
        if (shutdown_.load(std::memory_order_acquire)) {
            return false;
        }
        if (generation_.load(std::memory_order_acquire) != my_gen) {
            return true;
        }
        cpuPause();
    }

    std::unique_lock<std::mutex> lock(mutex_);
    sleepers_++;
    while (!shutdown_.load(std::memory_order_acquire) &&
           generation_.load(std::memory_order_acquire) == my_gen) {
        work_cv_.wait(lock);
    }
    sleepers_--;
    if (shutdown_.load(std::memory_order_acquire)) {
        return false;
    }
    return true;
}

void TaskSystemParallelThreadPoolSleeping::workerLoop(int tid) {
    int my_gen = 0;
    while (true) {
        workers_idle_.fetch_add(1, std::memory_order_acq_rel);
        if (!waitForLaunch(my_gen)) {
            return;
        }

        IRunnable* runnable = NULL;
        int total = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdown_.load(std::memory_order_acquire)) {
                return;
            }
            my_gen = generation_.load(std::memory_order_acquire);
            runnable = runnable_;
            total = total_;
            workers_idle_.fetch_sub(1, std::memory_order_acq_rel);
        }
        executeRange(tid, runnable, total);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }
    if (pool_size_ == 0) {
        for (int i = 0; i < num_total_tasks; i++) {
            runnable->runTask(i, num_total_tasks);
        }
        return;
    }

    while (true) {
        while (workers_idle_.load(std::memory_order_acquire) != pool_size_) {
            cpuPause();
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (workers_idle_.load(std::memory_order_acquire) != pool_size_) {
            continue;
        }
        runnable_ = runnable;
        total_ = num_total_tasks;
        finished_.store(0, std::memory_order_relaxed);
        next_.store(0, std::memory_order_relaxed);
        generation_.fetch_add(1, std::memory_order_release);
        if (sleepers_ > 0) {
            work_cv_.notify_all();
        }
        break;
    }

    // tid of the caller is the last slot.
    executeRange(pool_size_, runnable, num_total_tasks);

    std::unique_lock<std::mutex> lock(mutex_);
    while (finished_.load(std::memory_order_acquire) < num_total_tasks) {
        done_cv_.wait(lock);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // You do not need to implement this method.
    return;
}
