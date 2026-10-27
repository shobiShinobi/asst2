#include "tasksys.h"
#include <unordered_set>

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

}  // namespace


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 *
 * Workers sleep on work_cv_ when the ready queue is empty.
 * A launch sits in the waiting set (unmet > 0) until every dependency
 * has finished. The last task of a launch moves dependents whose unmet
 * count hits zero onto the ready queue and wakes sleepers.
 * sync() sleeps on sync_cv_ until unfinished_ drops to zero.
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads < 1 ? 1 : num_threads),
      shutdown_(false),
      unfinished_(0),
      ready_count_(0) {
    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        workers_.push_back(std::thread(
            &TaskSystemParallelThreadPoolSleeping::workerLoop, this));
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

void TaskSystemParallelThreadPoolSleeping::enqueueReady(TaskID id) {
    // Caller holds mutex_.
    Launch* launch = launches_[id].get();
    if (launch->total == 0) {
        markComplete(id);
        return;
    }
    if (launch->queued) {
        return;
    }
    bool wake = ready_.empty();
    launch->queued = true;
    ready_.push(id);
    ready_count_.store((int)ready_.size(), std::memory_order_release);
    if (wake) {
        work_cv_.notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::markComplete(TaskID id) {
    // Caller holds mutex_.
    Launch* launch = launches_[id].get();
    if (launch->done) {
        return;
    }
    launch->done = true;
    if (unfinished_ > 0) {
        unfinished_--;
    }
    for (int i = 0; i < (int)launch->dependents.size(); i++) {
        TaskID dep = launch->dependents[i];
        Launch* child = launches_[dep].get();
        child->unmet--;
        if (child->unmet == 0) {
            enqueueReady(dep);
        }
    }
    if (unfinished_ == 0) {
        sync_cv_.notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
        while (!shutdown_ && ready_.empty()) {
            // A dependent launch is often queued a few microseconds after
            // the early workers finish. Spin before sleeping so a chain
            // of bulk launches does not pay a wake-up every step.
            lock.unlock();
            for (int spins = 0; spins < 12000; spins++) {
                if (ready_count_.load(std::memory_order_acquire) > 0) {
                    break;
                }
                cpuPause();
            }
            lock.lock();
            if (!ready_.empty() || shutdown_) {
                break;
            }
            work_cv_.wait(lock);
        }
        if (shutdown_) {
            return;
        }

        TaskID id = ready_.front();
        Launch* launch = launches_[id].get();
        lock.unlock();

        int task = launch->next.fetch_add(1, std::memory_order_relaxed);
        if (task >= launch->total) {
            {
                std::lock_guard<std::mutex> guard(mutex_);
                if (!ready_.empty() && ready_.front() == id &&
                    launch->next.load(std::memory_order_relaxed) >= launch->total) {
                    ready_.pop();
                    launch->queued = false;
                    ready_count_.store((int)ready_.size(), std::memory_order_release);
                }
            }
            lock.lock();
            continue;
        }

        launch->runnable->runTask(task, launch->total);

        int prev = launch->remaining.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 1) {
            std::lock_guard<std::mutex> guard(mutex_);
            // Drop this launch from the ready queue if every index
            // has already been claimed, then wake dependents.
            if (!ready_.empty() && ready_.front() == id &&
                launch->next.load(std::memory_order_relaxed) >= launch->total) {
                ready_.pop();
                launch->queued = false;
                ready_count_.store((int)ready_.size(), std::memory_order_release);
            }
            markComplete(id);
        }

        lock.lock();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::lock_guard<std::mutex> lock(mutex_);

    TaskID id = (TaskID)launches_.size();
    Launch* launch = new Launch();
    launch->runnable = runnable;
    launch->total = num_total_tasks;
    launch->remaining.store(num_total_tasks, std::memory_order_relaxed);

    // Duplicate ids in deps count once. An already-finished dependency
    // is not a reason to wait.
    std::unordered_set<TaskID> seen;
    seen.reserve(deps.size());
    for (int i = 0; i < (int)deps.size(); i++) {
        TaskID dep = deps[i];
        if (!seen.insert(dep).second) {
            continue;
        }
        if (dep < 0 || dep >= id) {
            continue;
        }
        if (!launches_[dep]->done) {
            launch->unmet++;
            launches_[dep]->dependents.push_back(id);
        }
    }

    launches_.push_back(std::unique_ptr<Launch>(launch));

    if (launch->unmet == 0) {
        if (num_total_tasks == 0) {
            launch->done = true;
        } else {
            unfinished_++;
            enqueueReady(id);
        }
    } else {
        unfinished_++;
    }
    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (unfinished_ > 0) {
        sync_cv_.wait(lock);
    }
}
