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

// A slice bigger than this is split so idle workers can steal half.
// Smaller slices are run whole; splitting them costs more than the work.
const int kSplitAt = 8;

}  // namespace

bool TaskSystemParallelThreadPoolSleeping::WorkDeque::push(Chunk c) {
    int b = bottom.load(std::memory_order_relaxed);
    int t = top.load(std::memory_order_acquire);
    if (b - t >= kCap - 1) {
        return false;
    }
    buf[b & (kCap - 1)] = c;
    bottom.store(b + 1, std::memory_order_release);
    return true;
}

bool TaskSystemParallelThreadPoolSleeping::WorkDeque::pop(Chunk* out) {
    int b = bottom.load(std::memory_order_relaxed) - 1;
    bottom.store(b, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    int t = top.load(std::memory_order_relaxed);
    if (t <= b) {
        *out = buf[b & (kCap - 1)];
        if (t == b) {
            // compare_exchange overwrites t on failure. Keep the old
            // top so bottom lands just past this slot, not one further.
            int old_t = t;
            if (!top.compare_exchange_strong(t, old_t + 1,
                                             std::memory_order_seq_cst,
                                             std::memory_order_relaxed)) {
                bottom.store(old_t + 1, std::memory_order_relaxed);
                return false;
            }
            bottom.store(old_t + 1, std::memory_order_relaxed);
        }
        return true;
    }
    bottom.store(t, std::memory_order_relaxed);
    return false;
}

bool TaskSystemParallelThreadPoolSleeping::WorkDeque::steal(Chunk* out) {
    int t = top.load(std::memory_order_acquire);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    int b = bottom.load(std::memory_order_acquire);
    if (t >= b) {
        return false;
    }
    *out = buf[t & (kCap - 1)];
    if (!top.compare_exchange_strong(t, t + 1,
                                     std::memory_order_seq_cst,
                                     std::memory_order_relaxed)) {
        return false;
    }
    return true;
}


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
 * Each worker has its own deque of chunks (a launch id plus a task
 * range). The owner takes tasks from the back. A thief takes tasks
 * from the front, so whoever is busy keeps going and an idle worker
 * can peel work off without waiting on one shared queue.
 * A launch with unmet dependencies stays parked. The last task of a
 * launch wakes dependents whose unmet count hits zero.
 * Workers sleep on work_cv_ when every deque is empty.
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
      ready_count_(0),
      injector_(NULL) {
    deques_.reserve(num_threads_);
    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        deques_.push_back(std::unique_ptr<WorkDeque>(new WorkDeque()));
    }
    for (int i = 0; i < num_threads_; i++) {
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

void TaskSystemParallelThreadPoolSleeping::wakeIfIdle(int prev_count) {
    if (prev_count == 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        work_cv_.notify_all();
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
    launch->queued = true;

    // One chunk for the whole launch. The worker that pops it splits
    // the range onto its own deque; the others steal from there.
    nodes_.emplace_back();
    INode* node = &nodes_.back();
    node->chunk.id = id;
    node->chunk.launch = launch;
    node->chunk.lo = 0;
    node->chunk.hi = launch->total;
    node->next.store(NULL, std::memory_order_relaxed);

    INode* head = injector_.load(std::memory_order_relaxed);
    do {
        node->next.store(head, std::memory_order_relaxed);
    } while (!injector_.compare_exchange_weak(head, node,
                                              std::memory_order_release,
                                              std::memory_order_relaxed));

    int prev = ready_count_.fetch_add(1, std::memory_order_release);
    if (prev == 0) {
        work_cv_.notify_all();
    }
}

bool TaskSystemParallelThreadPoolSleeping::popOwn(int tid, Chunk* out) {
    if (!deques_[tid]->pop(out)) {
        return false;
    }
    ready_count_.fetch_sub(1, std::memory_order_release);
    return true;
}

bool TaskSystemParallelThreadPoolSleeping::steal(int tid, Chunk* out) {
    for (int k = 1; k < num_threads_; k++) {
        int victim = (tid + k) % num_threads_;
        if (deques_[victim]->steal(out)) {
            ready_count_.fetch_sub(1, std::memory_order_release);
            return true;
        }
    }
    return false;
}

bool TaskSystemParallelThreadPoolSleeping::popInjector(Chunk* out) {
    INode* head = injector_.load(std::memory_order_acquire);
    while (head != NULL) {
        INode* next = head->next.load(std::memory_order_relaxed);
        if (injector_.compare_exchange_weak(head, next,
                                            std::memory_order_acquire,
                                            std::memory_order_acquire)) {
            *out = head->chunk;
            ready_count_.fetch_sub(1, std::memory_order_release);
            return true;
        }
    }
    return false;
}

void TaskSystemParallelThreadPoolSleeping::publishSplit(int tid, Chunk chunk) {
    int n = chunk.hi - chunk.lo;
    if (n <= kSplitAt) {
        runChunk(chunk);
        return;
    }

    int pieces = n < num_threads_ ? n : num_threads_;
    int base = n / pieces;
    int rem = n % pieces;
    int cursor = chunk.lo;
    int published = 0;
    for (int i = 0; i < pieces - 1; i++) {
        int len = base + (i < rem ? 1 : 0);
        Chunk part;
        part.id = chunk.id;
        part.launch = chunk.launch;
        part.lo = cursor;
        part.hi = cursor + len;
        cursor += len;
        if (deques_[tid]->push(part)) {
            published++;
        } else {
            runChunk(part);
        }
    }
    if (published > 0) {
        int prev = ready_count_.fetch_add(published, std::memory_order_release);
        wakeIfIdle(prev);
    }

    Chunk mine;
    mine.id = chunk.id;
    mine.launch = chunk.launch;
    mine.lo = cursor;
    mine.hi = chunk.hi;
    // The piece we kept can still be bigger than the others when
    // num_threads does not divide the range. Split again so it
    // does not sit on one worker.
    if (mine.hi - mine.lo > kSplitAt) {
        publishSplit(tid, mine);
    } else {
        runChunk(mine);
    }
}

void TaskSystemParallelThreadPoolSleeping::runChunk(Chunk chunk) {
    Launch* launch = chunk.launch;
    int count = chunk.hi - chunk.lo;
    if (count <= 0) {
        return;
    }
    for (int t = chunk.lo; t < chunk.hi; t++) {
        launch->runnable->runTask(t, launch->total);
    }
    int prev = launch->remaining.fetch_sub(count, std::memory_order_acq_rel);
    if (prev == count) {
        std::lock_guard<std::mutex> guard(mutex_);
        markComplete(chunk.id);
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

void TaskSystemParallelThreadPoolSleeping::workerLoop(int tid) {
    while (true) {
        Chunk chunk;
        if (!popOwn(tid, &chunk) && !steal(tid, &chunk) && !popInjector(&chunk)) {
            // A dependent launch usually shows up a few microseconds
            // after the early workers finish. Spin before sleeping so
            // a chain of bulk launches does not pay a wake-up every step.
            for (int spins = 0; spins < 12000; spins++) {
                if (ready_count_.load(std::memory_order_acquire) > 0) {
                    break;
                }
                cpuPause();
            }
            std::unique_lock<std::mutex> lock(mutex_);
            if (shutdown_) {
                return;
            }
            if (ready_count_.load(std::memory_order_acquire) == 0) {
                work_cv_.wait(lock);
            }
            if (shutdown_) {
                return;
            }
            continue;
        }
        publishSplit(tid, chunk);
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
