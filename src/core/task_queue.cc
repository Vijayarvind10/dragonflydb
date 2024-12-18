#include "core/task_queue.h"
#include <absl/strings/str_cat.h>
#include <queue>
#include <atomic>
#include <memory>
#include <mutex>

#include "base/logging.h"

using namespace std;

namespace dfly {

__thread unsigned TaskQueue::blocked_submitters_ = 0;

class TaskQueue::Task {
public:
    Task(function<void()> task_func, unsigned priority)
        : task_func_(move(task_func)), priority_(priority) {}

    void Execute() const { task_func_(); }

    unsigned GetPriority() const { return priority_; }

private:
    function<void()> task_func_;
    unsigned priority_;
};

struct TaskComparator {
    bool operator()(const unique_ptr<TaskQueue::Task>& a, const unique_ptr<TaskQueue::Task>& b) {
        return a->GetPriority() < b->GetPriority();
    }
};

TaskQueue::TaskQueue(unsigned queue_size, unsigned start_size, unsigned pool_max_size)
    : queue_(queue_size), consumer_fibers_(start_size), pool_max_size_(pool_max_size) {
  CHECK_GT(start_size, 0u);
  CHECK_LE(start_size, pool_max_size);
  CHECK_GT(queue_size, 0u);
}

void TaskQueue::Start(std::string_view base_name) {
    for (size_t i = 0; i < consumer_fibers_.size(); ++i) {
        auto& fb = consumer_fibers_[i];
        CHECK(!fb.IsJoinable());

        string name = absl::StrCat(base_name, "/", i);
        fb = util::fb2::Fiber(name, [this] { this->Run(); });
    }
}

void TaskQueue::Shutdown() {
    queue_.Shutdown();
    {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        shutting_down_ = true;
    }
    for (auto& fb : consumer_fibers_) {
        fb.JoinIfNeeded();
    }
}

void TaskQueue::AdjustFiberPoolSize() {
    size_t queue_size = queue_.Size();
    size_t current_size = consumer_fibers_.size();
    
    if (queue_size > current_size && current_size < pool_max_size_) {
        size_t new_fiber_count = std::min(pool_max_size_, current_size + 1);
        for (size_t i = current_size; i < new_fiber_count; ++i) {
            string name = absl::StrCat("consumer_fiber/", i);
            consumer_fibers_[i] = util::fb2::Fiber(name, [this] { this->Run(); });
        }
    } else if (queue_size < current_size / 2) {
        size_t new_fiber_count = std::max(1u, current_size - 1);
        consumer_fibers_.resize(new_fiber_count);
    }
}

void TaskQueue::SubmitTask(function<void()> task_func, unsigned priority) {
    if (shutting_down_) {
        LOG(WARNING) << "Attempted to submit a task after shutdown.";
        return;
    }
    auto task = make_unique<Task>(move(task_func), priority);
    std::lock_guard<std::mutex> lock(queue_mutex_);
    queue_.push(move(task));
    AdjustFiberPoolSize();
}

void TaskQueue::Run() {
    while (true) {
        unique_ptr<Task> task = nullptr;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (!queue_.empty()) {
                task = move(queue_.top());
                queue_.pop();
            }
        }

        if (task) {
            task->Execute();
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void TaskQueue::Cancel() {
    std::lock_guard<std::mutex> lock(shutdown_mutex_);
    shutting_down_ = true;
}

}  // namespace dfly

