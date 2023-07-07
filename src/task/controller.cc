#include "task/controller.hh"

#include <sys/time.h>
#include <thread>

#include "task/algorithm/ftp_repair.hh"
#include "task/algorithm/ppr.hh"
#include "task/task_reader.hh"
#include "util/types.hh"

namespace exr {

Controller::Controller(const Count &total,
                       const DataSize &size, const DataSize &psize)
    : size_(size), psize_(psize), ac_(0, total), ptg_(nullptr),
      cur_tid_(0), gnum_(0), task_num_(0) {
  src_lists_ = std::make_unique<std::unique_ptr<Count[]>[]>(total - 1);
  for (Count i = 0; i < total - 1; ++i)
    src_lists_[i] = std::make_unique<Count[]>(total - 2);
}

Controller::~Controller() = default;

void Controller::Connect(const IPAddressList &ip_addresses) {
  ac_.Connect(ip_addresses);
}

void Controller::ChangeAlg(const Alg &alg, const Count *args,
                           const Path &path) {
  if (alg == 't') {
    ptg_ = pTaskGetter(new TaskReader(path));
  } else if (alg == 'j') {
    ptg_ = pTaskGetter(new PPR(
            args[0], args[1], args[2], args[3] * 1000, path));
  } else {
    ptg_ = pTaskGetter(new FTPRepair(
            args[0], args[1], args[2], alg, args[3] * 1000, path));
  }
}

//Calculate or load the path
bool Controller::GetTasks() {
  gnum_ = ptg_->GetNextGroupNumber();
  if (gnum_ == kMaxGroupNum) {
    return false;
  }
  return true;
}

BwType Controller::GetCapacity() { return ptg_->get_capacity(); }


//将一组任务分配给多个线程进行处理，并等待所有线程处理完毕后返回处理过的最大任务数。
Count Controller::DoTaskGroups(const Count &total) {
  //声明一个变量 max_task_num，表示处理过的最大任务数，初值为0。
  Count max_task_num = 0;
  //使用 std::make_unique 创建一个 std::unique_ptrstd::thread[] 对象 t，这个对象是一个数组，数组大小为 total-1。
  auto t = std::make_unique<std::thread[]>(total - 1);
  //通过一个 for 循环，遍历所有任务组，任务组的数量为 gnum_。
  //在循环的每次迭代中，ptg_->GetTaskNumber(i) 获取第 i 组任务的任务数，将其赋值给 task_num_。
  for (Count i = 0; i < gnum_; ++i) {
    task_num_ = ptg_->GetTaskNumber(i);
    //Send tasks of one group
    //在循环中，使用两个 for 循环，将任务分配给多个线程进行处理。
    //其中，第一个 for 循环从 1 开始，到 total-1 结束，创建 total-1 个线程，分别执行 DeliverTasks_(i, j) 函数。
    //这个函数用于将第 i 组任务中的第 j 个任务交给一个线程处理。
    //这里使用了 [&, i, j] 的 lambda 表达式进行线程函数的绑定。
    //其中的 & 表示以引用的方式捕获所有变量，i 和 j 表示以值的方式捕获这两个变量。
    for (Count j = 1; j < total; ++j)
      t[j-1] = std::thread([&, i, j] { DeliverTasks_(i, j); });
    //遍历所有线程对象，调用 join() 方法，等待所有线程处理完毕。
    //这样可以保证在当前任务组中所有任务都分配给线程并处理完毕后才会进入下一组任务。
    for (Count j = 0; j < total - 1; ++j) t[j].join();
    //Wait for finishing
    //如果当前任务组的任务数 task_num_ 大于 max_task_num，则更新 max_task_num 的值。
    if (task_num_ > max_task_num) max_task_num = task_num_;
    WaitForFinish_();
  }
  //循环结束后，返回处理过的最大任务数 max_task_num。
  return max_task_num;
}

void Controller::Close(const Count &total) {
  RepairTask end_task{0, 0, 0, 0, 0, 0, 0, 0};
  for (Count i = 1; i < total; ++i)
    ac_.Send(i, sizeof(end_task), &end_task);
}

void Controller::ReloadNodeBandwidth(const Count &total) {
  RepairTask reload_info{0, 0, 0, 1, 0, 1, 0, 0};
  for (Count i = 1; i < total; ++i)
    ac_.Send(i, sizeof(reload_info), &reload_info);
  Count r;
  for (Count i = 1; i < total; ++i) ac_.Receive(i, sizeof(r), &r);
}

void Controller::SetNewNodeBandwidth(const Count &total) {
  RepairTask set_new_info{0, 0, 0, 0, 0, 1, 0, 1};
  for (Count i = 1; i < total; ++i) {
    if (i == ptg_->GetRid()) set_new_info.bandwidth = 0;
    ac_.Send(i, sizeof(set_new_info), &set_new_info);
    set_new_info.bandwidth = 1;
  }
  Count r;
  for (Count i = 1; i < total; ++i) ac_.Receive(i, sizeof(r), &r);
}


//函数实现了将任务分配给指定节点的功能。
void Controller::DeliverTasks_(const Count &gid, const Count &nid){
  //函数首先通过 src_lists_ 成员变量获取指定节点的源列表（src_lists_ 是一个存储源列表的向量，nid - 1 表示该节点在向量中的下标）。
  auto &srcs = src_lists_[nid - 1];
  //函数使用一个循环来处理任务。循环变量 j 从 0 开始递增，同时引入了一个 tid 变量，其初始值为 cur_tid_。每次循环，j 和 tid 都会递增。task_num_ 是任务的总数。
  for (Count j = 0, tid = cur_tid_; j < task_num_; ++j, ++tid) {
    //Get task's content

    //函数首先创建一个 RepairTask 类型的对象 rt，并设置了它的一些属性。然后通过调用 ptg_->FillTask 将任务分配给指定节点。
    //该函数的参数包括 gid、j、nid、rt 和 srcs.get()。srcs.get() 返回指向源列表的指针。
    RepairTask rt{tid, 0, 0, 0, size_, psize_, 1, 0};
    ptg_->FillTask(gid, j, nid, rt, srcs.get());

    //Send to the node
    //如果 rt.size 大于 0，说明任务非空，函数将通过 ac_.Send 将任务发送给指定的节点 nid。
    if (rt.size > 0) {
      ac_.Send(nid, sizeof(rt), &rt);
      for (Count k = 0; k < rt.src_num; ++k)
        ac_.Send(nid, sizeof(srcs[k]), &(srcs[k]));
      //如果 rt.tar_id 等于 nid，说明任务的目标节点就是当前节点，函数将该节点的 ID 添加到 waits_ 向量中。
      if (rt.tar_id == nid) {
        //需要注意的是，在添加节点 ID 到 waits_ 向量之前，函数会获取一个互斥锁以保证线程安全。
        std::unique_lock<std::mutex> lck(mtx_);
        waits_.push_back(nid);
        lck.unlock();
      }
    }
  }
}

//等待所有任务完成。
//在循环中，通过 ac_.Receive() 方法接收指定地址处的数据，并将数据存储到 recv 变量中。
//在接收完所有数据后，将 cur_tid_ 的值加上 task_num_，表示当前已经处理的任务数。
//最后，清空 waits_ 容器，准备下一轮任务的接收。
void Controller::WaitForFinish_() {
  //Wait for finish
  //声明一个变量 recv，表示接收到的数据大小。
  Count recv;
  //遍历 waits_ 这个 vector 容器中的所有元素，其中 waits_ 是一个存储了所有等待接收的数据的 vector 容器。
  for (auto &x: waits_)
  //使用 ac_.Receive() 方法，从指定的地址 x 处接收 sizeof(recv) 个字节的数据，并将接收到的数据存储到 recv 变量中。
    ac_.Receive(x, sizeof(recv), &recv);
  //将 cur_tid_ 的值加上 task_num_，这里的 cur_tid_ 表示当前已经处理的任务数，task_num_ 表示当前任务组中的任务总数。
  cur_tid_ += task_num_;
  //清空 waits_ 容器
  waits_.clear();
}

} // namespace exr
