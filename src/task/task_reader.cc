#include "task/task_reader.hh"

#include <iostream>

namespace exr {

TaskReader::TaskReader(const Path &path) {
  task_file_ = std::ifstream(path);
  if (!task_file_.is_open()) {
    std::cerr << "Cannot open task file: " << path << std::endl;
    exit(-1);
  }
  task_file_ >> group_num_;
  cur_num_ = 0;
}

TaskReader::~TaskReader() {
  if (task_file_.is_open())
    task_file_.close();
}

Count TaskReader::GetNextGroupNumber() {
  if (++cur_num_ > group_num_) return kMaxGroupNum;

  capacity_ = 0;
  task_file_ >> task_num_;
  task_infos_ = std::make_unique<TaskInfo[]>(task_num_);
  for (Count i = 0; i < task_num_; ++i) {
    //Load task info
    task_file_ >> task_infos_[i].task_id >> task_infos_[i].offset
               >> task_infos_[i].size >> task_infos_[i].piece_size
               >> task_infos_[i].bandwidth >> task_infos_[i].node_num;
    capacity_ += task_infos_[i].bandwidth;
    //Get nodes' target
    auto node_tasks = std::make_unique<NodeTask[]>(task_infos_[i].node_num);
    for (Count j = 0; j < task_infos_[i].node_num; ++j)
      task_file_ >> node_tasks[j].node_id >> node_tasks[j].tar_id;
    task_infos_[i].node_tasks = std::move(node_tasks);
  }
  return 1;
}

Count TaskReader::GetTaskNumber(const Count &gid) {
  return gid == 0 ? task_num_ : 0;
}

// gid：类型为Count，表示全局ID。
// tid：类型为Count，表示任务ID。
// node_id：类型为Count，表示节点ID。
// rt：类型为RepairTask的引用，表示需要填充的修复任务对象。
// src_ids：类型为Count*的指针，表示源ID列表。

//从一个任务中获取特定节点的修复任务信息，并将其填充到RepairTask对象中。
void TaskReader::FillTask(const Count &gid, const Count &tid,
                          const Count &node_id,
                          RepairTask &rt, Count *src_ids) {
  // 通过任务ID获取任务信息，存储在auto &task中。
  auto &task = task_infos_[tid];
  rt.tar_id = 0;
  rt.src_num = 0;
  //遍历任务中的所有节点任务，；
  for (Count i = 0; i < task.node_num; ++i) {
    auto &ntask = task.node_tasks[i];
    //对于每个节点任务，如果其node_id等于node_id，则将其tar_id赋值给rt.tar_id
    if (ntask.node_id == node_id)
      rt.tar_id = ntask.tar_id;
    // 否则，如果其tar_id等于node_id，则将其node_id添加到src_ids中，并将rt.src_num自增。
    else if (ntask.tar_id == node_id)
      src_ids[(rt.src_num)++] = ntask.node_id;
  }
  //如果rt.tar_id等于0，则将rt.size赋值为0并返回。
  if (rt.tar_id == 0) {
    rt.size = 0;
    return;
  }

  rt.offset = task.offset;
  rt.size = task.size;
  rt.piece_size = task.piece_size;
  rt.bandwidth = task.bandwidth;
}

BwType TaskReader::get_capacity() { return capacity_; }

Count TaskReader::GetRid() { return 0; }

} // namespace exr
