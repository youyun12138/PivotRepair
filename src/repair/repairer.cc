#include "repair/repairer.hh"

namespace exr {

//Constructor
Repairer::Repairer(const Count &id, const Count &total,
                   const Path &load_path, const Path &store_path,
                   const Count &block_num, const DataSize &size,
                   const Path &bandwidth_path, const Name &eth_name,
                   const bool &if_print, const Count &recv_thr_num,
                   const Count &comp_thr_num, const Count &proc_thr_num)
    : id_(id), ac_(id, total), mp_(block_num, size),
      proceeder_(id, total, proc_thr_num, store_path, ac_),
      computer_(comp_thr_num, mp_, proceeder_),
      receiver_(total, id, load_path, recv_thr_num, ac_, mp_, computer_),
      bs_(eth_name, if_print), bandwidth_path_(bandwidth_path),
      on_run_(false) {}

//Destructor: to be sure that all the threads is already closed
Repairer::~Repairer() { WaitForFinish(); }

//Connect to other nodes and start the threads
void Repairer::Prepare(const IPAddressList &ip_addresses) {
  ac_.Connect(ip_addresses);
  receiver_.Run();
  computer_.Run();
  proceeder_.Run();

  std::unique_lock<std::mutex> lck(mtx_);
  task_getter_ = std::thread([&] { GetTaks(); });
  on_run_ = true;
}

//Used by creator to wait for this repairer closed by the master node
void Repairer::WaitForFinish() {
  std::unique_lock<std::mutex> lck(mtx_);
  if (on_run_) {
    task_getter_.join();
    on_run_ = false;
  }
}

//Get tasks from the master
void Repairer::GetTaks() {
  RepairTask rt; //接收到的任务
  Count src_id;  //任务源id
  while (true) {
    //Get from master and check if can quit
    ac_.Receive(0, sizeof(rt), &rt); //从master接收任务
    //检查接收到的任务信息，如果任务大小为 0 且任务块大小也为 0，则表示任务已全部完成，跳出循环。
    //否则，如果任务块大小不为 0，则需要设置带宽信息。
    if (rt.size == 0) {
      if (rt.piece_size == 0) {
        //No more task, the repair is ended
        break;
      } else {
        //Bandwidth
        if (rt.offset > 0) {
          //Need to reopen the bandwidhth file
          bs_.Open(bandwidth_path_);
        } else {
          //Load the bandwidth and set it
          if (!bs_.LoadNext()) {
            std::cerr << "Load bandwidth error" << std::endl;
            exit(-1);
          }
          bs_.SetBandwidth(id_, rt.bandwidth == 0);
        }
        //Tell the master that is already finished
        ac_.Send(0, sizeof(src_id), &src_id);
        continue;
      }
    }

    //如果接收到的任务大小不为 0，则说明有新的任务需要处理。
    //将任务的来源数量加一，然后使用 receiver_.PushData() 函数将任务发送给处理器进行处理，并等待所有处理器都接收到任务。
    //Has a new task, deliver to the processors
    rt.src_num += 1;
    receiver_.PushData({rt, id_});
    for (Count i = 1; i < rt.src_num; ++i) {
      ac_.Receive(0, sizeof(src_id), &src_id);
      receiver_.PushData({rt, src_id});
    }
    //if (rt.tar_id == id_) rt.piece_size = 0 - rt.piece_size;
    //receiver_.PushData({rt, id_});
  }
}

} // namespace exprocessors
