#include "repair/procs/compute_processor.hh"

#include <utility>
#include <vector>

namespace exr {

//Constructor and destructor
ComputeProcessor::ComputeProcessor(const Count &thr_n, MemoryPool &mp,
                                   DataProcessor<DataPiece> &next_prc)
    : DataProcessor<DataPiece>(1, thr_n), mp_(mp), next_prc_(next_prc),
      rc_(2, 1) {
  RSUnit coefs[2] = {1, 1};
  rc_.InitForEncode(coefs);
}

ComputeProcessor::~ComputeProcessor() { Close(); }

//Distribute
Count ComputeProcessor::Distribute(const DataPiece &data) { return 0; }

//Process the data
void ComputeProcessor::Process(DataPiece data, Count qid) {
  //Get Group, create one if not exist
  auto task_id = data.task_id;
  auto size = data.size;
  std::unique_lock<std::mutex> lck(mtx_);
  auto &pg = task_pieces_[task_id];
  lck.unlock();

  //如果数据的缓冲区为空并且大小大于 0，则说明这是一个任务信息，将其发送给下一个数据处理器，并将大小设为负值。
  //否则，将数据添加到 pg 中，如果添加失败，则将大小设为 0。
  //Deal with the content
  if (!(data.buf) && size > 0) {
    //Task info
    next_prc_.PushData(std::move(data));
    size = 0 - size;
  } else if (!AddPiece_(pg, std::move(data))) {
    //Data piece not sended out
    size = 0;
  }

  //获取 pg 的剩余互斥锁 rlck，并根据数据的大小更新 pg 中的信息。
  //如果数据大小为负，则说明这是一个任务信息，将 pg 的总大小设为负的数据大小的绝对值。
  //否则，将数据大小加到 pg 的总大小 total 中，并将 pg 的和 sum 加上数据大小。

  //如果 pg 的总大小等于和，说明所有数据都已经接收到了，这时释放 remain_mtx 互斥锁并获取 mtx_ 互斥锁，将 task_id 从 task_pieces_ 中移除。

  //Check if task ended
  std::unique_lock<std::mutex> rlck(pg.remain_mtx);
  if (size < 0)
    pg.total = 0 - size;
  else
    pg.sum += size;
  if (pg.sum == pg.total && pg.total > 0) {
    rlck.unlock();
    lck.lock();
    task_pieces_.erase(task_id);
  }
}

bool ComputeProcessor::AddPiece_(PieceGroup &pg, DataPiece data) {
  //Get piece, create one if not exist
  auto offset = data.offset;
  std::unique_lock<std::mutex> glck(pg.map_mtx);
  auto &ptp = pg.pieces[offset];
  //数据分片 data 的偏移量 offset 对应的数据分片 ptp 不存在时，创建一个新的数据分片 TempPiece，并将其添加到数据分片组 pg 中。
  if (!ptp) {
    ptp = std::make_unique<TempPiece>(std::move(data), 0);
    ptp->temp_buf = mp_.Get(0, offset);
    glck.unlock();
  } else { //如果 ptp 存在，则将 ptp 中的数据和 data 进行异或运算，并更新 ptp 中的信息。
    glck.unlock();
    //XOR two pieces and gather the infomation
    std::unique_lock<std::mutex> plck(ptp->mtx);
    ptp->dp.tar_id += data.tar_id;
    ptp->dp.delay_time += data.delay_time;
    if (!(ptp->dp.buf)) {
      ptp->dp.size = data.size;
      ptp->dp.buf = data.buf;
    } else if (data.buf) {
      BufUnit *srcs[2] = {ptp->dp.buf, data.buf},
              *tars[1] = {ptp->temp_buf};
      rc_.Encode(data.size, srcs, tars);
      ptp->dp.buf = ptp->temp_buf;
      ptp->temp_buf = data.buf;
    }
    plck.unlock();
  }


  //首先获取 ptp 的互斥锁 plck。接着，将 ptp->num 自增 1，表示接收到了一个数据分片。累加 ptp->src_num 和 data.src_num，表示当前数据分片的源数。

  //如果 ptp->src_num 等于 ptp->num，说明所有数据分片都已经接收到了，可以将它们发送给下一个数据处理器。
  //这时，先将 ptp->dp 封装成 DataPiece 类型，并使用 std::move() 将其移动到 next_prc_ 中，然后释放 plck 互斥锁并获取 glck 互斥锁，将 offset 从 pg.pieces 中移除，并返回布尔值 true。

  //如果 ptp->src_num 不等于 ptp->num，说明还没有接收到所有数据分片，不能将它们发送给下一个数据处理器。
  //这时，直接返回布尔值 false。

  //Check if need to send the data out
  std::unique_lock<std::mutex> plck(ptp->mtx);
  ++(ptp->num);
  ptp->src_num += data.src_num;
  if (ptp->src_num == ptp->num) {
    next_prc_.PushData(std::move(ptp->dp));
    plck.unlock();
    glck.lock();
    pg.pieces.erase(offset);
    return true;
  }
  return false;
}

} // namespace exr
