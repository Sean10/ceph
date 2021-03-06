// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCopyRequest.h"
#include "include/neorados/RADOS.hpp"
#include "common/errno.h"
#include "librados/snap_set_diff.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ReadResult.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::ObjectCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
ObjectCopyRequest<I>::ObjectCopyRequest(I *src_image_ctx,
                                        I *dst_image_ctx,
                                        librados::snap_t src_snap_id_start,
                                        librados::snap_t dst_snap_id_start,
                                        const SnapMap &snap_map,
                                        uint64_t dst_object_number,
                                        bool flatten, Handler* handler,
                                        Context *on_finish)
  : m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_cct(dst_image_ctx->cct),
    m_src_snap_id_start(src_snap_id_start),
    m_dst_snap_id_start(dst_snap_id_start), m_snap_map(snap_map),
    m_dst_object_number(dst_object_number), m_flatten(flatten),
    m_handler(handler), m_on_finish(on_finish) {
  ceph_assert(src_image_ctx->data_ctx.is_valid());
  ceph_assert(dst_image_ctx->data_ctx.is_valid());
  ceph_assert(!m_snap_map.empty());

  m_src_async_op = new io::AsyncOperation();
  m_src_async_op->start_op(*util::get_image_ctx(m_src_image_ctx));

  m_src_io_ctx.dup(m_src_image_ctx->data_ctx);
  m_dst_io_ctx.dup(m_dst_image_ctx->data_ctx);

  m_dst_oid = m_dst_image_ctx->get_object_name(dst_object_number);

  ldout(m_cct, 20) << "dst_oid=" << m_dst_oid << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::send() {
  send_list_snaps();
}

template <typename I>
void ObjectCopyRequest<I>::send_list_snaps() {
  // image extents are consistent across src and dst so compute once
  Striper::extent_to_file(m_cct, &m_dst_image_ctx->layout, m_dst_object_number,
                          0, m_dst_image_ctx->layout.object_size,
                          m_image_extents);
  ldout(m_cct, 20) << "image_extents=" << m_image_extents << dendl;

  io::SnapIds snap_ids;
  snap_ids.reserve(1 + m_snap_map.size());
  snap_ids.push_back(m_src_snap_id_start);
  for (auto& [src_snap_id, _] : m_snap_map) {
    if (src_snap_id != snap_ids.front()) {
      snap_ids.push_back(src_snap_id);
    }
  }

  auto list_snaps_flags = io::LIST_SNAPS_FLAG_DISABLE_LIST_FROM_PARENT;

  m_snapshot_delta.clear();

  auto ctx = create_context_callback<
    ObjectCopyRequest, &ObjectCopyRequest<I>::handle_list_snaps>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, util::get_image_ctx(m_src_image_ctx), io::AIO_TYPE_GENERIC);
  auto req = io::ImageDispatchSpec::create_list_snaps(
    *m_src_image_ctx, io::IMAGE_DISPATCH_LAYER_NONE, aio_comp,
    io::Extents{m_image_extents}, std::move(snap_ids), list_snaps_flags,
    &m_snapshot_delta, {});
  req->send();
}

template <typename I>
void ObjectCopyRequest<I>::handle_list_snaps(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list snaps: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  ldout(m_cct, 20) << "snapshot_delta=" << m_snapshot_delta << dendl;

  compute_dst_object_may_exist();
  compute_read_ops();

  send_read();
}

template <typename I>
void ObjectCopyRequest<I>::send_read() {
  if (m_read_snaps.empty()) {
    // all snapshots have been read
    merge_write_ops();
    compute_zero_ops();

    if (m_write_ops.empty()) {
      // nothing to copy
      finish(-ENOENT);
      return;
    }

    send_write_object();
    return;
  }

  auto index = *m_read_snaps.begin();
  auto& read_op = m_read_ops[index];
  if (read_op.image_interval.empty()) {
    // nothing written to this object for this snapshot (must be trunc/remove)
    handle_read(0);
    return;
  }

  auto io_context = m_src_image_ctx->duplicate_data_io_context();
  io_context->read_snap(index.second);

  io::Extents image_extents{read_op.image_interval.begin(),
                            read_op.image_interval.end()};
  io::ReadResult read_result{&read_op.image_extent_map,
                             &read_op.out_bl};

  ldout(m_cct, 20) << "read: src_snap_seq=" << index.second << ", "
                   << "image_extents=" << image_extents << dendl;

  int op_flags = (LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                  LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

  int read_flags = 0;
  if (index.second != m_src_image_ctx->snap_id) {
    read_flags |= io::READ_FLAG_DISABLE_CLIPPING;
  }

  auto ctx = create_context_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, util::get_image_ctx(m_src_image_ctx), io::AIO_TYPE_READ);

  auto req = io::ImageDispatchSpec::create_read(
    *m_src_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    std::move(image_extents), std::move(read_result), io_context, op_flags,
    read_flags, {});
  req->send();
}

template <typename I>
void ObjectCopyRequest<I>::handle_read(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to read from source object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (m_handler != nullptr) {
    auto index = *m_read_snaps.begin();
    auto& read_op = m_read_ops[index];
    m_handler->handle_read(read_op.out_bl.length());
  }

  ceph_assert(!m_read_snaps.empty());
  m_read_snaps.erase(m_read_snaps.begin());

  send_read();
}

template <typename I>
void ObjectCopyRequest<I>::send_write_object() {
  ceph_assert(!m_write_ops.empty());
  auto& write_ops = m_write_ops.begin()->second;

  // retrieve the destination snap context for the op
  SnapIds dst_snap_ids;
  librados::snap_t dst_snap_seq = 0;
  librados::snap_t src_snap_seq = m_write_ops.begin()->first;
  if (src_snap_seq != 0) {
    auto snap_map_it = m_snap_map.find(src_snap_seq);
    ceph_assert(snap_map_it != m_snap_map.end());

    auto dst_snap_id = snap_map_it->second.front();
    auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_id);
    ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
    if (!dst_may_exist_it->second && !write_ops.empty()) {
      // if the object cannot exist, the only valid op is to remove it
      ldout(m_cct, 20) << "object DNE: src_snap_seq=" << src_snap_seq << dendl;
      ceph_assert(write_ops.size() == 1U);
      ceph_assert(write_ops.begin()->type == WRITE_OP_TYPE_REMOVE);
    }

    // write snapshot context should be before actual snapshot
    ceph_assert(!snap_map_it->second.empty());
    auto dst_snap_ids_it = snap_map_it->second.begin();
    ++dst_snap_ids_it;

    dst_snap_ids = SnapIds{dst_snap_ids_it, snap_map_it->second.end()};
    if (!dst_snap_ids.empty()) {
      dst_snap_seq = dst_snap_ids.front();
    }
    ceph_assert(dst_snap_seq != CEPH_NOSNAP);
  }

  ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                   << "dst_snap_seq=" << dst_snap_seq << ", "
                   << "dst_snaps=" << dst_snap_ids << dendl;

  librados::ObjectWriteOperation op;
  if (!m_dst_image_ctx->migration_info.empty()) {
    cls_client::assert_snapc_seq(&op, dst_snap_seq,
                                 cls::rbd::ASSERT_SNAPC_SEQ_GT_SNAPSET_SEQ);
  }

  for (auto& write_op : write_ops) {
    switch (write_op.type) {
    case WRITE_OP_TYPE_WRITE:
      ldout(m_cct, 20) << "write op: " << write_op.object_offset << "~"
                       << write_op.object_length << dendl;
      op.write(write_op.object_offset, write_op.bl);
      op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                       LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      break;
    case WRITE_OP_TYPE_ZERO:
      ldout(m_cct, 20) << "zero op: " << write_op.object_offset << "~"
                       << write_op.object_length << dendl;
      op.zero(write_op.object_offset, write_op.object_length);
      break;
    case WRITE_OP_TYPE_REMOVE_TRUNC:
      ldout(m_cct, 20) << "create op" << dendl;
      op.create(false);
      [[fallthrough]];
    case WRITE_OP_TYPE_TRUNC:
      ldout(m_cct, 20) << "trunc op: " << write_op.object_offset << dendl;
      op.truncate(write_op.object_offset);
      break;
    case WRITE_OP_TYPE_REMOVE:
      ldout(m_cct, 20) << "remove op" << dendl;
      op.remove();
      break;
    default:
      ceph_abort();
    }
  }

  if (op.size() == (m_dst_image_ctx->migration_info.empty() ? 0 : 1)) {
    handle_write_object(0);
    return;
  }

  int r;
  Context *finish_op_ctx;
  {
    std::shared_lock owner_locker{m_dst_image_ctx->owner_lock};
    finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock, &r);
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_write_object(r);
      finish_op_ctx->complete(0);
    });
  librados::AioCompletion *comp = create_rados_callback(ctx);
  r = m_dst_io_ctx.aio_operate(m_dst_oid, comp, &op, dst_snap_seq, dst_snap_ids,
                               nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_write_object(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  } else if (r == -ERANGE) {
    ldout(m_cct, 10) << "concurrent deep copy" << dendl;
    r = 0;
  }
  if (r < 0) {
    lderr(m_cct) << "failed to write to destination object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  m_write_ops.erase(m_write_ops.begin());
  if (!m_write_ops.empty()) {
    send_write_object();
    return;
  }

  send_update_object_map();
}

template <typename I>
void ObjectCopyRequest<I>::send_update_object_map() {
  if (!m_dst_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP) ||
      m_dst_object_state.empty()) {
    finish(0);
    return;
  }

  m_dst_image_ctx->owner_lock.lock_shared();
  m_dst_image_ctx->image_lock.lock_shared();
  if (m_dst_image_ctx->object_map == nullptr) {
    // possible that exclusive lock was lost in background
    lderr(m_cct) << "object map is not initialized" << dendl;

    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
    finish(-EINVAL);
    return;
  }

  auto &dst_object_state = *m_dst_object_state.begin();
  auto it = m_snap_map.find(dst_object_state.first);
  ceph_assert(it != m_snap_map.end());
  auto dst_snap_id = it->second.front();
  auto object_state = dst_object_state.second;
  m_dst_object_state.erase(m_dst_object_state.begin());

  ldout(m_cct, 20) << "dst_snap_id=" << dst_snap_id << ", object_state="
                   << static_cast<uint32_t>(object_state) << dendl;

  int r;
  auto finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock, &r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
    finish(r);
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_update_object_map(r);
      finish_op_ctx->complete(0);
    });

  auto dst_image_ctx = m_dst_image_ctx;
  bool sent = dst_image_ctx->object_map->template aio_update<
    Context, &Context::complete>(dst_snap_id, m_dst_object_number, object_state,
                                 {}, {}, false, ctx);

  // NOTE: state machine might complete before we reach here
  dst_image_ctx->image_lock.unlock_shared();
  dst_image_ctx->owner_lock.unlock_shared();
  if (!sent) {
    ceph_assert(dst_snap_id == CEPH_NOSNAP);
    ctx->complete(0);
  }
}

template <typename I>
void ObjectCopyRequest<I>::handle_update_object_map(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update object map: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_dst_object_state.empty()) {
    send_update_object_map();
    return;
  }
  finish(0);
}

template <typename I>
Context *ObjectCopyRequest<I>::start_lock_op(ceph::shared_mutex &owner_lock,
					     int* r) {
  ceph_assert(ceph_mutex_is_locked(m_dst_image_ctx->owner_lock));
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    return new LambdaContext([](int r) {});
  }
  return m_dst_image_ctx->exclusive_lock->start_op(r);
}

template <typename I>
void ObjectCopyRequest<I>::compute_read_ops() {
  ldout(m_cct, 20) << dendl;

  m_src_image_ctx->image_lock.lock_shared();
  bool read_from_parent = (m_src_snap_id_start == 0 &&
                           m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->image_lock.unlock_shared();

  bool only_dne_extents = true;
  interval_set<uint64_t> dne_image_interval;

  // compute read ops for any data sections or for any extents that we need to
  // read from our parent
  for (auto& [key, image_intervals] : m_snapshot_delta) {
    io::WriteReadSnapIds write_read_snap_ids{key};

    // advance the src write snap id to the first valid snap id
    if (write_read_snap_ids != io::INITIAL_WRITE_READ_SNAP_IDS) {
      // don't attempt to read from snapshots that shouldn't exist in
      // case the OSD fails to give a correct snap list
      auto snap_map_it = m_snap_map.find(write_read_snap_ids.first);
      ceph_assert(snap_map_it != m_snap_map.end());
      auto dst_snap_seq = snap_map_it->second.front();

      auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_seq);
      ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
      if (!dst_may_exist_it->second) {
        ldout(m_cct, 20) << "DNE snapshot: " << write_read_snap_ids.first
                         << dendl;
        continue;
      }
    }

    for (auto& image_interval : image_intervals) {
      auto state = image_interval.get_val().state;
      switch (state) {
      case io::SNAPSHOT_EXTENT_STATE_DNE:
        ceph_assert(write_read_snap_ids == io::INITIAL_WRITE_READ_SNAP_IDS);
        if (read_from_parent) {
          // special-case for DNE object-extents since when flattening we need
          // to read data from the parent images extents
          ldout(m_cct, 20) << "DNE extent: "
                           << image_interval.get_off() << "~"
                           << image_interval.get_len() << dendl;
          dne_image_interval.insert(
            image_interval.get_off(), image_interval.get_len());
        }
        break;
      case io::SNAPSHOT_EXTENT_STATE_ZEROED:
        only_dne_extents = false;
        break;
      case io::SNAPSHOT_EXTENT_STATE_DATA:
        ldout(m_cct, 20) << "read op: "
                         << "snap_ids=" << write_read_snap_ids << " "
                         << image_interval.get_off() << "~"
                         << image_interval.get_len() << dendl;
        m_read_ops[write_read_snap_ids].image_interval.union_insert(
          image_interval.get_off(), image_interval.get_len());
        only_dne_extents = false;
        break;
      default:
        ceph_abort();
        break;
      }
    }
  }

  if (!dne_image_interval.empty() && (!only_dne_extents || m_flatten)) {
    auto snap_map_it = m_snap_map.begin();
    ceph_assert(snap_map_it != m_snap_map.end());

    auto src_snap_seq = snap_map_it->first;
    WriteReadSnapIds write_read_snap_ids{src_snap_seq, src_snap_seq};

    // prepare to prune the extents to the maximum parent overlap
    m_src_image_ctx->image_lock.lock_shared();
    uint64_t src_parent_overlap = 0;
    int r = m_src_image_ctx->get_parent_overlap(src_snap_seq,
                                                &src_parent_overlap);
    m_src_image_ctx->image_lock.unlock_shared();

    if (r < 0) {
      ldout(m_cct, 5) << "failed getting parent overlap for snap_id: "
                      << src_snap_seq << ": " << cpp_strerror(r) << dendl;
    } else {
      ldout(m_cct, 20) << "parent overlap=" << src_parent_overlap << dendl;
      for (auto& [image_offset, image_length] : dne_image_interval) {
        auto end_image_offset = std::min(
          image_offset + image_length, src_parent_overlap);
        if (image_offset >= end_image_offset) {
          // starting offset is beyond the end of the parent overlap
          continue;
        }

        image_length = end_image_offset - image_offset;
        ldout(m_cct, 20) << "parent read op: "
                         << "snap_ids=" << write_read_snap_ids << " "
                         << image_offset << "~" << image_length << dendl;
        m_read_ops[write_read_snap_ids].image_interval.union_insert(
          image_offset, image_length);
      }
    }
  }

  for (auto& [write_read_snap_ids, _] : m_read_ops) {
    m_read_snaps.push_back(write_read_snap_ids);
  }
}

template <typename I>
void ObjectCopyRequest<I>::merge_write_ops() {
  ldout(m_cct, 20) << dendl;

  for (auto& [write_read_snap_ids, read_op] : m_read_ops) {
    auto src_snap_seq = write_read_snap_ids.first;

    // convert the the resulting sparse image extent map to an interval ...
    auto& image_data_interval = m_dst_data_interval[src_snap_seq];
    for (auto [image_offset, image_length] : read_op.image_extent_map) {
      image_data_interval.union_insert(image_offset, image_length);
    }

    // ... and compute the difference between it and the image extents since
    // that indicates zeroed extents
    interval_set<uint64_t> intersection;
    intersection.intersection_of(read_op.image_interval, image_data_interval);
    read_op.image_interval.subtract(intersection);

    for (auto& [image_offset, image_length] : read_op.image_interval) {
      ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                       << "inserting sparse-read zero " << image_offset << "~"
                       << image_length << dendl;
      m_dst_zero_interval[src_snap_seq].union_insert(
        image_offset, image_length);
    }

    uint64_t buffer_offset = 0;
    for (auto [image_offset, image_length] : read_op.image_extent_map) {
      // convert image extents back to object extents for the write op
      striper::LightweightObjectExtents object_extents;
      Striper::file_to_extents(m_cct, &m_dst_image_ctx->layout, image_offset,
                               image_length, 0, buffer_offset, &object_extents);
      for (auto& object_extent : object_extents) {
        ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                         << "object_offset=" << object_extent.offset << ", "
                         << "object_length=" << object_extent.length << dendl;

        bufferlist tmp_bl;
        tmp_bl.substr_of(read_op.out_bl, buffer_offset, object_extent.length);
        m_write_ops[src_snap_seq].emplace_back(
          WRITE_OP_TYPE_WRITE, object_extent.offset, object_extent.length,
          std::move(tmp_bl));

        buffer_offset += object_extent.length;
      }
    }
  }
}

template <typename I>
void ObjectCopyRequest<I>::compute_zero_ops() {
  ldout(m_cct, 20) << dendl;

  m_src_image_ctx->image_lock.lock_shared();
  bool hide_parent = (m_src_snap_id_start == 0 &&
                      m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->image_lock.unlock_shared();

  // collect all known zeroed extents from the snapshot delta
  for (auto& [write_read_snap_ids, image_intervals] : m_snapshot_delta) {
    auto src_snap_seq = write_read_snap_ids.first;
    for (auto& image_interval : image_intervals) {
      auto state = image_interval.get_val().state;
      switch (state) {
      case io::SNAPSHOT_EXTENT_STATE_ZEROED:
        if (write_read_snap_ids != io::WriteReadSnapIds{0, 0}) {
          ldout(m_cct, 20) << "zeroed extent: "
                           << "src_snap_seq=" << src_snap_seq << " "
                           << image_interval.get_off() << "~"
                           << image_interval.get_len() << dendl;
          m_dst_zero_interval[src_snap_seq].union_insert(
            image_interval.get_off(), image_interval.get_len());
        } else if (hide_parent) {
          auto first_src_snap_id = m_snap_map.begin()->first;
          ldout(m_cct, 20) << "zeroed (hide parent) extent: "
                           << "src_snap_seq=" << first_src_snap_id << "  "
                           << image_interval.get_off() << "~"
                           << image_interval.get_len() << dendl;
          m_dst_zero_interval[first_src_snap_id].union_insert(
            image_interval.get_off(), image_interval.get_len());
        }
        break;
      case io::SNAPSHOT_EXTENT_STATE_DNE:
      case io::SNAPSHOT_EXTENT_STATE_DATA:
        break;
      default:
        ceph_abort();
        break;
      }
    }
  }

  bool fast_diff = m_dst_image_ctx->test_features(RBD_FEATURE_FAST_DIFF);
  uint64_t prev_end_size = 0;

  // ensure we have a zeroed interval for each snapshot
  for (auto& [src_snap_seq, _] : m_snap_map) {
    m_dst_zero_interval[src_snap_seq];
  }

  // compute zero ops from the zeroed intervals
  for (auto &it : m_dst_zero_interval) {
    auto src_snap_seq = it.first;
    auto &zero_interval = it.second;

    // subtract any data intervals from our zero intervals
    auto& data_interval = m_dst_data_interval[src_snap_seq];
    interval_set<uint64_t> intersection;
    intersection.intersection_of(zero_interval, data_interval);
    zero_interval.subtract(intersection);

    auto snap_map_it = m_snap_map.find(src_snap_seq);
    ceph_assert(snap_map_it != m_snap_map.end());
    auto dst_snap_seq = snap_map_it->second.front();

    auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_seq);
    ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
    if (!dst_may_exist_it->second && prev_end_size > 0) {
      ldout(m_cct, 5) << "object DNE for snap_id: " << dst_snap_seq << dendl;
      m_write_ops[src_snap_seq].emplace_back(WRITE_OP_TYPE_REMOVE, 0, 0);
      prev_end_size = 0;
      continue;
    }

    if (hide_parent) {
      std::shared_lock image_locker{m_dst_image_ctx->image_lock};
      uint64_t parent_overlap = 0;
      int r = m_dst_image_ctx->get_parent_overlap(dst_snap_seq,
                                                  &parent_overlap);
      if (r < 0) {
        ldout(m_cct, 5) << "failed getting parent overlap for snap_id: "
                        << dst_snap_seq << ": " << cpp_strerror(r) << dendl;
      }
      if (parent_overlap == 0) {
        ldout(m_cct, 20) << "no parent overlap" << dendl;
        hide_parent = false;
      } else {
        auto image_extents = m_image_extents;
        uint64_t overlap = m_dst_image_ctx->prune_parent_extents(
          image_extents, parent_overlap);
        if (overlap == 0) {
          ldout(m_cct, 20) << "no parent overlap" << dendl;
          hide_parent = false;
        } else if (src_snap_seq == m_dst_zero_interval.begin()->first) {
          for (auto e : image_extents) {
            prev_end_size += e.second;
          }
          ceph_assert(prev_end_size <= m_dst_image_ctx->layout.object_size);
        }
      }
    }

    uint64_t end_size = prev_end_size;

    // update end_size if there are writes into higher offsets
    auto iter = m_write_ops.find(src_snap_seq);
    if (iter != m_write_ops.end()) {
      for (auto &write_op : iter->second) {
        end_size = std::max(
          end_size, write_op.object_offset + write_op.object_length);
      }
    }

    ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                     << "dst_snap_seq=" << dst_snap_seq << ", "
                     << "zero_interval=" << zero_interval << ", "
                     << "end_size=" << end_size << dendl;
    for (auto z = zero_interval.begin(); z != zero_interval.end(); ++z) {
      // convert image extents back to object extents for the write op
      striper::LightweightObjectExtents object_extents;
      Striper::file_to_extents(m_cct, &m_dst_image_ctx->layout, z.get_start(),
                               z.get_len(), 0, 0, &object_extents);
      for (auto& object_extent : object_extents) {
        if (object_extent.offset + object_extent.length >= end_size) {
          // zero interval at the object end
          if (object_extent.offset == 0 && hide_parent) {
            ldout(m_cct, 20) << "WRITE_OP_TYPE_REMOVE_TRUNC" << dendl;
            m_write_ops[src_snap_seq].emplace_back(
              WRITE_OP_TYPE_REMOVE_TRUNC, 0, 0);
          } else if (object_extent.offset < prev_end_size) {
            if (object_extent.offset == 0) {
              ldout(m_cct, 20) << "WRITE_OP_TYPE_REMOVE" << dendl;
              m_write_ops[src_snap_seq].emplace_back(
                WRITE_OP_TYPE_REMOVE, 0, 0);
            } else {
              ldout(m_cct, 20) << "WRITE_OP_TYPE_TRUNC " << object_extent.offset
                               << dendl;
              m_write_ops[src_snap_seq].emplace_back(
                WRITE_OP_TYPE_TRUNC, object_extent.offset, 0);
            }
          }
          end_size = std::min(end_size, object_extent.offset);
        } else {
          // zero interval inside the object
          ldout(m_cct, 20) << "WRITE_OP_TYPE_ZERO "
                           << object_extent.offset << "~"
                           << object_extent.length << dendl;
          m_write_ops[src_snap_seq].emplace_back(
            WRITE_OP_TYPE_ZERO, object_extent.offset, object_extent.length);
        }
      }
    }

    ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                     << "end_size=" << end_size << dendl;
    if (end_size > 0 || hide_parent) {
      m_dst_object_state[src_snap_seq] = OBJECT_EXISTS;
      if (fast_diff && end_size == prev_end_size &&
          m_write_ops.count(src_snap_seq) == 0) {
        m_dst_object_state[src_snap_seq] = OBJECT_EXISTS_CLEAN;
      }
    }
    prev_end_size = end_size;
  }
}

template <typename I>
void ObjectCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  // ensure IoCtxs are closed prior to proceeding
  auto on_finish = m_on_finish;

  m_src_async_op->finish_op();
  delete m_src_async_op;
  delete this;

  on_finish->complete(r);
}

template <typename I>
void ObjectCopyRequest<I>::compute_dst_object_may_exist() {
  std::shared_lock image_locker{m_dst_image_ctx->image_lock};

  auto snap_ids = m_dst_image_ctx->snaps;
  snap_ids.push_back(CEPH_NOSNAP);

  for (auto snap_id : snap_ids) {
    m_dst_object_may_exist[snap_id] =
      (m_dst_object_number < m_dst_image_ctx->get_object_count(snap_id));
  }

  ldout(m_cct, 20) << "dst_object_may_exist=" << m_dst_object_may_exist
                   << dendl;
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;
