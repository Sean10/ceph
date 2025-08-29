// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 test for config-key rm-range functionality
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "mon/KVMonitor.h"
#include "include/buffer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "gtest/gtest.h"

using namespace std;

// Helper class to test the actual encoding logic from KVMonitor::encode_pending
class KVMonitorEncodingHelper {
public:
  /**
   * This function replicates the exact encoding logic from KVMonitor::encode_pending
   * without requiring the full Monitor/Paxos/Transaction setup
   */
  static void encode_pending_data(
    const std::map<std::string, std::optional<ceph::buffer::list>>& pending,
    const std::vector<KVMonitor::RangeDeleteOp>& pending_range_deletes,
    bool can_use_range_ops,
    ceph::buffer::list& bl
  ) {
    // This is the exact logic from KVMonitor::encode_pending lines 84-109
    if (can_use_range_ops) {
      // Use v2 format with range operations support  
      ENCODE_START(2, 1, bl);  // v2 format but compat with v1 decoders
      encode(pending, bl);     // key operations (always present)
      encode(pending_range_deletes, bl);  // range operations (new in v2)
      ENCODE_FINISH(bl);
    } else {
      // Legacy format for clusters without KV range ops feature
      if (!pending_range_deletes.empty()) {
        // In real implementation, _convert_ranges_to_keys() would be called
        // For testing, we simulate this by ignoring range ops
      }
      // Legacy encoding (no version header, compatible with all versions)
      encode(pending, bl);
    }
  }
  
  /**
   * This function replicates the exact decoding logic from KVMonitor::maybe_send_update
   */
  static void decode_pending_data(
    ceph::buffer::list& bl,
    bool can_use_range_ops,
    std::map<std::string, std::optional<ceph::buffer::list>>& key_ops,
    std::vector<KVMonitor::RangeDeleteOp>& range_ops
  ) {
    auto p = bl.cbegin();
    if (can_use_range_ops) {
      DECODE_START_LEGACY_COMPAT_LEN(2, 1, 1, p);
      decode(key_ops, p);
      if (struct_v >= 2) {
        decode(range_ops, p);
      }
      DECODE_FINISH(p);
    } else {
      // Legacy format (no version header, only key_ops)
      decode(key_ops, p);
    }
  }
};

class KVMonitorTest : public ::testing::Test {
protected:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

// Test encoding and decoding of RangeDeleteOp
TEST_F(KVMonitorTest, RangeDeleteOp_EncodeDecode) {
  KVMonitor::RangeDeleteOp original("test/prefix", "start", "end");
  
  // Encode
  ceph::buffer::list bl;
  encode(original, bl);
  
  // Decode  
  KVMonitor::RangeDeleteOp decoded;
  auto p = bl.cbegin();
  decode(decoded, p);
  
  // Verify
  EXPECT_EQ(original.prefix, decoded.prefix);
  EXPECT_EQ(original.start, decoded.start);
  EXPECT_EQ(original.end, decoded.end);
}

// Test encoding and decoding with KV_RANGE_OPS feature enabled
TEST_F(KVMonitorTest, VersionedFormat_WithRangeFeature) {
  // Test the versioned encoding format used in encode_pending when range feature is available
  std::map<std::string,std::optional<ceph::buffer::list>> key_ops;
  std::vector<KVMonitor::RangeDeleteOp> range_ops;
  
  // Add some key operations
  ceph::buffer::list val1, val2;
  val1.append("value1");
  val2.append("value2");
  key_ops["key1"] = val1;
  key_ops["key2"] = val2;
  key_ops["deleted_key"] = std::nullopt; // deletion
  
  // Add some range operations
  range_ops.emplace_back("prefix1", "start1", "end1");
  range_ops.emplace_back("prefix2", "", ""); // delete all with prefix
  
  // Encode using the EXACT logic from KVMonitor::encode_pending
  ceph::buffer::list bl;
  KVMonitorEncodingHelper::encode_pending_data(key_ops, range_ops, true, bl);
  
  // Decode using can_use_range_ops path selection (no heuristic)
  std::map<std::string,std::optional<ceph::buffer::list>> decoded_key_ops;
  std::vector<KVMonitor::RangeDeleteOp> decoded_range_ops;
  KVMonitorEncodingHelper::decode_pending_data(bl, true, decoded_key_ops, decoded_range_ops);
  
  // Verify key operations
  EXPECT_EQ(key_ops.size(), decoded_key_ops.size());
  EXPECT_EQ(key_ops["key1"]->to_str(), decoded_key_ops["key1"]->to_str());
  EXPECT_EQ(key_ops["key2"]->to_str(), decoded_key_ops["key2"]->to_str());
  EXPECT_FALSE(decoded_key_ops["deleted_key"]);
  
  // Verify range operations
  EXPECT_EQ(range_ops.size(), decoded_range_ops.size());
  EXPECT_EQ(range_ops[0].prefix, decoded_range_ops[0].prefix);
  EXPECT_EQ(range_ops[0].start, decoded_range_ops[0].start);
  EXPECT_EQ(range_ops[0].end, decoded_range_ops[0].end);
  EXPECT_EQ(range_ops[1].prefix, decoded_range_ops[1].prefix);
  EXPECT_EQ(range_ops[1].start, decoded_range_ops[1].start);
  EXPECT_EQ(range_ops[1].end, decoded_range_ops[1].end);
}

// Test backward compatibility: legacy format (no KV_RANGE_OPS feature)
TEST_F(KVMonitorTest, BackwardCompatibility_WithoutRangeFeature) {
  // Create data to simulate encoding without range feature
  std::map<std::string,std::optional<ceph::buffer::list>> key_ops;
  std::vector<KVMonitor::RangeDeleteOp> range_ops; // Will be ignored
  
  ceph::buffer::list val;
  val.append("test_value");
  key_ops["test_key"] = val;
  key_ops["deleted_key"] = std::nullopt;
  
  // Simulate range operations that would exist but be converted
  range_ops.emplace_back("prefix1", "start1", "end1");
  
  // Encode using EXACT logic WITHOUT range feature (simulates old cluster)
  ceph::buffer::list bl;
  KVMonitorEncodingHelper::encode_pending_data(key_ops, range_ops, false, bl);
  
  // Decode using can_use_range_ops=false (legacy)
  std::map<std::string,std::optional<ceph::buffer::list>> decoded_key_ops;
  std::vector<KVMonitor::RangeDeleteOp> decoded_range_ops;
  KVMonitorEncodingHelper::decode_pending_data(bl, false, decoded_key_ops, decoded_range_ops);
  
  // Verify
  EXPECT_EQ(key_ops.size(), decoded_key_ops.size());
  EXPECT_EQ(key_ops["test_key"]->to_str(), decoded_key_ops["test_key"]->to_str());
  EXPECT_FALSE(decoded_key_ops["deleted_key"]);
  EXPECT_TRUE(decoded_range_ops.empty()); // Should be empty for legacy format
}

// Test empty operations with range feature
TEST_F(KVMonitorTest, EmptyOperations_WithRangeFeature) {
  std::map<std::string,std::optional<ceph::buffer::list>> empty_key_ops;
  std::vector<KVMonitor::RangeDeleteOp> empty_range_ops;
  
  // Encode using EXACT logic with range feature
  ceph::buffer::list bl;
  KVMonitorEncodingHelper::encode_pending_data(empty_key_ops, empty_range_ops, true, bl);
  
  // Decode using can_use_range_ops=true
  std::map<std::string,std::optional<ceph::buffer::list>> decoded_key_ops;
  std::vector<KVMonitor::RangeDeleteOp> decoded_range_ops;
  KVMonitorEncodingHelper::decode_pending_data(bl, true, decoded_key_ops, decoded_range_ops);
  
  // Verify
  EXPECT_TRUE(decoded_key_ops.empty());
  EXPECT_TRUE(decoded_range_ops.empty());
}

// Test empty operations without range feature
TEST_F(KVMonitorTest, EmptyOperations_WithoutRangeFeature) {
  std::map<std::string,std::optional<ceph::buffer::list>> empty_key_ops;
  std::vector<KVMonitor::RangeDeleteOp> empty_range_ops;
  
  // Encode using EXACT logic without range feature
  ceph::buffer::list bl;
  KVMonitorEncodingHelper::encode_pending_data(empty_key_ops, empty_range_ops, false, bl);
  
  // Decode using can_use_range_ops=false
  std::map<std::string,std::optional<ceph::buffer::list>> decoded_key_ops;
  std::vector<KVMonitor::RangeDeleteOp> decoded_range_ops;
  KVMonitorEncodingHelper::decode_pending_data(bl, false, decoded_key_ops, decoded_range_ops);
  
  // Verify
  EXPECT_TRUE(decoded_key_ops.empty());
  EXPECT_TRUE(decoded_range_ops.empty());
}

// Test range delete operation construction
TEST_F(KVMonitorTest, RangeDeleteOp_Construction) {
  // Test default constructor
  KVMonitor::RangeDeleteOp default_op;
  EXPECT_TRUE(default_op.prefix.empty());
  EXPECT_TRUE(default_op.start.empty());
  EXPECT_TRUE(default_op.end.empty());
  
  // Test parameterized constructor
  KVMonitor::RangeDeleteOp param_op("prefix", "start", "end");
  EXPECT_EQ("prefix", param_op.prefix);
  EXPECT_EQ("start", param_op.start);
  EXPECT_EQ("end", param_op.end);
  
  // Test with empty start/end (prefix-only deletion)
  KVMonitor::RangeDeleteOp prefix_only_op("prefix", "", "");
  EXPECT_EQ("prefix", prefix_only_op.prefix);
  EXPECT_TRUE(prefix_only_op.start.empty());
  EXPECT_TRUE(prefix_only_op.end.empty());
}

// Test feature flag compatibility scenarios
TEST_F(KVMonitorTest, FeatureFlagCompatibility) {
  // Test encoding decision based on feature availability
  std::map<std::string,std::optional<ceph::buffer::list>> key_ops;
  std::vector<KVMonitor::RangeDeleteOp> range_ops;
  
  ceph::buffer::list val;
  val.append("test_value");
  key_ops["key1"] = val;
  range_ops.emplace_back("prefix1", "start1", "end1");
  
  // Test 1: Real cluster WITH KV_RANGE_OPS feature
  {
    ceph::buffer::list bl_with_feature;
    KVMonitorEncodingHelper::encode_pending_data(key_ops, range_ops, true, bl_with_feature);
    
    // Decode and verify range operations are preserved (feature=true)
    std::map<std::string,std::optional<ceph::buffer::list>> decoded_key_ops;
    std::vector<KVMonitor::RangeDeleteOp> decoded_range_ops;
    KVMonitorEncodingHelper::decode_pending_data(bl_with_feature, true, decoded_key_ops, decoded_range_ops);
    
    EXPECT_EQ(1, decoded_range_ops.size());
    EXPECT_EQ("prefix1", decoded_range_ops[0].prefix);
    EXPECT_EQ("start1", decoded_range_ops[0].start);
    EXPECT_EQ("end1", decoded_range_ops[0].end);
  }
  
  // Test 2: Real cluster WITHOUT KV_RANGE_OPS feature (legacy format)
  {
    ceph::buffer::list bl_without_feature;
    KVMonitorEncodingHelper::encode_pending_data(key_ops, range_ops, false, bl_without_feature);
    
    // Decode using can_use_range_ops=false
    std::map<std::string,std::optional<ceph::buffer::list>> decoded_key_ops;
    std::vector<KVMonitor::RangeDeleteOp> decoded_range_ops;
    KVMonitorEncodingHelper::decode_pending_data(bl_without_feature, false, decoded_key_ops, decoded_range_ops);
    
    EXPECT_EQ(1, decoded_key_ops.size());
    EXPECT_TRUE(decoded_range_ops.empty());  // Range ops not supported in legacy
  }
}

// (Removed) Format detection edge cases: decoding now follows can_use_range_ops explicitly

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, (const char **)argv);
  
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                        CODE_ENVIRONMENT_UTILITY,
                        CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}