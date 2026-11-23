theorem ttl_refines (n unit : Int) :
    ttl n unit ↔ AdapterProtocols.ttlFits n unit = true := by
  simp only [AdapterProtocols.ttlFits, decide_eq_true_eq, ttl]
  by_cases pos : 0 < unit
  · simp [Int.le_ediv_iff_mul_le pos, and_assoc]
  · simp [pos]

theorem budget_refines (start now sec : Int) (issued : 0 < start)
    (range : 0 ≤ sec ∧ sec ≤ 43200) :
    budget start now sec ↔ sec = 0 ∨ now + sec * 1000 ≤ start + 43200000 := by
  unfold budget
  omega

-- Include the entire transaction literal, not just a search for one field.
example : luaFields = ["IsTxn: true", "StartTS: luaCommitFloor(c.startTS)", "CommitTS: commitTS",
    "ReadKeys: readKeys", "Elems: elems"] := by decide
example : multipartFields = ["IsTxn: true", "StartTS: startTS", "CommitTS: commitTS",
    "Elem Op: kv.Put, Key: s3keys.BucketMetaKey(completion.bucket), Value: bucketFence",
    "Elem Op: kv.Put, Key: headKey, Value: manifestBody",
    "Elem Op: kv.Del, Key: completion.uploadMetaKey",
    "Elem Op: kv.Del, Key: s3keys.GCUploadKey(completion.bucket, completion.generation, completion.objectKey, completion.uploadID)",
    "ReadKeys: completion.partKeys"] := by decide

example : abortFields = ["IsTxn: true", "StartTS: startTS", "Elem Op: kv.Del, Key: uploadMetaKey",
    "Elem Op: kv.Del, Key: s3keys.GCUploadKey(bucket, meta.Generation, objectKey, uploadID)"] := by decide

example : beginFields = ["IsTxn: true", "StartTS: startTS", "CommitTS: commitTS",
    "Elem Op: kv.Put, Key: s3keys.BucketMetaKey(bucket), Value: bucketFence",
    "Elem Op: kv.Put, Key: s3keys.UploadMetaKey(bucket, meta.Generation, objectKey, uploadID), Value: body",
    "Elem Op: kv.Put, Key: s3keys.GCUploadKey(bucket, meta.Generation, objectKey, uploadID), Value: body"] := by decide

example : partFields = ["IsTxn: true", "StartTS: version.startTS", "CommitTS: version.commitTS", "Elems: elems",
    "ReadKeys: [][]byte{state.uploadMetaKey, s3keys.BucketGenerationKey(bucket), partKey}"] := by decide

example : visibilityFields = ["IsTxn: true", "StartTS: readTS",
    "ReadKeys: [][]byte{dataKey, oldVisKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}",
    "Elem Op: kv.Del, Key: oldVisKey", "Elem Op: kv.Put, Key: newVisKey, Value: []byte(rec.MessageID)",
    "Elem Op: kv.Put, Key: dataKey, Value: recordBytes"] := by decide

end ProtocolGo
