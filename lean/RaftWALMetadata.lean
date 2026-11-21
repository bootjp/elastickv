import Std

/-!
Model of the indexed Raft data commit in store/lsm_store.go and
store/raft_entry.go, with the default synchronous Pebble write options.

An entry represents the whole batch, including each staged operation's
timestamp and the largest advertised commit timestamp. The staged list
includes operations with no new versions, as the Go minimum does. Pebble publishes a batch to
its readers atomically, may do so before sync completes, and reports success
only after sync. Before sync is observed, recovery may retain or drop the
whole batch. A fatal WAL error is modeled by the same recovery alternatives.
Ordinary errors before the Pebble batch takes effect leave the prior image.

From staging the metadata through returning from the helper, applyMu
excludes other watermark writers and the shared dbMu prevents a DB swap.
For the batched path this interval starts after the DB identity check.
The metadata lock can be released during sync only for indexed data batches
whose minimum staged timestamp exceeds the previously published watermark.
The model gates new metadata reads during apply. Retention and admission for
reads that already acquired a snapshot belong to the surrounding protocol.
The caller supplies an already committed Raft entry with a positive
index at least as large as the existing applied index. The surrounding
protocol handles stale-entry checks, replay, snapshot replacement, and
mutation admission. The recovery step below reopens the same WAL, including its
surviving images. Natural numbers model the comparisons and max used here.
-/
namespace RaftWALMetadata

structure StagedWrite (Mutation : Type) where
  timestamp : Nat
  mutations : List Mutation

structure Entry (Mutation : Type) where
  index : Nat
  advertisedTS : Nat
  staged : List (StagedWrite Mutation)
  advance : Bool
  forceSync : Bool
  readinessCount : Nat

def Entry.minStagedTS (entry : Entry Mutation) : Nat :=
  (entry.staged.map StagedWrite.timestamp).min?.getD 0

-- Eligibility in commitMutationBatch and raftEntryScope.canCommitWithoutMetadataLock.
def singleFastPath (index : Nat) (advance : Bool) (watermark timestamp : Nat) : Bool :=
  (index != 0) && advance && decide (watermark < timestamp)

def scopedFastPath (entry : Entry Mutation) (watermark : Nat) : Bool :=
  singleFastPath entry.index entry.advance watermark entry.minStagedTS &&
    !entry.forceSync && (entry.readinessCount == 0)

theorem scoped_path_requires_data_only (h : scopedFastPath entry watermark = true) :
    entry.index ≠ 0 ∧ entry.advance = true ∧ entry.forceSync = false ∧
      entry.readinessCount = 0 ∧ watermark < entry.minStagedTS := by
  simp [scopedFastPath, singleFastPath] at h
  exact ⟨h.1.1.1.1, h.1.1.1.2, h.1.2, h.2, h.1.1.2⟩

private theorem minimum_le_staged_timestamp {entry : Entry Mutation} {write : StagedWrite Mutation}
    (member : write ∈ entry.staged) : entry.minStagedTS ≤ write.timestamp := by
  have mapped : write.timestamp ∈ entry.staged.map StagedWrite.timestamp :=
    List.mem_map.mpr ⟨write, member, rfl⟩
  unfold Entry.minStagedTS
  cases h : (entry.staged.map StagedWrite.timestamp).min? with
  | none => exact Nat.zero_le _
  | some minimum => exact (List.min?_eq_some_iff.mp h).2 _ mapped

structure Image (Mutation : Type) where
  entries : List (Entry Mutation)
  appliedIndex : Nat
  watermark : Nat

def Image.install (prior : Image Mutation) (entry : Entry Mutation) : Image Mutation :=
  ⟨entry :: prior.entries, entry.index, max prior.watermark entry.advertisedTS⟩

inductive State (Mutation : Type)
  | idle (prior : Image Mutation)
  | staged (prior : Image Mutation) (entry : Entry Mutation)
  | visible (prior : Image Mutation) (entry : Entry Mutation)
  | synced (prior : Image Mutation) (entry : Entry Mutation)
  | published (prior : Image Mutation) (entry : Entry Mutation)

def State.durable : State Mutation → Image Mutation
  | .idle prior | .staged prior _ | .visible prior _ => prior
  | .synced prior entry | .published prior entry => prior.install entry

def State.readable : State Mutation → Image Mutation
  | .idle prior | .staged prior _ => prior
  | .visible prior entry | .synced prior entry | .published prior entry => prior.install entry

def State.lastCommitTS : State Mutation → Nat
  | .idle prior | .staged prior _ | .visible prior _ | .synced prior _ => prior.watermark
  | .published prior entry => (prior.install entry).watermark

def State.ordered : State Mutation → Prop
  | .idle _ => True
  | .staged prior entry | .visible prior entry | .synced prior entry
  | .published prior entry => prior.appliedIndex ≤ entry.index

def State.canReadMetadata : State Mutation → Prop
  | .idle _ | .published _ _ => True
  | .staged prior entry | .visible prior entry | .synced prior entry =>
      scopedFastPath entry prior.watermark = true

inductive Event (Mutation : Type)
  | begin (entry : Entry Mutation)
  | visible
  | sync
  | publish
  | ack (entry : Entry Mutation)
  | abort
  | recover
  | read (observedTS : Nat)

inductive Step : State Mutation → Event Mutation → State Mutation → Prop
  | begin (ordered : prior.appliedIndex ≤ entry.index) (indexed : entry.index ≠ 0) :
      Step (.idle prior) (.begin entry) (.staged prior entry)
  | expose : Step (.staged prior entry) .visible (.visible prior entry)
  | sync : Step (.visible prior entry) .sync (.synced prior entry)
  | publish : Step (.synced prior entry) .publish (.published prior entry)
  | ack : Step (.published prior entry) (.ack entry) (.idle (prior.install entry))
  | abort : Step (.staged prior entry) .abort (.idle prior)
  | reopenIdle : Step (.idle prior) .recover (.idle prior)
  | reopenStagedDrop : Step (.staged prior entry) .recover (.idle prior)
  | reopenStagedKeep : Step (.staged prior entry) .recover (.idle (prior.install entry))
  | reopenVisibleDrop : Step (.visible prior entry) .recover (.idle prior)
  | reopenVisibleKeep : Step (.visible prior entry) .recover (.idle (prior.install entry))
  | reopenSynced : Step (.synced prior entry) .recover (.idle (prior.install entry))
  | reopenPublished : Step (.published prior entry) .recover (.idle (prior.install entry))
  | read (allowed : s.canReadMetadata) : Step s (.read s.lastCommitTS) s

theorem published_watermark_is_durable (s : State Mutation) :
    s.lastCommitTS ≤ s.durable.watermark := by
  cases s <;> simp [State.lastCommitTS, State.durable, Image.install, Nat.le_max_left]

theorem metadata_read_is_durable (h : Step s (Event.read ts) next) :
    ts ≤ s.durable.watermark ∧ next = s := by
  cases h
  exact ⟨published_watermark_is_durable _, rfl⟩

-- A visible Pebble index may lead the published watermark. Each image
-- still contains the matching batch, stored watermark, and applied index.
theorem visibility_and_sync_boundaries (prior : Image Mutation) (entry : Entry Mutation) :
    (State.visible prior entry).readable = prior.install entry ∧
    (State.visible prior entry).durable = prior ∧
    (State.visible prior entry).lastCommitTS = prior.watermark ∧
    (State.synced prior entry).durable = prior.install entry ∧
    (State.synced prior entry).lastCommitTS = prior.watermark := by
  exact ⟨rfl, rfl, rfl, rfl, rfl⟩

theorem next_writer_requires_idle (h : Step s (Event.begin entry) next) :
    ∃ prior, s = State.idle prior ∧ entry.index ≠ 0 ∧ prior.appliedIndex ≤ entry.index := by
  cases h with
  | begin ordered indexed => exact ⟨_, rfl, indexed, ordered⟩

theorem metadata_read_during_sync (prior : Image Mutation) (entry : Entry Mutation)
    (eligible : scopedFastPath entry prior.watermark = true) :
    Step (.visible prior entry) (.read prior.watermark) (.visible prior entry) :=
  Step.read eligible

theorem visible_batch_at_or_below_watermark_blocks_metadata_read
    (member : write ∈ entry.staged) (old : write.timestamp ≤ prior.watermark) :
    ¬ Step (.visible prior entry) (.read prior.watermark) (.visible prior entry) := by
  intro read
  cases read with
  | read allowed =>
    have fresh := (scoped_path_requires_data_only allowed).2.2.2.2
    have bound := minimum_le_staged_timestamp member
    omega

def versionsAtOrBefore (ts : Nat) (image : Image Mutation) : List Mutation :=
  image.entries.flatMap (fun entry =>
    (entry.staged.filter (fun write => write.timestamp ≤ ts)).flatMap StagedWrite.mutations)

-- The production eligibility gate supplies freshness for every staged operation.
theorem snapshot_unchanged_for_overlapping_metadata_read (prior : Image Mutation)
    (entry : Entry Mutation)
    (read : Step (.visible prior entry) (.read prior.watermark) (.visible prior entry)) :
    versionsAtOrBefore prior.watermark (State.visible prior entry).readable =
      versionsAtOrBefore prior.watermark prior := by
  cases read with
  | read allowed =>
    have fresh := (scoped_path_requires_data_only allowed).2.2.2.2
    have empty : (entry.staged.filter (fun write => write.timestamp ≤ prior.watermark)) = [] := by
      rw [List.filter_eq_nil_iff]
      intro write member
      have := Nat.lt_of_lt_of_le fresh (minimum_le_staged_timestamp member)
      simp [Nat.not_le.mpr this]
    simp [versionsAtOrBefore, State.readable, Image.install, empty]

theorem recovery_selects_whole_image (h : Step (.visible prior entry) .recover next) :
    next = State.idle prior ∨ next = State.idle (prior.install entry) := by
  cases h
  · exact Or.inl rfl
  · exact Or.inr rfl

theorem ack_requires_synced_publication (h : Step s (Event.ack entry) next) :
    ∃ prior, s = State.published prior entry ∧
      next.durable = prior.install entry ∧ entry ∈ next.durable.entries := by
  cases h
  exact ⟨_, rfl, rfl, List.Mem.head _⟩

theorem step_preserves_order (h : Step s event next) (ordered : s.ordered) :
    next.ordered := by
  cases h <;> simp_all [State.ordered]

theorem step_preserves_durable (h : Step s event next) :
    s.durable.entries ⊆ next.durable.entries ∧ s.durable.watermark ≤ next.durable.watermark := by
  cases h <;> simp [State.durable, Image.install, List.subset_def, Nat.le_max_left]
  all_goals exact fun member => Or.inr member

theorem step_index_monotone (h : Step s event next) (ordered : s.ordered) :
    s.durable.appliedIndex ≤ next.durable.appliedIndex := by
  cases h <;> simp_all [State.durable, State.ordered, Image.install]

inductive Run : State Mutation → List (Event Mutation) → State Mutation → Prop
  | nil : Run s [] s
  | cons : Step s event next → Run next events final → Run s (event :: events) final

theorem durable_boundaries_monotone (h : Run s events final) (ordered : s.ordered) :
    s.durable.appliedIndex ≤ final.durable.appliedIndex ∧
    s.durable.watermark ≤ final.durable.watermark := by
  induction h with
  | nil => exact ⟨Nat.le_refl _, Nat.le_refl _⟩
  | cons step _ ih =>
    obtain ⟨index, watermark⟩ := ih (step_preserves_order step ordered)
    exact ⟨Nat.le_trans (step_index_monotone step ordered) index,
      Nat.le_trans (step_preserves_durable step).2 watermark⟩

theorem run_preserves_durable_entries (h : Run s events final) :
    s.durable.entries ⊆ final.durable.entries := by
  induction h with
  | nil => exact fun _ member => member
  | cons step _ ih => exact fun _ member => ih ((step_preserves_durable step).1 member)

theorem acknowledged_batch_survives_recovery (h : Run s events final)
    (acknowledged : Event.ack entry ∈ events) : entry ∈ final.durable.entries := by
  induction h with
  | nil => simp at acknowledged
  | cons step tail ih =>
    rcases List.mem_cons.mp acknowledged with same | later
    · cases same
      obtain ⟨_, _, _, member⟩ := ack_requires_synced_publication step
      exact run_preserves_durable_entries tail member
    · exact ih later

end RaftWALMetadata
