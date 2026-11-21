import Std

/-!
Model of the indexed Raft data commit in store/lsm_store.go and
store/raft_entry.go, with the default synchronous Pebble write options.

An entry represents the whole batch, including the sequence of data writes
and its largest advertised commit timestamp. Pebble publishes a batch to
its readers atomically, may do so before sync completes, and reports success
only after sync. Before sync is observed, recovery may retain or drop the
whole batch. A fatal WAL error is modeled by the same recovery alternatives.
Ordinary errors before the Pebble batch takes effect leave the prior image.

From staging the metadata through returning from the helper, applyMu
excludes other watermark writers and the shared dbMu prevents a DB swap.
For the batched path this interval starts after the DB identity check.
The caller supplies an already committed Raft entry with a positive
index at least as large as the existing applied index. The surrounding
protocol handles stale-entry checks, replay, snapshot replacement, and
mutation admission. The recovery step below reopens the same WAL, including its
surviving images. Natural numbers model the comparisons and max used here.
-/
namespace RaftWALMetadata

structure Entry (Mutation : Type) where
  index : Nat
  advertisedTS : Nat
  mutations : List Mutation

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
  | read : Step s (.read s.lastCommitTS) s

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

theorem metadata_read_during_sync (prior : Image Mutation) (entry : Entry Mutation) :
    Step (.visible prior entry) (.read prior.watermark) (.visible prior entry) :=
  Step.read

-- Even if the Pebble batch is already visible, an old timestamp selects the
-- same versions when every new version was fenced above that timestamp.
def versionsAtOrBefore (stamp : Mutation → Nat) (ts : Nat) (image : Image Mutation) :
    List Mutation :=
  (image.entries.flatMap Entry.mutations).filter (fun m => stamp m ≤ ts)

theorem fenced_snapshot_unchanged_during_sync (prior : Image Mutation) (entry : Entry Mutation)
    (stamp : Mutation → Nat) (fresh : ∀ m ∈ entry.mutations, prior.watermark < stamp m) :
    versionsAtOrBefore stamp prior.watermark (State.visible prior entry).readable =
      versionsAtOrBefore stamp prior.watermark prior := by
  have empty : (entry.mutations.filter (fun m => stamp m ≤ prior.watermark)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro m member
    simp [Nat.not_le.mpr (fresh m member)]
  simp [versionsAtOrBefore, State.readable, Image.install, List.filter_append, empty]

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

-- Eligibility in commitMutationBatch and raftEntryScope.isIndexedRaftDataOnly.
def singleFastPath (index : Nat) (advance : Bool) : Bool :=
  (index != 0) && advance

def scopedFastPath (index : Nat) (advance forceSync : Bool) (readinessCount : Nat) : Bool :=
  singleFastPath index advance && !forceSync && (readinessCount == 0)

theorem scoped_path_requires_data_only {index : Nat} {advance forceSync : Bool}
    {readinessCount : Nat} (h : scopedFastPath index advance forceSync readinessCount = true) :
    index ≠ 0 ∧ advance = true ∧ forceSync = false ∧ readinessCount = 0 := by
  simp [scopedFastPath, singleFastPath] at h
  exact ⟨h.1.1.1, h.1.1.2, h.1.2, h.2⟩

end RaftWALMetadata
