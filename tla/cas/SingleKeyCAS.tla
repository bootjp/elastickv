---------------------------- MODULE SingleKeyCAS ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS UseWatcher, UnprotectedCleanup

Nodes == {1, 2}

Row(version, value) == [version |-> version, value |-> value]
Catalog(version, owns, fenced) ==
    [version |-> version, owns |-> owns, fenced |-> fenced]

Activate == [kind |-> "activate"]
Split == [kind |-> "catalog", route |-> Catalog(2, TRUE, FALSE)]
Fence == [kind |-> "catalog", route |-> Catalog(3, TRUE, TRUE)]
Move == [kind |-> "catalog", route |-> Catalog(4, FALSE, FALSE)]
Arm(armed) == [kind |-> "arm", armed |-> armed]
Certify == [kind |-> "certify"]
Put(version, value) == [kind |-> "put", row |-> Row(version, value)]
CAS(observed, expected, version, value) ==
    [kind |-> "cas", observed |-> observed, expected |-> expected,
     row |-> Row(version, value)]
Cleanup(cursor, ceiling, limit) ==
    [kind |-> "cleanup", cursor |-> cursor, ceiling |-> ceiling, limit |-> limit]

Workload(authority, entries, verdicts) ==
    [authority |-> authority, entries |-> entries, verdicts |-> verdicts]

\* Each log is shared. Commit, watcher progress, replay, leader change and
\* each replica's apply order interleave independently within each workload.
Workloads == {
    Workload(TRUE,
      <<Activate, Split, CAS(2, 1, 2, 7), Cleanup(0, 2, 1),
        CAS(2, 1, 3, 8), CAS(2, 2, 3, 9)>>,
      <<"control", "control", "applied", "cleanupBlocked", "mismatch", "applied">>),
    Workload(TRUE,
      <<Activate, Split, CAS(2, 1, 2, 7), Fence, CAS(2, 2, 3, 8),
        Move, CAS(2, 2, 3, 9)>>,
      <<"control", "control", "applied", "control", "routeFenced",
        "control", "routeFenced">>),
    Workload(TRUE,
      <<Activate, CAS(1, 1, 2, 7), Arm(TRUE), Cleanup(0, 2, 1),
        Certify, Cleanup(0, 2, 1), Arm(FALSE), CAS(1, 1, 3, 8)>>,
      <<"control", "applied", "control", "cleanupBlocked", "control",
        "cleaned", "control", "unavailable">>),
    Workload(TRUE,
      <<Put(2, 0), Put(3, 0), Cleanup(1, 2, 1), Activate, CAS(1, 1, 4, 7),
        CAS(1, 3, 3, 8), CAS(1, 3, 4, 9)>>,
      <<"written", "written", "cleaned", "control", "mismatch", "timestamp", "applied">>),
    Workload(FALSE,
      <<Activate, CAS(1, 1, 2, 7)>>,
      <<"control", "halt">>),
    Workload(TRUE,
      <<Activate, CAS(2, 1, 2, 7)>>,
      <<"control", "halt">>)
}

VARIABLES workload, committed, replicas, leader
vars == <<workload, committed, replicas, leader>>

Image(authority) ==
    [route |-> IF authority THEN Catalog(1, TRUE, FALSE) ELSE Catalog(0, FALSE, FALSE),
     protection |-> FALSE, armed |-> FALSE, covered |-> FALSE,
     rows |-> <<Row(1, 0)>>, issued |-> 1, consumed |-> <<>>, verdicts |-> <<>>]

InitialReplicas ==
    /\ committed = 0
    /\ leader = 1
    /\ replicas = [n \in Nodes |-> [index |-> 0, watcher |-> 1, replayed |-> FALSE,
                                   image |-> Image(workload.authority)]]

Init == workload \in Workloads /\ InitialReplicas

HeadVersion(s) == IF s.rows = <<>> THEN 0 ELSE s.rows[1].version
Record(s, verdict) == [s EXCEPT !.verdicts = Append(@, verdict)]
Write(s, row) == [s EXCEPT !.rows = <<row>> \o @, !.issued = row.version]

CleanRows(rows, e) ==
    LET eligible == SelectSeq(SubSeq(rows, e.cursor + 1, Len(rows)),
                              LAMBDA row: row.version <= e.ceiling)
        selected == SubSeq(eligible, 1, IF e.limit < Len(eligible) THEN e.limit ELSE Len(eligible))
    IN SelectSeq(rows, LAMBDA row: \A i \in DOMAIN selected: selected[i] # row)

ApplyCAS(s, watcher, e) ==
    IF ~s.protection \/ s.route.version = 0 THEN Record(s, "unavailable")
    ELSE IF ~s.route.owns \/ s.route.fenced THEN Record(s, "routeFenced")
    ELSE IF s.armed \/ s.covered THEN Record(s, "unavailable")
    ELSE IF UseWatcher /\ watcher < e.observed THEN Record(s, "missingCatalog")
    ELSE IF e.row.version <= s.issued THEN Record(s, "timestamp")
    ELSE IF e.expected = 0 \/ e.expected # HeadVersion(s) THEN Record(s, "mismatch")
    ELSE Record([Write(s, e.row) EXCEPT !.consumed = Append(@, e.expected)], "applied")

ApplyEntry(s, watcher, e) ==
    CASE e.kind = "activate" -> Record([s EXCEPT !.protection = TRUE], "control")
      [] e.kind = "catalog" ->
         Record(IF s.route.version = 0 THEN s ELSE [s EXCEPT !.route = e.route], "control")
      [] e.kind = "arm" -> Record([s EXCEPT !.armed = e.armed], "control")
      [] e.kind = "certify" -> Record([s EXCEPT !.covered = TRUE], "control")
      [] e.kind = "put" ->
         IF e.row.version <= s.issued THEN Record(s, "timestamp")
         ELSE Record(Write(s, e.row), "written")
      [] e.kind = "cas" -> ApplyCAS(s, watcher, e)
      [] e.kind = "cleanup" ->
         IF s.protection /\ ~UnprotectedCleanup /\ ~s.covered THEN Record(s, "cleanupBlocked")
         ELSE Record([s EXCEPT !.rows = CleanRows(@, e)], "cleaned")

CanApply(s, e) ==
    IF e.kind = "cas"
    THEN s.protection /\ s.route.version # 0 /\ e.observed <= s.route.version
    ELSE TRUE

Commit(n) ==
    /\ n = leader
    /\ committed < Len(workload.entries)
    /\ committed' = committed + 1
    /\ UNCHANGED <<workload, replicas, leader>>

CatalogPublished ==
    1 + Cardinality({i \in 1..committed: workload.entries[i].kind = "catalog"})

Watch(n) ==
    /\ replicas[n].watcher < CatalogPublished
    /\ replicas' = [replicas EXCEPT ![n].watcher = @ + 1]
    /\ UNCHANGED <<workload, committed, leader>>

Apply(n) ==
    LET r == replicas[n]
        i == r.index + 1
    IN /\ i <= committed
       /\ CanApply(r.image, workload.entries[i])
       /\ replicas' = [replicas EXCEPT ![n].index = i,
                         ![n].image = ApplyEntry(r.image, r.watcher, workload.entries[i])]
       /\ UNCHANGED <<workload, committed, leader>>

Elect ==
    \* Both voters retain access to the committed log after election.
    /\ leader = 1
    /\ leader' = 2
    /\ UNCHANGED <<workload, committed, replicas>>

Replay(n) ==
    \* A restarted/re-elected voter can encounter any already durable entry.
    \* Only replayed changes, to make this otherwise stuttering step visible.
    /\ leader = 2
    /\ replicas[n].index > 0
    /\ ~replicas[n].replayed
    /\ replicas' = [replicas EXCEPT ![n].replayed = TRUE]
    /\ UNCHANGED <<workload, committed, leader>>

Next == Elect \/ \E n \in Nodes: Commit(n) \/ Watch(n) \/ Apply(n) \/ Replay(n)
Spec == Init /\ [][Next]_vars


\* Independent compaction preserves the visible head and changes no log index.
\* Restrict it to the point after coverage is installed and before cleanup.
CompactCovered(n) ==
    /\ replicas[n].index = 3
    /\ Len(replicas[n].image.rows) > 1
    /\ replicas' = [replicas EXCEPT ![n].image.rows = SubSeq(@, 1, 1)]
    /\ UNCHANGED <<workload, committed, leader>>

RetentionInit ==
    /\ workload = Workload(TRUE, <<Activate, Put(2, 7), Certify, Cleanup(0, 2, 1)>>,
                           <<"control", "written", "control", "cleaned">>)
    /\ InitialReplicas

RetentionSpec == RetentionInit /\ [][Next \/ (\E n \in Nodes: CompactCovered(n))]_vars

VisibleHead(s) == IF s.rows = <<>> THEN <<>> ELSE <<s.rows[1]>>

VisibleHeadAgrees ==
    \A a, b \in Nodes:
        replicas[a].index = replicas[b].index =>
            VisibleHead(replicas[a].image) = VisibleHead(replicas[b].image)

ProgressBounds ==
    /\ committed \in 0..Len(workload.entries)
    /\ \A n \in Nodes:
          /\ replicas[n].index \in 0..committed
          /\ replicas[n].watcher \in 1..CatalogPublished
          /\ Len(replicas[n].image.verdicts) = replicas[n].index

EqualAppliedIndexAgrees ==
    \A a, b \in Nodes:
      replicas[a].index = replicas[b].index => replicas[a].image = replicas[b].image

SingleConsumption ==
    \A n \in Nodes:
      LET tokens == replicas[n].image.consumed
      IN Len(tokens) = Cardinality({tokens[i]: i \in DOMAIN tokens})

NoTokenRevival ==
    \A n \in Nodes:
      LET s == replicas[n].image
      IN s.covered \/ \A i \in DOMAIN s.consumed: s.consumed[i] < HeadVersion(s)

VersionOrder ==
    \A n \in Nodes:
      LET s == replicas[n].image
      IN /\ \A i \in DOMAIN s.rows: s.rows[i].version \in 1..s.issued
         /\ \A i, j \in DOMAIN s.rows: i < j => s.rows[i].version > s.rows[j].version
         /\ \A i \in DOMAIN s.consumed: s.consumed[i] < s.issued

ProducedVersionsAdvance ==
    [][\A n \in Nodes:
        LET before == replicas[n].image
            after == replicas'[n].image
        IN /\ before.issued <= after.issued
           /\ Len(before.rows) < Len(after.rows) => before.issued < after.issued]_vars

ExpectedVerdicts ==
    \A n \in Nodes:
      replicas[n].image.verdicts = SubSeq(workload.verdicts, 1, replicas[n].index)

ExpectedReadiness ==
    \A n \in Nodes:
      LET r == replicas[n]
          i == r.index + 1
      IN i <= committed =>
         (CanApply(r.image, workload.entries[i]) <=> workload.verdicts[i] # "halt")

=============================================================================
