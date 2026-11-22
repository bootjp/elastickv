--------------------------- MODULE MCSplitMembership ---------------------------
EXTENDS Naturals

CONSTANT Mode
ASSUME Mode \in {"safe", "early", "stale"}

(* A source ACK includes an overwrite and a delete. Prior replay proof
   supplies the sealed target image. Readiness is a prefix of the target log. *)
Sealed == [kept |-> "value", deleted |-> "absent"]
BeforeAck == [kept |-> "old", deleted |-> "old"]
Empty == [kept |-> "absent", deleted |-> "absent"]
NoRead == [endpoint |-> "none", result |-> "unavailable", image |-> Empty]

VARIABLES epoch, confirmed, role, appliedReady, owner, cachedOwner,
          sourceCleaned, newLeader, observation
vars == <<epoch, confirmed, role, appliedReady, owner, cachedOwner,
          sourceCleaned, newLeader, observation>>

Init ==
  /\ epoch = 1 /\ confirmed = 0
  /\ role = "absent" /\ appliedReady = FALSE
  /\ owner = "source" /\ cachedOwner = "source"
  /\ sourceCleaned = FALSE /\ newLeader = FALSE /\ observation = NoRead

Confirm ==
  /\ role = "absent" /\ confirmed' = epoch
  /\ UNCHANGED <<epoch, role, appliedReady, owner, cachedOwner,
                 sourceCleaned, newLeader, observation>>

Add ==
  /\ confirmed = 1 /\ role = "absent"
  /\ role' = "learner" /\ epoch' = epoch + 1
  /\ UNCHANGED <<confirmed, appliedReady, owner, cachedOwner,
                 sourceCleaned, newLeader, observation>>

ApplyReady ==
  /\ role \in {"learner", "voter"} /\ ~appliedReady
  /\ appliedReady' = TRUE
  /\ UNCHANGED <<epoch, confirmed, role, owner, cachedOwner,
                 sourceCleaned, newLeader, observation>>

Promote ==
  /\ role = "learner"
  /\ Mode = "early" \/ appliedReady
  /\ role' = "voter" /\ epoch' = epoch + 1
  /\ UNCHANGED <<confirmed, appliedReady, owner, cachedOwner,
                 sourceCleaned, newLeader, observation>>

(* The checked implementation publishes from the earlier confirmation.
   New membership inherits the durable Ready prefix through Raft apply. *)
Publish ==
  /\ role = "voter" /\ owner = "source" /\ confirmed = 1
  /\ owner' = "target"
  /\ UNCHANGED <<epoch, confirmed, role, appliedReady, cachedOwner,
                 sourceCleaned, newLeader, observation>>

Cleanup ==
  /\ owner = "target" /\ ~sourceCleaned /\ sourceCleaned' = TRUE
  /\ UNCHANGED <<epoch, confirmed, role, appliedReady, owner, cachedOwner,
                 newLeader, observation>>

HealIngress ==
  /\ owner = "target" /\ cachedOwner = "source" /\ cachedOwner' = owner
  /\ UNCHANGED <<epoch, confirmed, role, appliedReady, owner,
                 sourceCleaned, newLeader, observation>>

Transfer ==
  /\ owner = "target" /\ role = "voter" /\ ~newLeader /\ newLeader' = TRUE
  /\ UNCHANGED <<epoch, confirmed, role, appliedReady, owner, cachedOwner,
                 sourceCleaned, observation>>

ReadNewMember ==
  /\ owner = "target" /\ role = "voter"
  /\ observation' =
     IF ~newLeader /\ cachedOwner = "source"
     THEN [endpoint |-> "new", result |-> "unavailable", image |-> Empty]
     ELSE [endpoint |-> IF newLeader THEN "leader" ELSE "new",
           result |-> "ok", image |-> IF appliedReady THEN Sealed ELSE Empty]
  /\ UNCHANGED <<epoch, confirmed, role, appliedReady, owner, cachedOwner,
                 sourceCleaned, newLeader>>

ReadOldIngress ==
  /\ owner = "target"
  /\ observation' =
     IF cachedOwner = "target"
     THEN [endpoint |-> "old", result |-> "ok", image |-> Sealed]
     ELSE IF Mode = "stale"
          THEN [endpoint |-> "old", result |-> "ok",
                image |-> IF sourceCleaned THEN Empty ELSE BeforeAck]
          ELSE [endpoint |-> "old", result |-> "unavailable", image |-> Empty]
  /\ UNCHANGED <<epoch, confirmed, role, appliedReady, owner, cachedOwner,
                 sourceCleaned, newLeader>>

Next == Confirm \/ Add \/ ApplyReady \/ Promote \/ Publish \/ Cleanup \/ HealIngress \/
        Transfer \/ ReadNewMember \/ ReadOldIngress
Spec == Init /\ [][Next]_vars

AcknowledgedOverwriteVisible ==
  observation.result = "ok" => observation.image.kept = Sealed.kept
DeletedValueNeverRevives ==
  observation.result = "ok" => observation.image.deleted = "absent"

=============================================================================
