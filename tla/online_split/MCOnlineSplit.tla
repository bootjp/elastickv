--------------------------- MODULE MCOnlineSplit ---------------------------
EXTENDS Naturals, Sequences

CONSTANTS Cells, MaxFrames, Mode
ASSUME /\ Cells # {}
       /\ MaxFrames \in Nat
       /\ Mode \in {"safe", "gap", "tail", "config", "cleanup", "owner", "invalid"}

Values == {"absent", "seed", "value", "tombstone"}
Postimages == [cell : Cells, value : {"value", "tombstone"}]
Phases == {"capture", "invalid", "fenced", "sealed", "ready", "published",
           "cleaned", "aborted"}
Epochs == 1..2
InitialImage == [cell \in Cells |-> "seed"]
EmptyImage == [cell \in Cells |-> "absent"]
Apply(image, effect) == [image EXCEPT ![effect.cell] = effect.value]

VARIABLES phase, source, target, logical, journal, base, sampled, baseDone,
          imported, sealed, epoch, attested, confirmed, publishedEpoch, publishedProof,
          targetWritten, wrongOwnerWrite, invalidated
vars == <<phase, source, target, logical, journal, base, sampled, baseDone,
          imported, sealed, epoch, attested, confirmed, publishedEpoch, publishedProof,
          targetWritten, wrongOwnerWrite, invalidated>>

Init ==
  /\ phase = "capture"
  /\ source = InitialImage /\ logical = InitialImage /\ target = EmptyImage
  /\ journal = <<>> /\ base = EmptyImage /\ sampled = {} /\ baseDone = FALSE
  /\ imported = 0 /\ sealed = 0 /\ epoch = 1 /\ attested = 0 /\ confirmed = 0
  /\ publishedEpoch = 0 /\ publishedProof = 0
  /\ targetWritten = FALSE /\ wrongOwnerWrite = FALSE /\ invalidated = FALSE

RECURSIVE Replay(_, _)
Replay(image, n) ==
  IF n = 0 THEN image ELSE Apply(Replay(image, n - 1), journal[n])

Published == phase \in {"published", "cleaned"}

(* An ordinary ACK follows a durable indexed source write and its frame. A
   pre-publication abort leaves the source as the public owner. *)
SourceWrite(effect) ==
  /\ phase \in {"invalid", "aborted"} \/ (phase = "capture" /\ Len(journal) < MaxFrames)
  /\ source' = Apply(source, effect) /\ logical' = Apply(logical, effect)
  /\ journal' = IF phase = "capture" THEN Append(journal, effect) ELSE journal
  /\ UNCHANGED <<phase, target, base, sampled, baseDone, imported, sealed,
                 epoch, attested, confirmed, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite, invalidated>>

(* Unsupported writes invalidate capture and still update source state. *)
InvalidateAndWrite(effect) ==
  /\ phase = "capture"
  /\ phase' = "invalid" /\ invalidated' = TRUE
  /\ source' = Apply(source, effect) /\ logical' = Apply(logical, effect)
  /\ UNCHANGED <<target, journal, base, sampled, baseDone, imported, sealed,
                 epoch, attested, confirmed, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite>>

(* Independent base iterators may see different journal prefixes. Bases
   for untouched cells agree. The target can replay only after the base. *)
CopyCell(cell, n) ==
  /\ phase = "capture" /\ ~baseDone /\ cell \notin sampled
  /\ base' = [base EXCEPT ![cell] = Replay(InitialImage, n)[cell]]
  /\ target' = [target EXCEPT ![cell] = base'[cell]]
  /\ sampled' = sampled \cup {cell}
  /\ UNCHANGED <<phase, source, logical, journal, baseDone, imported, sealed,
                 epoch, attested, confirmed, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite, invalidated>>

FinishBase ==
  /\ phase = "capture" /\ ~baseDone /\ sampled = Cells /\ baseDone' = TRUE
  /\ UNCHANGED <<phase, source, target, logical, journal, base, sampled,
                 imported, sealed, epoch, attested, confirmed, publishedEpoch,
                 publishedProof, targetWritten, wrongOwnerWrite, invalidated>>

ImportFrame(n) ==
  /\ phase \in {"capture", "fenced", "sealed"} /\ baseDone
  /\ imported < n /\ n <= Len(journal)
  /\ Mode = "gap" \/ n = imported + 1
  /\ target' = Apply(target, journal[n]) /\ imported' = n
  /\ UNCHANGED <<phase, source, logical, journal, base, sampled, baseDone,
                 sealed, epoch, attested, confirmed, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite, invalidated>>

(* Exact duplicate receipts return ACK without a data write. A lost reply,
   controller restart, missing RPC, or refused request also stutters. *)
DuplicateReceipt(n) ==
  /\ n \in 1..imported
  /\ UNCHANGED vars

Fence ==
  /\ phase = "capture" \/ (Mode = "invalid" /\ phase = "invalid")
  /\ baseDone /\ phase' = "fenced"
  /\ UNCHANGED <<source, target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, attested, confirmed, publishedEpoch,
                 publishedProof, targetWritten, wrongOwnerWrite, invalidated>>

Seal ==
  /\ phase = "fenced" /\ phase' = "sealed" /\ sealed' = Len(journal)
  /\ UNCHANGED <<source, target, logical, journal, base, sampled, baseDone,
                 imported, epoch, attested, confirmed, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite, invalidated>>

Ready ==
  /\ phase \in {"sealed", "ready"}
  /\ baseDone /\ (Mode = "tail" \/ imported = sealed)
  /\ phase' = "ready" /\ attested' = epoch
  /\ UNCHANGED <<source, target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, confirmed, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite, invalidated>>

ChangeConfiguration ==
  /\ epoch < 2 /\ epoch' = epoch + 1
  /\ UNCHANGED <<phase, source, target, logical, journal, base, sampled,
                 baseDone, imported, sealed, attested, confirmed, publishedEpoch,
                 publishedProof, targetWritten, wrongOwnerWrite, invalidated>>

(* The runner performs its final probe and member revalidation before the
   separate catalog transaction. "config" keeps that concrete split between
   actions. The strict model requires the proof to remain current at publish. *)
Confirm ==
  /\ phase = "ready" /\ attested = epoch /\ confirmed' = epoch
  /\ UNCHANGED <<phase, source, target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, attested, publishedEpoch, publishedProof,
                 targetWritten, wrongOwnerWrite, invalidated>>

Publish ==
  /\ phase = "ready" /\ confirmed # 0 /\ confirmed = attested
  /\ Mode = "config" \/ confirmed = epoch
  /\ phase' = "published" /\ publishedEpoch' = epoch /\ publishedProof' = confirmed
  /\ UNCHANGED <<source, target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, attested, confirmed, targetWritten, wrongOwnerWrite, invalidated>>

Abort ==
  /\ phase \in {"capture", "invalid", "fenced", "sealed", "ready"}
  /\ phase' = "aborted"
  /\ UNCHANGED <<source, target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, attested, confirmed, publishedEpoch,
                 publishedProof, targetWritten, wrongOwnerWrite, invalidated>>

TargetWrite(effect) ==
  /\ Published /\ ~targetWritten
  /\ target' = Apply(target, effect) /\ logical' = Apply(logical, effect)
  /\ targetWritten' = TRUE
  /\ UNCHANGED <<phase, source, journal, base, sampled, baseDone, imported,
                 sealed, epoch, attested, confirmed, publishedEpoch, publishedProof,
                 wrongOwnerWrite, invalidated>>

StaleSourceWrite(effect) ==
  /\ Mode = "owner" /\ Published /\ ~wrongOwnerWrite
  /\ source' = Apply(source, effect) /\ wrongOwnerWrite' = TRUE
  /\ UNCHANGED <<phase, target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, attested, confirmed, publishedEpoch,
                 publishedProof, targetWritten, invalidated>>

Cleanup ==
  /\ phase # "cleaned" /\ (phase = "published" \/ Mode = "cleanup")
  /\ source' = EmptyImage /\ phase' = "cleaned"
  /\ UNCHANGED <<target, logical, journal, base, sampled, baseDone,
                 imported, sealed, epoch, attested, confirmed, publishedEpoch,
                 publishedProof, targetWritten, wrongOwnerWrite, invalidated>>

Next ==
  \/ \E effect \in Postimages :
       SourceWrite(effect) \/ InvalidateAndWrite(effect) \/
       TargetWrite(effect) \/ StaleSourceWrite(effect)
  \/ \E cell \in Cells, n \in 0..Len(journal) : CopyCell(cell, n)
  \/ \E n \in 1..Len(journal) : ImportFrame(n) \/ DuplicateReceipt(n)
  \/ FinishBase \/ Fence \/ Seal \/ Ready \/ ChangeConfiguration
  \/ Confirm \/ Publish \/ Abort \/ Cleanup

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ source \in [Cells -> Values] /\ target \in [Cells -> Values]
  /\ logical \in [Cells -> Values] /\ base \in [Cells -> Values]
  /\ sampled \subseteq Cells /\ baseDone \in BOOLEAN
  /\ Len(journal) <= MaxFrames
  /\ \A i \in 1..Len(journal) : journal[i] \in Postimages
  /\ imported \in 0..Len(journal) /\ sealed \in 0..Len(journal)
  /\ epoch \in Epochs /\ attested \in {0} \cup Epochs /\ confirmed \in {0} \cup Epochs
  /\ publishedEpoch \in {0} \cup Epochs /\ publishedProof \in {0} \cup Epochs
  /\ targetWritten \in BOOLEAN /\ wrongOwnerWrite \in BOOLEAN /\ invalidated \in BOOLEAN

ImportIsExactPrefix == (baseDone /\ ~Published) => target = Replay(base, imported)
VisibleImageMatchesCommits == (IF Published THEN target ELSE source) = logical
PublicationHasCurrentEvidence == Published => publishedEpoch = publishedProof
CleanupAfterPublication == phase = "cleaned" => publishedEpoch \in Epochs
NoStaleOwnerAck == ~wrongOwnerWrite
InvalidatedGenerationNeverPublished == invalidated => ~Published
OwnershipMonotone == Published => Published'
OwnerNeverReturns == [][OwnershipMonotone]_vars

=============================================================================
