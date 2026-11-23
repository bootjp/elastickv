open AdapterProtocols

structure LuaOp where
  cmd : String
  key : Nat
  value : Option Int := none
  deriving Lean.ToJson

private def get (k : Nat) : LuaOp := ⟨"get", k, none⟩
private def put (k : Nat) (v : Option Int) : LuaOp := ⟨"set", k, v⟩

private def luaCase (ops : List LuaOp) : Lean.Json := Id.run do
  let initial := [some (2 : Int), none, some 8]
  let db := fun i => initial[i]?.join
  let (replies, final) := execute db (ops.map fun op =>
    if op.cmd = "get" then .get op.key else .set op.key op.value)
  return Lean.Json.mkObj [("ops", Lean.toJson ops), ("initial", Lean.toJson initial),
    ("reads", Lean.toJson replies), ("final", Lean.toJson ((List.range 3).map final))]

private def partCase (ids : List Nat) (wrong : Bool) : Lean.Json := Id.run do
  let requested := ids.map fun n => (n, if wrong then "changed" else toString n)
  let rows := fun n => if n ∈ [1,2,3] then some (Part.mk n (toString n) 5242880 1) else none
  let before : Upload := ⟨true, rows, none⟩
  let parts := (publish before requested rows).bind Upload.head
  return Lean.Json.mkObj [("ids", Lean.toJson ids), ("wrong", Lean.toJson wrong),
    ("published", Lean.toJson (parts.map (List.map Part.num)))]

private def receiptCase (first last deadline now sec : Int) : Lean.Json := Id.run do
  let r : Receipt := ⟨1, 1, last, deadline⟩
  return Lean.Json.mkObj [("first", Lean.toJson first), ("last", Lean.toJson last),
    ("deadline", Lean.toJson deadline), ("now", Lean.toJson now), ("seconds", Lean.toJson sec),
    ("next", Lean.toJson ((change r 1 1 now sec).map Receipt.deadline))]

def main : IO Unit := do
  let commands := [get 0, get 1, put 0 (some 7), put 1 (some 5), put 0 none]
  let lua := commands.flatMap fun a => commands.map fun b => luaCase [a, b, get 0, get 1]
  let parts := [[1], [1,2,3], [1,3], [], [1,1], [2,1], [0], [10001], [1,4]].flatMap
    fun ids => [partCase ids true, partCase ids false]
  let receipts := [0, 36000001].flatMap fun last =>
    [36001000, 36001001, 40000000].flatMap fun now =>
      [0, 1, 7200, 43199, 43200].map fun sec => receiptCase 1000 last 40000000 now sec
  let ttl := ([none, some 1000, some 1001] : List (Option Int)).flatMap fun deadline =>
    ([999, 1000, 1001, 1002] : List Int).map fun now =>
      Lean.Json.mkObj [("deadline", Lean.toJson deadline), ("now", Lean.toJson now),
        ("expected", Lean.toJson (pttl deadline now))]
  IO.println (Lean.Json.mkObj [("lua", Lean.toJson lua), ("parts", Lean.toJson parts),
    ("receipts", Lean.toJson receipts), ("ttl", Lean.toJson ttl)]).compress
