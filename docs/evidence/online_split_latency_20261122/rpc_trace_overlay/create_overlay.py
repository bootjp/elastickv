import json,sys
from pathlib import Path
source=Path(sys.argv[1]).resolve()
overlay=Path(__file__).resolve().parent
print(json.dumps({"Replace":{str(source/"internal/loadtest"/(name+".go")):str(overlay/(name+".go.txt")) for name in ["raw","runner"]}}))
