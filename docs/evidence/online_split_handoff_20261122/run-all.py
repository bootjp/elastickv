import subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
subprocess.run(['python3',str(root.parent/'no-split-sync.py'),'no-split-sync-2'],cwd=root.parent,check=True)
subprocess.run(['python3',str(root/'measure.py')],cwd=root,check=True)
(root/'all-complete').write_text('done\n')
