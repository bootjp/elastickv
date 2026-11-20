
## Unprofiled fixed-load results

| Trial | Read success/s | Write success/s | Read RPC p99 ms | Write RPC p99 ms | Capacity drops | Scheduler drops | Unknown writes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 (baseline) | 195.00 | 181.95 | 80.94 | 178.59 | 32270 | 172 | 4246 |
| B1 (candidate) | 199.20 | 185.49 | 77.04 | 173.29 | 29930 | 103 | 4334 |
| B2 (candidate) | 198.51 | 184.88 | 80.07 | 173.66 | 30340 | 107 | 4301 |
| A2 (baseline) | 194.64 | 180.53 | 81.92 | 181.73 | 32869 | 119 | 4296 |

| Trial | Preflight upper s | Versions checked | Cross-group Done s | ACK writes | Audit reads passed | Post-Done write/readbacks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 4.641 | 4771 | 191.615 | 54611 | 163833 | 3000/3000 |
| B1 | 4.243 | 4800 | 193.875 | 55672 | 167016 | 3000/3000 |
| B2 | 3.951 | 4747 | 190.047 | 55494 | 166482 | 3000/3000 |
| A2 | 2.961 | 4517 | 186.651 | 54188 | 162564 | 3000/3000 |
