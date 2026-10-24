| Backend | Metric (ms) | n | Mean | p50 | p95 | Min | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| docker | create | 20 | 115.797 | 111.871 | 134.005 | 93.553 | 148.817 |
| docker | attach-to-exit | 20 | 227.402 | 224.347 | 256.020 | 194.519 | 278.110 |
| docker | workload | 20 | 126.943 | 125.892 | 146.102 | 106.157 | 150.255 |
| docker | cleanup | 20 | 69.570 | 67.892 | 80.544 | 62.810 | 82.225 |
| docker | total | 20 | 444.493 | 432.657 | 522.458 | 391.481 | 527.027 |
| gvisor | create | 20 | 120.887 | 117.509 | 134.887 | 104.374 | 149.639 |
| gvisor | attach-to-exit | 20 | 259.339 | 254.072 | 286.005 | 226.502 | 329.123 |
| gvisor | workload | 20 | 160.351 | 159.881 | 178.134 | 137.997 | 184.349 |
| gvisor | cleanup | 20 | 70.851 | 71.292 | 76.725 | 59.395 | 86.837 |
| gvisor | total | 20 | 483.321 | 475.283 | 527.825 | 430.904 | 607.421 |

Excluded 3 warmups for docker.
Excluded 3 warmups for gvisor.
Percentiles use nearest rank. Workload timings come from daemon timestamps; attach and total include orchestration overhead. These are local, warm-image no-op measurements.
