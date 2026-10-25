| Backend | Metric (ms) | n | Mean | p50 | p95 | Min | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| docker | create | 20 | 114.779 | 110.864 | 129.066 | 103.343 | 131.805 |
| docker | attach-to-exit | 20 | 221.923 | 219.797 | 239.576 | 192.814 | 257.869 |
| docker | workload | 20 | 121.534 | 120.993 | 130.206 | 110.898 | 131.194 |
| docker | cleanup | 20 | 69.662 | 69.713 | 79.005 | 60.048 | 80.828 |
| docker | total | 20 | 437.567 | 433.374 | 471.646 | 392.572 | 498.260 |
| gvisor | create | 20 | 113.008 | 112.929 | 127.629 | 93.024 | 146.375 |
| gvisor | attach-to-exit | 20 | 240.718 | 238.302 | 263.980 | 209.438 | 264.047 |
| gvisor | workload | 20 | 149.587 | 148.056 | 166.294 | 125.416 | 168.733 |
| gvisor | cleanup | 20 | 68.698 | 67.915 | 80.086 | 60.392 | 81.414 |
| gvisor | total | 20 | 453.487 | 444.425 | 503.460 | 409.266 | 525.473 |

Excluded 3 warmups for docker.
Excluded 3 warmups for gvisor.
Percentiles use nearest rank. Workload timings come from daemon timestamps; attach and total include orchestration overhead. These are local, warm-image no-op measurements.
