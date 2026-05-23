======================================================================
📊 PERFORMANCE TEST SUMMARY
======================================================================

# (Baseline - Kafka)
Latency Measurement Definitions:
  • Producer tests: Time from send() to broker acknowledgment
  • Consumer tests: N/A (throughput only for pre-populated messages)
  • E2E Latency:    Full round-trip time (produce → consume)
======================================================================

## Real Kafka
Producer Throughput:
  ✓ Throughput: 23,400 msg/s (22.85 MB/s)
  ✓ Latency:    avg=153.33ms, p95=283.12ms, p99=299.07ms
  ✓ Errors:     0

Consumer Throughput:
  ✓ Throughput: 2,058 msg/s (1.99 MB/s)
  ℹ Latency:    N/A (throughput test only)
  ✓ Errors:     0

Concurrent Producers (x5):
  ✓ Throughput: 2,060 msg/s (2.01 MB/s)
  ✓ Latency:    avg=2.40ms, p95=3.53ms, p99=4.31ms
  ✓ Errors:     0

Concurrent Consumers (x3):
  ✓ Throughput: 1,150 msg/s (1.11 MB/s)
  ℹ Latency:    N/A (throughput test only)
  ✓ Errors:     0

End-to-End Latency:
  ✓ Throughput: 64 msg/s (0.06 MB/s)
  ✓ Latency:    avg=8001.93ms, p95=14671.99ms, p99=15226.75ms
  ✓ Errors:     0

Large Messages:
  ✓ Throughput: 57 msg/s (5.60 MB/s)
  ✓ Latency:    avg=17.24ms, p95=19.25ms, p99=134.56ms
  ✓ Errors:     0

---

## Mine Toy Kafka
Producer Throughput:
  ✓ Throughput: 30,788 msg/s (30.07 MB/s)
  ✓ Latency:    avg=149.40ms, p95=285.82ms, p99=304.46ms
  ✓ Errors:     0

Consumer Throughput:
  ✓ Throughput: 15,973 msg/s (15.48 MB/s)
  ℹ Latency:    N/A (throughput test only)
  ✓ Errors:     0

Concurrent Producers (x5):
  ✓ Throughput: 1,715 msg/s (1.67 MB/s)
  ✓ Latency:    avg=2.89ms, p95=5.36ms, p99=8.30ms
  ✓ Errors:     0

Concurrent Consumers (x3):
  ✓ Throughput: 9,950 msg/s (9.64 MB/s)
  ℹ Latency:    N/A (throughput test only)
  ✓ Errors:     0

End-to-End Latency:
  ✓ Throughput: 65 msg/s (0.06 MB/s)
  ✓ Latency:    avg=7957.39ms, p95=14636.27ms, p99=15203.93ms
  ✓ Errors:     0

Large Messages:
  ✓ Throughput: 72 msg/s (7.04 MB/s)
  ✓ Latency:    avg=13.66ms, p95=14.36ms, p99=27.54ms
  ✓ Errors:     0

# Milestone 3.1 (Baseline - No Persistence)
Producer Throughput:
  Throughput: 32,158 msg/s (31.40 MB/s)
  Latency:    avg=137.76ms, p95=270.21ms, p99=286.04ms
  Errors:     0

Consumer Throughput:
  Throughput: 17,714 msg/s (16.56 MB/s)
  Latency:    avg=3674.19ms, p95=3875.38ms, p99=3908.27ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,225 msg/s (2.17 MB/s)
  Latency:    avg=2.22ms, p95=3.25ms, p99=3.95ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,886 msg/s (2.70 MB/s)
  Latency:    avg=3553.39ms, p95=3737.45ms, p99=3769.25ms
  Errors:     0

End-to-End Latency:
  Throughput: 63 msg/s (0.06 MB/s)
  Latency:    avg=8282.14ms, p95=15104.62ms, p99=15810.97ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.17 MB/s)
  Latency:    avg=120.61ms, p95=131.21ms, p99=146.51ms
  Errors:     0

======================================================================

# Milestone 3.2 (Before Optimization - Sync Writes)
Producer Throughput:
  Throughput: 31,572 msg/s (30.83 MB/s)  ⚠️ -1.8%
  Latency:    avg=145.32ms, p95=281.86ms, p99=299.81ms
  Errors:     0

Consumer Throughput:
  Throughput: 16,693 msg/s (15.60 MB/s)  ⚠️ -5.8%
  Latency:    avg=3786.35ms, p95=3977.07ms, p99=3998.51ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,208 msg/s (2.16 MB/s)  ⚠️ -0.8%
  Latency:    avg=2.24ms, p95=3.26ms, p99=4.02ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,756 msg/s (9.12 MB/s)  ✅ +238%
  Latency:    avg=3589.95ms, p95=3780.28ms, p99=3822.97ms
  Errors:     0

End-to-End Latency:
  Throughput: 57 msg/s (0.06 MB/s)  ⚠️ -9.5%
  Latency:    avg=9015.65ms, p95=16671.97ms, p99=17353.38ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.26 MB/s)  ✅ +1.1%
  Latency:    avg=119.28ms, p95=123.67ms, p99=150.09ms
  Errors:     0

======================================================================

# Milestone 3.2 (After Optimization - Async Writes + Batching)
Producer Throughput:
  Throughput: 30,828 msg/s (30.11 MB/s)
  Latency:    avg=145.86ms, p95=270.72ms, p99=289.09ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,183 msg/s (17.00 MB/s)
  Latency:    avg=3784.56ms, p95=3935.94ms, p99=3957.84ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,090 msg/s (2.04 MB/s)
  Latency:    avg=2.37ms, p95=3.68ms, p99=4.83ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,877 msg/s (9.23 MB/s)
  Latency:    avg=3611.87ms, p95=3759.56ms, p99=3800.20ms
  Errors:     0

End-to-End Latency:
  Throughput: 57 msg/s (0.06 MB/s)
  Latency:    avg=9041.16ms, p95=16711.19ms, p99=17432.77ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.11 MB/s)
  Latency:    avg=121.59ms, p95=136.95ms, p99=162.25ms
  Errors:     0

======================================================================

# Milestone 3.3
Producer Throughput:
  Throughput: 31,748 msg/s (31.00 MB/s)
  Latency:    avg=160.74ms, p95=260.01ms, p99=267.39ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,670 msg/s (17.45 MB/s)
  Latency:    avg=55481.40ms, p95=64817.92ms, p99=64843.03ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,190 msg/s (2.14 MB/s)
  Latency:    avg=2.26ms, p95=3.33ms, p99=3.98ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,989 msg/s (2.79 MB/s)
  Latency:    avg=3572.49ms, p95=3699.97ms, p99=3717.64ms
  Errors:     0

End-to-End Latency:
  Throughput: 59 msg/s (0.06 MB/s)
  Latency:    avg=38001.32ms, p95=72080.90ms, p99=73086.28ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (7.80 MB/s)
  Latency:    avg=126.48ms, p95=142.83ms, p99=204.20ms
  Errors:     0

======================================================================

# Milestone 4.1
Producer Throughput:
  Throughput: 31,162 msg/s (30.43 MB/s)
  Latency:    avg=165.05ms, p95=265.05ms, p99=272.48ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,371 msg/s (17.17 MB/s)
  Latency:    avg=905922.57ms, p95=1069794.50ms, p99=1069815.84ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,172 msg/s (2.12 MB/s)
  Latency:    avg=2.29ms, p95=3.31ms, p99=3.96ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,837 msg/s (9.19 MB/s)
  Latency:    avg=3661.62ms, p95=3800.11ms, p99=3817.58ms
  Errors:     0

End-to-End Latency:
  Throughput: 60 msg/s (0.06 MB/s)
  Latency:    avg=544616.98ms, p95=1072143.28ms, p99=1073181.88ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (7.94 MB/s)
  Latency:    avg=124.17ms, p95=128.24ms, p99=130.77ms
  Errors:     0

======================================================================

# Milestone 4.2
Producer Throughput:
  Throughput: 34,639 msg/s (33.83 MB/s)
  Latency:    avg=151.86ms, p95=273.52ms, p99=280.16ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,352 msg/s (17.15 MB/s)
  Latency:    avg=48917.93ms, p95=57316.83ms, p99=57337.56ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,214 msg/s (2.16 MB/s)
  Latency:    avg=2.25ms, p95=3.25ms, p99=3.92ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,891 msg/s (2.70 MB/s)
  Latency:    avg=3652.74ms, p95=3799.17ms, p99=3814.81ms
  Errors:     0

End-to-End Latency:
  Throughput: 59 msg/s (0.06 MB/s)
  Latency:    avg=33776.35ms, p95=64519.86ms, p99=65747.04ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (7.95 MB/s)
  Latency:    avg=123.94ms, p95=139.48ms, p99=153.96ms
  Errors:     0

======================================================================

# Milestone 4.3
  Producer Throughput:
  Throughput: 31,156 msg/s (30.43 MB/s)
  Latency:    avg=149.65ms, p95=285.69ms, p99=303.35ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,106 msg/s (16.92 MB/s)
  Latency:    avg=3744.48ms, p95=3948.22ms, p99=3979.41ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,236 msg/s (2.18 MB/s)
  Latency:    avg=2.21ms, p95=3.21ms, p99=3.83ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,876 msg/s (9.23 MB/s)
  Latency:    avg=3525.25ms, p95=3703.04ms, p99=3733.14ms
  Errors:     0

End-to-End Latency:
  Throughput: 61 msg/s (0.06 MB/s)
  Latency:    avg=8550.87ms, p95=15753.26ms, p99=16318.13ms
  Errors:     0

Large Messages:
  Throughput: 9 msg/s (8.52 MB/s)
  Latency:    avg=115.67ms, p95=124.17ms, p99=139.52ms
  Errors:     0

======================================================================

# Milestone 5.1
Producer Throughput:
  Throughput: 33,663 msg/s (32.87 MB/s)
  Latency:    avg=156.75ms, p95=270.12ms, p99=276.88ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,671 msg/s (17.45 MB/s)
  Latency:    avg=82559.29ms, p95=97472.76ms, p99=97506.96ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,155 msg/s (2.10 MB/s)
  Latency:    avg=2.30ms, p95=3.37ms, p99=4.91ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,807 msg/s (9.17 MB/s)
  Latency:    avg=3618.33ms, p95=3772.83ms, p99=3791.36ms
  Errors:     0

End-to-End Latency:
  Throughput: 57 msg/s (0.06 MB/s)
  Latency:    avg=56904.88ms, p95=103348.74ms, p99=104386.20ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.42 MB/s)
  Latency:    avg=117.06ms, p95=123.43ms, p99=127.35ms
  Errors:     0

======================================================================

# Milestone 5.2
Producer Throughput:
  Throughput: 34,405 msg/s (33.60 MB/s)
  Latency:    avg=158.45ms, p95=268.07ms, p99=274.78ms
  Errors:     0

Consumer Throughput:
  Throughput: 19,030 msg/s (17.79 MB/s)
  Latency:    avg=49982.58ms, p95=59103.42ms, p99=59134.54ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,190 msg/s (2.14 MB/s)
  Latency:    avg=2.27ms, p95=3.29ms, p99=4.04ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,975 msg/s (2.78 MB/s)
  Latency:    avg=3487.55ms, p95=3631.28ms, p99=3647.87ms
  Errors:     0

End-to-End Latency:
  Throughput: 60 msg/s (0.06 MB/s)
  Latency:    avg=32593.18ms, p95=63935.79ms, p99=65083.51ms
  Errors:     0

Large Messages:
  Throughput: 9 msg/s (8.65 MB/s)
  Latency:    avg=113.97ms, p95=118.69ms, p99=123.10ms
  Errors:     0

======================================================================

# Milestone 5.3
Producer Throughput:
  Throughput: 34,829 msg/s (34.01 MB/s)
  Latency:    avg=151.31ms, p95=264.18ms, p99=270.75ms
  Errors:     0

Consumer Throughput:
  Throughput: 17,638 msg/s (16.48 MB/s)
  Latency:    avg=48669.09ms, p95=57518.14ms, p99=57554.30ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,180 msg/s (2.13 MB/s)
  Latency:    avg=2.28ms, p95=3.30ms, p99=3.97ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,817 msg/s (9.18 MB/s)
  Latency:    avg=3641.88ms, p95=3789.19ms, p99=3805.76ms
  Errors:     0

End-to-End Latency:
  Throughput: 56 msg/s (0.05 MB/s)
  Latency:    avg=32491.25ms, p95=63135.60ms, p99=64371.58ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (7.94 MB/s)
  Latency:    avg=124.20ms, p95=139.66ms, p99=234.68ms
  Errors:     0

======================================================================

# Milestone 6.1
Producer Throughput:
  Throughput: 33,195 msg/s (32.42 MB/s)
  Latency:    avg=156.83ms, p95=277.24ms, p99=284.44ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,541 msg/s (17.33 MB/s)
  Latency:    avg=48423.00ms, p95=57200.84ms, p99=57230.78ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,218 msg/s (2.17 MB/s)
  Latency:    avg=2.24ms, p95=3.25ms, p99=3.98ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,903 msg/s (2.71 MB/s)
  Latency:    avg=3564.27ms, p95=3766.62ms, p99=3783.52ms
  Errors:     0

End-to-End Latency:
  Throughput: 61 msg/s (0.06 MB/s)
  Latency:    avg=35116.43ms, p95=64108.89ms, p99=65239.77ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.19 MB/s)
  Latency:    avg=120.37ms, p95=122.61ms, p99=130.96ms
  Errors:     0

======================================================================

# Milestone 6.2
Producer Throughput:
  Throughput: 33,688 msg/s (32.90 MB/s)
  Latency:    avg=152.51ms, p95=275.42ms, p99=282.42ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,514 msg/s (17.30 MB/s)
  Latency:    avg=47584.36ms, p95=55737.33ms, p99=55776.12ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,231 msg/s (2.18 MB/s)
  Latency:    avg=2.23ms, p95=3.23ms, p99=3.84ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,798 msg/s (9.16 MB/s)
  Latency:    avg=3514.31ms, p95=3671.10ms, p99=3689.84ms
  Errors:     0

End-to-End Latency:
  Throughput: 57 msg/s (0.06 MB/s)
  Latency:    avg=33041.45ms, p95=60912.53ms, p99=62059.30ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.28 MB/s)
  Latency:    avg=119.09ms, p95=124.60ms, p99=130.41ms
  Errors:     0

======================================================================

# Milestone 6.3
Producer Throughput:
  Throughput: 33,084 msg/s (32.31 MB/s)
  Latency:    avg=153.90ms, p95=278.66ms, p99=286.00ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,144 msg/s (16.96 MB/s)
  Latency:    avg=49540.55ms, p95=57890.30ms, p99=57937.18ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,201 msg/s (2.15 MB/s)
  Latency:    avg=2.26ms, p95=3.27ms, p99=3.85ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,888 msg/s (2.70 MB/s)
  Latency:    avg=3601.34ms, p95=3734.18ms, p99=3752.93ms
  Errors:     0

End-to-End Latency:
  Throughput: 59 msg/s (0.06 MB/s)
  Latency:    avg=33226.18ms, p95=65400.67ms, p99=66537.08ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.21 MB/s)
  Latency:    avg=120.11ms, p95=124.63ms, p99=140.19ms
  Errors:     0

======================================================================

# Milestone 7.1
Producer Throughput:
  Throughput: 32,994 msg/s (32.22 MB/s)
  Latency:    avg=151.18ms, p95=249.76ms, p99=256.83ms
  Errors:     0

Consumer Throughput:
  Throughput: 17,842 msg/s (16.68 MB/s)
  Latency:    avg=51324.48ms, p95=60149.53ms, p99=60189.53ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,187 msg/s (2.14 MB/s)
  Latency:    avg=2.27ms, p95=3.31ms, p99=4.01ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,893 msg/s (2.70 MB/s)
  Latency:    avg=3561.23ms, p95=3752.99ms, p99=3769.01ms
  Errors:     0

End-to-End Latency:
  Throughput: 63 msg/s (0.06 MB/s)
  Latency:    avg=32943.61ms, p95=64234.36ms, p99=65380.98ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.28 MB/s)
  Latency:    avg=119.04ms, p95=122.29ms, p99=125.34ms
  Errors:     0

======================================================================

# Milestone 7.2
Producer Throughput:
  Throughput: 34,773 msg/s (33.96 MB/s)
  Latency:    avg=144.91ms, p95=268.16ms, p99=274.94ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,348 msg/s (17.15 MB/s)
  Latency:    avg=49074.73ms, p95=57995.08ms, p99=58032.21ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,200 msg/s (2.15 MB/s)
  Latency:    avg=2.26ms, p95=3.27ms, p99=3.88ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,986 msg/s (2.79 MB/s)
  Latency:    avg=3623.16ms, p95=3769.03ms, p99=3784.12ms
  Errors:     0

End-to-End Latency:
  Throughput: 58 msg/s (0.06 MB/s)
  Latency:    avg=34231.39ms, p95=65276.06ms, p99=66597.96ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.03 MB/s)
  Latency:    avg=122.79ms, p95=136.51ms, p99=208.82ms
  Errors:     0

======================================================================

# Milestone 7.3
Producer Throughput:
  Throughput: 33,949 msg/s (33.15 MB/s)
  Latency:    avg=151.47ms, p95=257.67ms, p99=265.16ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,348 msg/s (17.15 MB/s)
  Latency:    avg=49474.26ms, p95=58131.85ms, p99=58162.38ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,184 msg/s (2.13 MB/s)
  Latency:    avg=2.27ms, p95=3.31ms, p99=4.05ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,904 msg/s (2.71 MB/s)
  Latency:    avg=3607.57ms, p95=3744.50ms, p99=3760.90ms
  Errors:     0

End-to-End Latency:
  Throughput: 59 msg/s (0.06 MB/s)
  Latency:    avg=33093.73ms, p95=65112.79ms, p99=66370.29ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.18 MB/s)
  Latency:    avg=120.62ms, p95=124.53ms, p99=128.31ms
  Errors:     0

======================================================================

# Milestone 8
Producer Throughput:
  Throughput: 34,081 msg/s (33.28 MB/s)
  Latency:    avg=150.05ms, p95=273.76ms, p99=280.50ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,262 msg/s (17.07 MB/s)
  Latency:    avg=49927.13ms, p95=58598.34ms, p99=58629.85ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,203 msg/s (2.15 MB/s)
  Latency:    avg=2.26ms, p95=3.27ms, p99=3.92ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,890 msg/s (2.70 MB/s)
  Latency:    avg=3597.75ms, p95=3767.85ms, p99=3784.56ms
  Errors:     0

End-to-End Latency:
  Throughput: 56 msg/s (0.05 MB/s)
  Latency:    avg=34654.24ms, p95=66618.32ms, p99=67772.78ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.26 MB/s)
  Latency:    avg=119.16ms, p95=122.74ms, p99=128.37ms
  Errors:     0

======================================================================

# Milestone 9
Producer Throughput:
  Throughput: 34,078 msg/s (33.28 MB/s)
  Latency:    avg=159.53ms, p95=276.41ms, p99=283.09ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,317 msg/s (17.12 MB/s)
  Latency:    avg=48822.25ms, p95=57489.53ms, p99=57521.03ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,205 msg/s (2.15 MB/s)
  Latency:    avg=2.25ms, p95=3.27ms, p99=3.85ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,891 msg/s (2.70 MB/s)
  Latency:    avg=3615.41ms, p95=3763.26ms, p99=3778.84ms
  Errors:     0

End-to-End Latency:
  Throughput: 58 msg/s (0.06 MB/s)
  Latency:    avg=33113.33ms, p95=65258.35ms, p99=66304.50ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.34 MB/s)
  Latency:    avg=118.19ms, p95=122.62ms, p99=126.01ms
  Errors:     0

======================================================================

# Milestone 10
Producer Throughput:
  Throughput: 34,623 msg/s (33.81 MB/s)
  Latency:    avg=151.61ms, p95=270.31ms, p99=276.93ms
  Errors:     0

Consumer Throughput:
  Throughput: 21,574 msg/s (20.16 MB/s)
  Latency:    avg=50779.59ms, p95=59500.16ms, p99=59536.48ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,107 msg/s (2.06 MB/s)
  Latency:    avg=2.36ms, p95=3.56ms, p99=6.21ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,982 msg/s (2.79 MB/s)
  Latency:    avg=3490.26ms, p95=3651.72ms, p99=3669.17ms
  Errors:     0

End-to-End Latency:
  Throughput: 56 msg/s (0.05 MB/s)
  Latency:    avg=32942.60ms, p95=65092.71ms, p99=66693.09ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (7.94 MB/s)
  Latency:    avg=124.08ms, p95=157.85ms, p99=229.43ms
  Errors:     0

======================================================================

# Milestone 11
Producer Throughput:
  Throughput: 34,510 msg/s (33.70 MB/s)
  Latency:    avg=153.61ms, p95=273.03ms, p99=279.69ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,361 msg/s (17.16 MB/s)
  Latency:    avg=51857.06ms, p95=60927.56ms, p99=60970.61ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,211 msg/s (2.16 MB/s)
  Latency:    avg=2.24ms, p95=3.24ms, p99=3.83ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,977 msg/s (2.78 MB/s)
  Latency:    avg=3671.63ms, p95=3808.13ms, p99=3828.42ms
  Errors:     0

End-to-End Latency:
  Throughput: 58 msg/s (0.06 MB/s)
  Latency:    avg=33595.03ms, p95=65856.01ms, p99=67036.84ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.24 MB/s)
  Latency:    avg=119.68ms, p95=122.99ms, p99=128.21ms
  Errors:     0

**Solution Applied:** DISABLED consumer lag computation thread entirely
- Lag computation code remains in place but thread is not started
- Can be re-enabled via config: consumer.lag.metrics.enabled=true (future work)
- Performance partially restored but still ~10-16% below Milestone 10 for producer/consumer

**Analysis:**
The monitoring infrastructure (MonitoringManager, MetricsCollector) adds some overhead even without the lag computation thread. Performance is significantly better than with lag computation enabled, but not fully back to Milestone 10 baseline. This suggests:
1. MonitoringManager HTTP server may have minor overhead
2. MetricsCollector instrumentation adds some latency
3. Additional initialization code in broker startup adds overhead

**Recommendation:** For production use, consider making MonitoringManager optional via config.

======================================================================

# Milestone 12
Producer Throughput:
  Throughput: 34,630 msg/s (33.82 MB/s)
  Latency:    avg=146.98ms, p95=269.73ms, p99=276.35ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,259 msg/s (17.07 MB/s)
  Latency:    avg=47955.61ms, p95=56657.25ms, p99=56702.62ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,212 msg/s (2.16 MB/s)
  Latency:    avg=2.24ms, p95=3.28ms, p99=4.02ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,795 msg/s (9.16 MB/s)
  Latency:    avg=3731.01ms, p95=3869.12ms, p99=3884.68ms
  Errors:     0

End-to-End Latency:
  Throughput: 59 msg/s (0.06 MB/s)
  Latency:    avg=31438.78ms, p95=61444.99ms, p99=62520.43ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.16 MB/s)
  Latency:    avg=120.76ms, p95=128.03ms, p99=138.82ms
  Errors:     0

======================================================================

# Milestone 13
Producer Throughput:
  Throughput: 32,693 msg/s (31.93 MB/s)
  Latency:    avg=158.58ms, p95=288.30ms, p99=295.30ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,326 msg/s (17.13 MB/s)
  Latency:    avg=52131.99ms, p95=61410.51ms, p99=61453.05ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,160 msg/s (2.11 MB/s)
  Latency:    avg=2.30ms, p95=3.33ms, p99=4.22ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,817 msg/s (9.18 MB/s)
  Latency:    avg=3585.80ms, p95=3782.11ms, p99=3801.26ms
  Errors:     0

End-to-End Latency:
  Throughput: 57 msg/s (0.06 MB/s)
  Latency:    avg=33932.25ms, p95=64342.24ms, p99=65641.44ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.03 MB/s)
  Latency:    avg=122.82ms, p95=131.03ms, p99=159.06ms
  Errors:     0

======================================================================

# Milestone 14
Producer Throughput:
  Throughput: 34,928 msg/s (34.11 MB/s)
  Latency:    avg=149.75ms, p95=269.20ms, p99=275.74ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,160 msg/s (16.97 MB/s)
  Latency:    avg=49768.73ms, p95=58240.65ms, p99=58273.96ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,229 msg/s (2.18 MB/s)
  Latency:    avg=2.22ms, p95=3.27ms, p99=3.84ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,884 msg/s (2.70 MB/s)
  Latency:    avg=3569.98ms, p95=3732.73ms, p99=3750.60ms
  Errors:     0

End-to-End Latency:
  Throughput: 58 msg/s (0.06 MB/s)
  Latency:    avg=34177.45ms, p95=65294.86ms, p99=66918.23ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.31 MB/s)
  Latency:    avg=118.67ms, p95=123.50ms, p99=124.50ms
  Errors:     0

======================================================================

# Milestone 15
Producer Throughput:
  Throughput: 34,642 msg/s (33.83 MB/s)
  Latency:    avg=153.05ms, p95=267.02ms, p99=273.61ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,162 msg/s (16.97 MB/s)
  Latency:    avg=51725.76ms, p95=60719.32ms, p99=60758.48ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,269 msg/s (2.22 MB/s)
  Latency:    avg=2.19ms, p95=3.18ms, p99=3.74ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,836 msg/s (9.19 MB/s)
  Latency:    avg=3599.27ms, p95=3774.34ms, p99=3790.82ms
  Errors:     0

End-to-End Latency:
  Throughput: 56 msg/s (0.05 MB/s)
  Latency:    avg=33263.16ms, p95=63757.72ms, p99=64961.71ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.13 MB/s)
  Latency:    avg=121.30ms, p95=124.78ms, p99=140.19ms
  Errors:     0

======================================================================

# Milestone 16
Producer Throughput:
  Throughput: 33,770 msg/s (32.98 MB/s)
  Latency:    avg=146.04ms, p95=238.41ms, p99=245.07ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,640 msg/s (17.42 MB/s)
  Latency:    avg=50827.16ms, p95=59247.31ms, p99=59274.61ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,229 msg/s (2.18 MB/s)
  Latency:    avg=2.23ms, p95=3.23ms, p99=3.84ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 2,886 msg/s (2.70 MB/s)
  Latency:    avg=3555.04ms, p95=3739.02ms, p99=3755.95ms
  Errors:     0

End-to-End Latency:
  Throughput: 60 msg/s (0.06 MB/s)
  Latency:    avg=33398.81ms, p95=64128.51ms, p99=65055.44ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (7.92 MB/s)
  Latency:    avg=124.48ms, p95=137.45ms, p99=149.92ms
  Errors:     0

======================================================================

# Milestone 17
Producer Throughput:
  Throughput: 33,447 msg/s (32.66 MB/s)
  Latency:    avg=151.11ms, p95=260.96ms, p99=267.63ms
  Errors:     0

Consumer Throughput:
  Throughput: 18,198 msg/s (17.01 MB/s)
  Latency:    avg=51416.89ms, p95=60225.15ms, p99=60265.97ms
  Errors:     0

Concurrent Producers (x5):
  Throughput: 2,227 msg/s (2.17 MB/s)
  Latency:    avg=2.22ms, p95=3.23ms, p99=3.86ms
  Errors:     0

Concurrent Consumers (x3):
  Throughput: 9,792 msg/s (9.15 MB/s)
  Latency:    avg=3700.02ms, p95=3853.14ms, p99=3871.46ms
  Errors:     0

End-to-End Latency:
  Throughput: 57 msg/s (0.06 MB/s)
  Latency:    avg=34060.41ms, p95=63065.18ms, p99=64351.61ms
  Errors:     0

Large Messages:
  Throughput: 8 msg/s (8.16 MB/s)
  Latency:    avg=120.81ms, p95=126.98ms, p99=130.35ms
  Errors:     0

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

======================================================================

# Milestone 12

```
