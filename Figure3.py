import matplotlib.pyplot as plt
import numpy as np

# ==============================================================================
# 1. Complete Experimental Data (1s - 11s)
# ==============================================================================
freqs = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])

# Average Throughput (TPS)
tps_data = np.array([
    1942.32, 1974.09, 2091.24, 2180.57, 
    2391.63, 2381.53, 2190.07, 2394.04, 
    2270.09, 2396.39, 2300.12
])

# Cumulative Disk I/O (MB)
io_data = np.array([
    3200.24, 1536.18, 1024.23, 768.18, 
    641.40, 512.25, 384.98, 384.25, 
    257.84, 257.24, 256.81
])

# Recovery Time (seconds)
rec_data = np.array([
    0.0766, 0.0959, 0.0774, 0.0724, 
    0.1165, 0.0751, 0.1006, 0.0813, 
    0.1414, 0.1081, 0.1086
])

# ==============================================================================
# 2. Plotting Configuration
# ==============================================================================
# Use a wider canvas (24x7) to ensure breathing room for all three plots
fig, axes = plt.subplots(1, 3, figsize=(24, 7))
plt.style.use('seaborn-v0_8-whitegrid')

# -------------------------------------------------------
# Chart 1: Throughput (Performance)
# -------------------------------------------------------
ax1 = axes[0]
ax1.plot(freqs, tps_data, marker='o', linewidth=3, color='#2ca02c', label='TPS')
ax1.set_title('Performance Trend: Throughput vs. Frequency', fontsize=16, fontweight='bold')
ax1.set_xlabel('Checkpoint Interval (Seconds)', fontsize=12)
ax1.set_ylabel('TPS (Transactions/sec)', fontsize=12, fontweight='bold', color='#2ca02c')
ax1.grid(True, linestyle='--', alpha=0.6)
ax1.set_xticks(freqs)

# Keep the background shading for the optimal zone as it is useful and non-intrusive
ax1.axvspan(5, 11, color='#2ca02c', alpha=0.1)
ax1.text(8, 2000, "Optimal High\nPerformance Zone", ha='center', fontsize=11, fontweight='bold', color='green')

# -------------------------------------------------------
# Chart 2: Disk I/O (Cost)
# -------------------------------------------------------
ax2 = axes[1]
ax2.plot(freqs, io_data, marker='s', linewidth=3, color='#ff7f0e', label='Disk I/O')
# Fill color
ax2.fill_between(freqs, io_data, color='#ff7f0e', alpha=0.2)
ax2.set_title('Cost Efficiency: Cumulative Disk I/O', fontsize=16, fontweight='bold')
ax2.set_xlabel('Checkpoint Interval (Seconds)', fontsize=12)
ax2.set_ylabel('Total MB Written (Lower is Better)', fontsize=12, fontweight='bold', color='#ff7f0e')
ax2.grid(True, linestyle='--', alpha=0.6)
ax2.set_xticks(freqs)

# -------------------------------------------------------
# Chart 3: Recovery Risk (Risk)
# -------------------------------------------------------
ax3 = axes[2]
ax3.plot(freqs, rec_data, marker='^', linewidth=3, color='#d62728', markersize=9, label='Recovery Time')
ax3.set_title('Availability Risk: Recovery Time Volatility', fontsize=16, fontweight='bold')
ax3.set_xlabel('Checkpoint Interval (Seconds)', fontsize=12)
ax3.set_ylabel('Recovery Time (Seconds)', fontsize=12, fontweight='bold', color='#d62728')
ax3.grid(True, linestyle='--', alpha=0.6)
ax3.set_xticks(freqs)

# Keep the trend line, as it is important for showing "increased risk with lower frequency"
z = np.polyfit(freqs, rec_data, 1)
p = np.poly1d(z)
ax3.plot(freqs, p(freqs), linestyle='--', color='blue', alpha=0.5, linewidth=2, label='Risk Trend')

ax3.legend(loc='upper left')

# -------------------------------------------------------
# Final Layout
# -------------------------------------------------------
plt.suptitle('Checkpoint Frequency Optimization Analysis (1s - 11s)', fontsize=20, y=1.02)
plt.tight_layout()
plt.show()