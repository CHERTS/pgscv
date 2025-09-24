package collector

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/shirou/gopsutil/v4/mem"
	"github.com/stretchr/testify/assert"
)

func TestMeminfoCollector_Update(t *testing.T) {
	vInfo, err := mem.VirtualMemory()
	assert.NoError(t, err)
	assert.NotNil(t, vInfo)

	fmt.Printf("Total: %v, Free: %v, UsedPercent: %f%%\n", vInfo.Total, vInfo.Free, vInfo.UsedPercent)
	fmt.Printf("SwapTotal: %d, SwapFree: %d, SwapCached: %d\n", vInfo.SwapTotal, vInfo.SwapFree, vInfo.SwapCached)
	data, _ := json.MarshalIndent(vInfo, "", "")
	var jsonMap map[string]any
	if err := json.Unmarshal([]byte(data), &jsonMap); err != nil {
		fmt.Println("Error reading JSON:", err)
	}
	for param, value := range jsonMap {
		fmt.Printf("%s - %f\n", param, value.(float64))
	}

	sInfo, err := mem.SwapMemory()
	assert.NoError(t, err)
	assert.NotNil(t, sInfo)

	if runtime.GOOS == "linux" {
		vmstat, err := os.ReadFile("/proc/vmstat")
		assert.NoError(t, err)
		fmt.Println(string(vmstat))

		var input = pipelineInput{
			required: []string{
				// memInfo
				"node_memory_total", "node_memory_free", "node_memory_available", "node_memory_active",
				"node_memory_buffers", "node_memory_cached", "node_memory_active",
				"node_memory_swapCached", "node_memory_swapTotal", "node_memory_swapFree",
				// vmstat
				"node_vmstat_nr_anon_pages", "node_vmstat_nr_mapped", "node_vmstat_nr_dirty", "node_vmstat_nr_writeback",
				"node_vmstat_pgpgin", "node_vmstat_pgpgout", "node_vmstat_pswpin", "node_vmstat_pswpout",
			},
			optional: []string{
				// memInfo
				"node_memory_sreclaimable", "node_memory_slab", "node_memory_vmallocUsed", "node_memory_hugePagesFree",
				"node_memory_lowFree", "node_memory_mapped", "node_memory_laundry", "node_memory_dirty",
				"node_memory_shared", "node_memory_used", "node_memory_writeBack", "node_memory_highFree",
				"node_memory_hugePagesTotal", "node_memory_vmallocTotal", "node_memory_hugePageSize",
				"node_memory_wired", "node_memory_committedAS", "node_memory_vmallocChunk",
				"node_memory_hugePagesRsvd", "node_memory_usedPercent", "node_memory_writeBackTmp",
				"node_memory_sunreclaim", "node_memory_highTotal", "node_memory_hugePagesSurp",
				"node_memory_anonHugePages", "node_memory_pageTables", "node_memory_inactive",
				"node_memory_commitLimit", "node_memory_lowTotal", "node_memory_SwapUsed",
				// vmstat
				"node_vmstat_nr_free_pages", "node_vmstat_nr_zone_inactive_anon", "node_vmstat_nr_zone_active_anon",
				"node_vmstat_nr_zone_inactive_file", "node_vmstat_nr_zone_active_file", "node_vmstat_nr_zone_unevictable",
				"node_vmstat_nr_zone_write_pending", "node_vmstat_nr_mlock", "node_vmstat_nr_page_table_pages",
				"node_vmstat_nr_kernel_stack", "node_vmstat_nr_bounce", "node_vmstat_nr_zspages", "node_vmstat_nr_free_cma",
				"node_vmstat_numa_hit", "node_vmstat_numa_miss", "node_vmstat_numa_foreign", "node_vmstat_numa_interleave",
				"node_vmstat_numa_local", "node_vmstat_numa_other", "node_vmstat_nr_inactive_anon", "node_vmstat_nr_active_anon",
				"node_vmstat_nr_inactive_file", "node_vmstat_nr_active_file", "node_vmstat_nr_unevictable",
				"node_vmstat_nr_slab_reclaimable", "node_vmstat_nr_slab_unreclaimable", "node_vmstat_nr_isolated_anon",
				"node_vmstat_nr_isolated_file", "node_vmstat_workingset_nodes", "node_vmstat_workingset_refault",
				"node_vmstat_workingset_activate", "node_vmstat_workingset_restore", "node_vmstat_workingset_nodereclaim",
				"node_vmstat_nr_file_pages", "node_vmstat_nr_writeback_temp", "node_vmstat_nr_shmem",
				"node_vmstat_nr_shmem_hugepages", "node_vmstat_nr_shmem_pmdmapped", "node_vmstat_nr_file_hugepages",
				"node_vmstat_nr_file_pmdmapped", "node_vmstat_nr_anon_transparent_hugepages", "node_vmstat_nr_unstable",
				"node_vmstat_nr_vmscan_write", "node_vmstat_nr_vmscan_immediate_reclaim", "node_vmstat_nr_dirtied",
				"node_vmstat_nr_written", "node_vmstat_nr_kernel_misc_reclaimable", "node_vmstat_nr_dirty_threshold",
				"node_vmstat_nr_dirty_background_threshold", "node_vmstat_pgalloc_dma", "node_vmstat_pgalloc_dma32",
				"node_vmstat_pgalloc_normal", "node_vmstat_pgalloc_movable", "node_vmstat_allocstall_dma",
				"node_vmstat_allocstall_dma32", "node_vmstat_allocstall_normal", "node_vmstat_allocstall_movable",
				"node_vmstat_pgskip_dma", "node_vmstat_pgskip_dma32", "node_vmstat_pgskip_normal",
				"node_vmstat_pgskip_movable", "node_vmstat_pgfree", "node_vmstat_pgactivate", "node_vmstat_pgdeactivate",
				"node_vmstat_pglazyfree", "node_vmstat_pgfault", "node_vmstat_pgmajfault", "node_vmstat_pglazyfreed",
				"node_vmstat_pgrefill", "node_vmstat_pgsteal_kswapd", "node_vmstat_pgsteal_direct",
				"node_vmstat_pgscan_kswapd", "node_vmstat_pgscan_direct", "node_vmstat_pgscan_direct_throttle",
				"node_vmstat_zone_reclaim_failed", "node_vmstat_pginodesteal", "node_vmstat_slabs_scanned",
				"node_vmstat_kswapd_inodesteal", "node_vmstat_kswapd_low_wmark_hit_quickly",
				"node_vmstat_kswapd_high_wmark_hit_quickly", "node_vmstat_pageoutrun", "node_vmstat_pgrotated",
				"node_vmstat_drop_pagecache", "node_vmstat_drop_slab", "node_vmstat_oom_kill", "node_vmstat_numa_pte_updates",
				"node_vmstat_numa_huge_pte_updates", "node_vmstat_numa_hint_faults", "node_vmstat_numa_hint_faults_local",
				"node_vmstat_numa_pages_migrated", "node_vmstat_pgmigrate_success", "node_vmstat_pgmigrate_fail",
				"node_vmstat_compact_migrate_scanned", "node_vmstat_compact_free_scanned", "node_vmstat_compact_isolated",
				"node_vmstat_compact_stall", "node_vmstat_compact_fail", "node_vmstat_compact_success",
				"node_vmstat_compact_daemon_wake", "node_vmstat_compact_daemon_migrate_scanned",
				"node_vmstat_compact_daemon_free_scanned", "node_vmstat_htlb_buddy_alloc_success",
				"node_vmstat_htlb_buddy_alloc_fail", "node_vmstat_unevictable_pgs_culled",
				"node_vmstat_unevictable_pgs_scanned", "node_vmstat_unevictable_pgs_rescued",
				"node_vmstat_unevictable_pgs_mlocked", "node_vmstat_unevictable_pgs_munlocked",
				"node_vmstat_unevictable_pgs_cleared", "node_vmstat_unevictable_pgs_stranded",
				"node_vmstat_thp_fault_alloc", "node_vmstat_thp_fault_fallback", "node_vmstat_thp_collapse_alloc",
				"node_vmstat_thp_collapse_alloc_failed", "node_vmstat_thp_file_alloc", "node_vmstat_thp_file_mapped",
				"node_vmstat_thp_split_page", "node_vmstat_thp_split_page_failed", "node_vmstat_thp_deferred_split_page",
				"node_vmstat_thp_split_pmd", "node_vmstat_thp_split_pud", "node_vmstat_thp_zero_page_alloc",
				"node_vmstat_thp_zero_page_alloc_failed", "node_vmstat_thp_swpout", "node_vmstat_thp_swpout_fallback",
				"node_vmstat_thp_migration_fail", "node_vmstat_workingset_restore_anon", "node_vmstat_workingset_activate_file",
				"node_vmstat_workingset_refault_file", "node_vmstat_thp_migration_split", "node_vmstat_workingset_refault_anon",
				"node_vmstat_pgreuse", "node_vmstat_thp_migration_success", "node_vmstat_workingset_activate_anon",
				"node_vmstat_workingset_restore_file", "node_vmstat_balloon_inflate", "node_vmstat_balloon_deflate",
				"node_vmstat_balloon_migrate", "node_vmstat_swap_ra", "node_vmstat_swap_ra_hit", "node_vmstat_nr_foll_pin_acquired",
				"node_vmstat_pgsteal_anon", "node_vmstat_pgsteal_file", "node_vmstat_pgscan_file", "node_vmstat_pgscan_anon",
				"node_vmstat_thp_file_fallback_charge", "node_vmstat_nr_foll_pin_released", "node_vmstat_thp_file_fallback",
				"node_vmstat_thp_fault_fallback_charge", "node_vmstat_nr_swapcached", "node_vmstat_direct_map_level2_splits",
				"node_vmstat_direct_map_level3_splits", "node_vmstat_workingset_refault", "node_vmstat_workingset_activate",
				"node_vmstat_workingset_restore", "node_vmstat_pgdemote_kswapd", "node_vmstat_pgdemote_direct",
				"node_vmstat_pgsteal_khugepaged", "node_vmstat_pgskip_device", "node_vmstat_pgdemote_khugepaged",
				"node_vmstat_thp_scan_exceed_none_pte", "node_vmstat_nr_throttled_written", "node_vmstat_thp_scan_exceed_share_pte",
				"node_vmstat_pgpromote_candidate", "node_vmstat_pgscan_khugepaged", "node_vmstat_zswpin",
				"node_vmstat_thp_scan_exceed_swap_pte", "node_vmstat_allocstall_device", "node_vmstat_pgpromote_success",
				"node_vmstat_pgalloc_device", "node_vmstat_nr_sec_page_table_pages", "node_vmstat_cow_ksm",
				"node_vmstat_ksm_swpin_copy", "node_vmstat_zswpout", "node_vmstat_nr_unaccepted", "node_vmstat_zswpwb",
			},
			collector: NewMeminfoCollector,
		}

		pipeline(t, input)
	}
}

func Test_getVmstatStats(t *testing.T) {
	if runtime.GOOS == "linux" {
		s, err := getVmstatStats()
		assert.NoError(t, err)
		assert.Greater(t, len(s), 0)
	}
}

func Test_parseVmstatStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/vmstat.golden"))
	assert.NoError(t, err)

	stats, err := parseVmstatStats(file)
	assert.NoError(t, err)

	wantStats := map[string]float64{
		"oom_kill":            10,
		"nr_zone_active_file": 1933629,
		"nr_unevictable":      24,
		"nr_writeback":        0,
		"pgactivate":          57995375,
	}

	for k, want := range wantStats {
		if got, ok := stats[k]; ok {
			assert.Equal(t, want, got)
		} else {
			assert.Fail(t, "not found")
		}
	}

	assert.NoError(t, file.Close())

	// test with invalid values
	file, err = os.Open(filepath.Clean("testdata/proc/vmstat.invalid.1"))
	assert.NoError(t, err)
	stats, err = parseVmstatStats(file)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(stats))
	assert.NoError(t, file.Close())

	// test with wrong number of fields
	file, err = os.Open(filepath.Clean("testdata/proc/vmstat.invalid.2"))
	assert.NoError(t, err)
	_, err = parseVmstatStats(file)
	assert.Error(t, err)
	assert.NoError(t, file.Close())

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/proc/netdev.golden"))
	assert.NoError(t, err)

	stats, err = parseVmstatStats(file)
	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.NoError(t, file.Close())
}
