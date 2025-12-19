#![allow(dead_code)]

use crate::sstable::{SSTable, SSTableBuilder};
use crate::manifest::Manifest;
use bytes::Bytes;
use crabstash_common::{Key, Result};
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::path::Path;

const LEVEL0_COMPACTION_TRIGGER: usize = 4;
const LEVEL_SIZE_MULTIPLIER: u64 = 10;
const BASE_LEVEL_SIZE: u64 = 10 * 1024 * 1024;

struct MergeEntry {
    key: Key,
    value: Option<Bytes>,
    sst_idx: usize,
}

impl Eq for MergeEntry {}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key.cmp(&self.key)
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct CompactionTask {
    pub level: u32,
    pub input_ssts: Vec<u64>,
    pub output_level: u32,
}

pub struct Compactor {
    dir: std::path::PathBuf,
}

impl Compactor {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
        }
    }

    pub fn pick_compaction(&self, manifest: &Manifest) -> Option<CompactionTask> {
        if let Some(level0) = manifest.levels.get(&0)
            && level0.sst_ids.len() >= LEVEL0_COMPACTION_TRIGGER
        {
            return Some(CompactionTask {
                level: 0,
                input_ssts: level0.sst_ids.clone(),
                output_level: 1,
            });
        }

        for level in 1..7 {
            if let Some(level_meta) = manifest.levels.get(&level) {
                let level_size: u64 = level_meta.sst_ids.len() as u64 * BASE_LEVEL_SIZE;
                let max_size = BASE_LEVEL_SIZE * LEVEL_SIZE_MULTIPLIER.pow(level);

                if level_size > max_size {
                    return Some(CompactionTask {
                        level,
                        input_ssts: vec![level_meta.sst_ids[0]],
                        output_level: level + 1,
                    });
                }
            }
        }

        None
    }

    pub fn compact(
        &self,
        task: &CompactionTask,
        manifest: &mut Manifest,
    ) -> Result<Vec<u64>> {
        let ssts: Vec<SSTable> = task.input_ssts
            .iter()
            .map(|&id| SSTable::open(id, self.dir.join(format!("{:06}.sst", id))))
            .collect::<Result<_>>()?;

        let new_sst_id = manifest.allocate_sst_id();
        let total_keys: usize = ssts.iter().map(|_| 1000).sum();
        let mut builder = SSTableBuilder::new(new_sst_id, &self.dir, total_keys)?;

        let mut heap: BinaryHeap<MergeEntry> = BinaryHeap::new();
        for (idx, sst) in ssts.iter().enumerate() {
            // TODO: implement iterator for SSTable
            let _ = idx;
            let _ = sst;
        }

        while let Some(entry) = heap.pop() {
            while heap.peek().map(|e: &MergeEntry| e.key.data() == entry.key.data()).unwrap_or(false) {
                heap.pop();
            }

            builder.add(&entry.key, entry.value.as_ref())?;
        }

        builder.finish()?;

        for &id in &task.input_ssts {
            manifest.remove_sst(task.level, id)?;
            let path = self.dir.join(format!("{:06}.sst", id));
            std::fs::remove_file(path).ok();
        }

        manifest.add_sst(task.output_level, new_sst_id)?;

        Ok(vec![new_sst_id])
    }
}
