use crate::iterator::StorageIterator;
use crate::manifest::Manifest;
use crate::sstable::{SSTable, SSTableBuilder, SSTableIterator};
use crabstash_common::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const LEVEL0_COMPACTION_TRIGGER: usize = 4;
const LEVEL_SIZE_MULTIPLIER: u64 = 10;
const BASE_LEVEL_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub level: u32,
    pub input_ssts: Vec<u64>,
    pub output_level: u32,
}

pub struct Compactor {
    dir: PathBuf,
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
            let mut input_ssts = level0.sst_ids.clone();

            if let Some(level1) = manifest.levels.get(&1) {
                input_ssts.extend(level1.sst_ids.iter().copied());
            }

            return Some(CompactionTask {
                level: 0,
                input_ssts,
                output_level: 1,
            });
        }

        for level in 1..6 {
            if let Some(level_meta) = manifest.levels.get(&level) {
                let level_size: u64 = level_meta.sst_ids.len() as u64 * BASE_LEVEL_SIZE;
                let max_size = BASE_LEVEL_SIZE * LEVEL_SIZE_MULTIPLIER.pow(level);

                if level_size > max_size && !level_meta.sst_ids.is_empty() {
                    let input_id = level_meta.sst_ids[0];
                    let mut input_ssts = vec![input_id];

                    if let Some(next_level) = manifest.levels.get(&(level + 1)) {
                        let input_sst =
                            SSTable::open(input_id, self.dir.join(format!("{:06}.sst", input_id)));
                        if let Ok(sst) = input_sst {
                            for &next_id in &next_level.sst_ids {
                                let next_path = self.dir.join(format!("{:06}.sst", next_id));
                                if let Ok(next_sst) = SSTable::open(next_id, next_path)
                                    && Self::ranges_overlap(&sst, &next_sst)
                                {
                                    input_ssts.push(next_id);
                                }
                            }
                        }
                    }

                    return Some(CompactionTask {
                        level,
                        input_ssts,
                        output_level: level + 1,
                    });
                }
            }
        }

        None
    }

    fn ranges_overlap(a: &SSTable, b: &SSTable) -> bool {
        !(a.max_key < b.min_key || b.max_key < a.min_key)
    }

    pub fn compact(&self, task: &CompactionTask, manifest: &mut Manifest) -> Result<Vec<u64>> {
        if task.input_ssts.is_empty() {
            return Ok(vec![]);
        }

        let mut iterators: Vec<SSTableIterator> = Vec::new();
        for &id in &task.input_ssts {
            let path = self.dir.join(format!("{:06}.sst", id));
            let sst = Arc::new(SSTable::open(id, path)?);
            iterators.push(SSTableIterator::new(sst)?);
        }

        let mut merged = MergedIterator::new(iterators);

        let new_sst_id = manifest.allocate_sst_id();
        let estimated_keys = task.input_ssts.len() * 1000;
        let mut builder = SSTableBuilder::new(new_sst_id, &self.dir, estimated_keys)?;

        let mut last_key: Option<Vec<u8>> = None;
        while merged.is_valid() {
            let key = merged.key().clone();
            let value = merged.value().cloned();

            let dominated = last_key
                .as_ref()
                .is_some_and(|lk| lk.as_slice() == key.data());

            if !dominated {
                builder.add(&key, value.as_ref())?;
                last_key = Some(key.data().to_vec());
            }

            merged.advance()?;
        }

        builder.finish()?;

        for &id in &task.input_ssts {
            let level = if manifest
                .levels
                .get(&task.level)
                .is_some_and(|l| l.sst_ids.contains(&id))
            {
                task.level
            } else {
                task.output_level
            };
            manifest.remove_sst(level, id)?;
            let path = self.dir.join(format!("{:06}.sst", id));
            std::fs::remove_file(path).ok();
        }

        manifest.add_sst(task.output_level, new_sst_id)?;

        Ok(vec![new_sst_id])
    }
}

struct MergedIterator {
    iterators: Vec<SSTableIterator>,
}

impl MergedIterator {
    fn new(iterators: Vec<SSTableIterator>) -> Self {
        Self { iterators }
    }

    fn is_valid(&self) -> bool {
        self.iterators.iter().any(|it| it.is_valid())
    }

    fn current_idx(&self) -> Option<usize> {
        self.iterators
            .iter()
            .enumerate()
            .filter(|(_, it)| it.is_valid())
            .min_by(|(_, a), (_, b)| a.key().cmp(b.key()))
            .map(|(idx, _)| idx)
    }

    fn key(&self) -> &crabstash_common::Key {
        let idx = self.current_idx().unwrap();
        self.iterators[idx].key()
    }

    fn value(&self) -> Option<&bytes::Bytes> {
        let idx = self.current_idx().unwrap();
        self.iterators[idx].value()
    }

    fn advance(&mut self) -> Result<()> {
        if let Some(idx) = self.current_idx() {
            self.iterators[idx].next()?;
        }
        Ok(())
    }
}
