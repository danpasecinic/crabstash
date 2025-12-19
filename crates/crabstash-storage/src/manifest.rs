use crabstash_common::Result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct LevelMeta {
    #[allow(dead_code)]
    pub level: u32,
    pub sst_ids: Vec<u64>,
}

pub struct Manifest {
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
    pub levels: HashMap<u32, LevelMeta>,
    pub next_sst_id: u64,
    pub next_wal_id: u64,
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file,
            levels: HashMap::new(),
            next_sst_id: 1,
            next_wal_id: 1,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path.as_ref())?;

        let mut manifest = Self {
            path: path.as_ref().to_path_buf(),
            file: file.try_clone()?,
            levels: HashMap::new(),
            next_sst_id: 1,
            next_wal_id: 1,
        };

        manifest.load(file)?;
        Ok(manifest)
    }

    fn load(&mut self, file: File) -> Result<()> {
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split(':').collect();

            match parts.first() {
                Some(&"SST") => {
                    let level: u32 = parts[1].parse().unwrap();
                    let id: u64 = parts[2].parse().unwrap();
                    let entry = self.levels.entry(level).or_insert(LevelMeta {
                        level,
                        sst_ids: Vec::new(),
                    });
                    entry.sst_ids.push(id);
                    self.next_sst_id = self.next_sst_id.max(id + 1);
                }
                Some(&"DEL") => {
                    let level: u32 = parts[1].parse().unwrap();
                    let id: u64 = parts[2].parse().unwrap();
                    if let Some(entry) = self.levels.get_mut(&level) {
                        entry.sst_ids.retain(|&x| x != id);
                    }
                }
                Some(&"WAL") => {
                    let id: u64 = parts[1].parse().unwrap();
                    self.next_wal_id = self.next_wal_id.max(id + 1);
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn add_sst(&mut self, level: u32, id: u64) -> Result<()> {
        writeln!(self.file, "SST:{}:{}", level, id)?;
        self.file.sync_all()?;

        let entry = self.levels.entry(level).or_insert(LevelMeta {
            level,
            sst_ids: Vec::new(),
        });
        entry.sst_ids.push(id);
        self.next_sst_id = self.next_sst_id.max(id + 1);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn remove_sst(&mut self, level: u32, id: u64) -> Result<()> {
        writeln!(self.file, "DEL:{}:{}", level, id)?;
        self.file.sync_all()?;

        if let Some(entry) = self.levels.get_mut(&level) {
            entry.sst_ids.retain(|&x| x != id);
        }

        Ok(())
    }

    pub fn add_wal(&mut self, id: u64) -> Result<()> {
        writeln!(self.file, "WAL:{}", id)?;
        self.file.sync_all()?;
        self.next_wal_id = self.next_wal_id.max(id + 1);
        Ok(())
    }

    pub fn allocate_sst_id(&mut self) -> u64 {
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        id
    }

    pub fn allocate_wal_id(&mut self) -> u64 {
        let id = self.next_wal_id;
        self.next_wal_id += 1;
        id
    }
}
