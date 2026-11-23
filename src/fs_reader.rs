use futures::future::BoxFuture;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::VecDeque;
use tokio::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// A queue and a bunch of threads 
pub struct FsReader {
    num_parallel: u32,
    root_dir: String,
    poller: FuturesUnordered<BoxFuture<'static, tokio::io::Result<(Vec<PathBuf>, Duration)>>>,
}

impl FsReader {
    pub fn new(num_parallel: u32, root_dir: &str) -> FsReader {
        FsReader {
            num_parallel,
            root_dir: String::from(root_dir),
            poller: FuturesUnordered::<BoxFuture<'static, tokio::io::Result<(Vec<PathBuf>, Duration)>>>::new(),
        }
    }

    pub async fn timed_read(dir: PathBuf) -> tokio::io::Result<(Vec<PathBuf>, Duration)> {
        let t = Instant::now();
        let subdirs = Self::read_dir_entries(dir).await?;
        Ok((subdirs, t.elapsed()))
    }

    // Returns the paths of all the directory entries within `dir`
    pub async fn read_dir_entries(dir: PathBuf) -> tokio::io::Result<Vec<PathBuf>> {
        let mut dirents = Vec::new();
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            dirents.push(entry.path());
        }
        // println!("dir: {:?}, entries: {}", dirpath, dirents.len());
        Ok(dirents)
    }

    pub async fn run_read(&mut self) -> tokio::io::Result<()> {
        let t0 = Instant::now();
        let root_path = PathBuf::from(&self.root_dir);

        // Read out the entries from each finished `add_dir_entries` future, filling
        // these back into the poller to read their sub-dirs/files.
        let mut to_schedule = VecDeque::new();
        let mut poller_tokens = self.num_parallel;

        let mut peak = 0usize;
        let mut dirs_read = 0u64;
        let mut sum_dur = Duration::ZERO;

        to_schedule.push_back(root_path);
        loop {
            while poller_tokens > 0
            {
                match to_schedule.pop_front() {
                    Some(e) => {
                        self.poller.push(FsReader::timed_read(e).boxed());
                        poller_tokens -= 1;
                    },
                    None => { break; }
                }
            }
            // Collect the results, enqueuing the subdirs in the results back into
            // `to_schedule` queue and replenishing a token for each completed read_dir.
            match self.poller.next().await {
                Some(Ok((dirents, d))) => {
                    peak = peak.max(self.poller.len());
                    dirs_read += 1;
                    sum_dur += d;

                    for e in dirents {
                        // println!("de: {:?}", &e);
                        if e.is_dir() {
                            to_schedule.push_back(e);
                        }
                    }
                   if poller_tokens < self.num_parallel {
                        poller_tokens += 1;
                    }
                },
                Some(Err(e)) => {
                    println!("read_dir failed: {e}");
                   if poller_tokens < self.num_parallel {
                        poller_tokens += 1;
                    }
                }
                None => {
                    if to_schedule.is_empty() {
                        break;
                    }
                }
            }
        }

    let wall = t0.elapsed();
    eprintln!(
        "dirs_read={dirs_read} wall={wall:?} sum_dir_time={sum_dur:?} \
         avg_concurrency={:.2} peak_in_flight={peak} cap={}",
        sum_dur.as_secs_f64() / wall.as_secs_f64(),
        self.num_parallel,
    );
    Ok(())

    }
}
