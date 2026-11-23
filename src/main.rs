use std::{io::stdin};
use std::collections::VecDeque;
use walkdir::WalkDir;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use fs_walker_svc::{
    FindFirstRequest, FindResponse, FsItem, FsDirContents, fs_item,
    fs_walker_server::{FsWalker, FsWalkerServer},
    fs_walker_client::FsWalkerClient
};
use clap::{Parser, Subcommand};

mod fs_reader;

#[derive(Debug, Clone, Subcommand)]
enum Mode {
    Server {
        #[arg(long, short, default_value_t = 7541)]
        port: u32,
    },
    Client {
        #[arg(long, short, default_value_t = 7541)]
        port: u32,
    },
    Test {
        #[arg(long, short)]
        root_path: String,

        #[arg(long, short, default_value_t = 32)]
        num_parallel: u32,
    }
}

#[derive(Parser, Debug)]
#[command(version, about = "Filesystem walker client and server")]
struct Args {
    #[clap(subcommand)]
    mode: Mode,
}

pub mod fs_walker_svc {
    tonic::include_proto!("fs_walker_svc");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    // console_subscriber::init();

    let args = Args::parse();
    match args.mode {
        Mode::Server{port} => {
            let addr = format!("[::1]:{}", port);
            let addr = addr.parse().unwrap();
            println!("Starting server at addr {}...", &addr);
            let svc = FsWalkerService::default();

            Server::builder().add_service(FsWalkerServer::new(svc))
                .serve(addr).
                await?;
            Ok(())
        },
        Mode::Client{port} => {
            let addr = format!("http://[::1]:{}", port);
            println!("Starting client at addr {}...", &addr);
            tokio::spawn(async {
                run_test_client(addr).await;
            }).await?;
            Ok(())
        },
        Mode::Test{root_path, num_parallel} => {
            let mut reader = fs_reader::FsReader::new(num_parallel, &root_path);
            reader.run_read().await?;
            Ok(())
        },
    }
}

async fn run_test_client(addr: String)
{
    let mut client = FsWalkerClient::connect(String::from("http://") + &addr).await.unwrap();
    
    loop {
        let mut dirname = String::new();
        println!("Enter dir to walk: ");
        stdin().read_line(&mut dirname).unwrap();
        let dirname = dirname.trim();

        let req = Request::new(FindFirstRequest {
            rootdir: dirname.to_string(),
        });
        let mut stream = client.find_first(req).await.unwrap().into_inner();
        while let Ok(Some(resp)) = stream.message().await {
            println!("{:?}", resp);
        }
    }
}

#[derive(Debug, Default)]
pub struct FsWalkerService {}

#[tonic::async_trait]
impl FsWalker for FsWalkerService
{
    type FindFirstStream = ReceiverStream<Result<FindResponse, Status>>;

    async fn find_first(&self, req: Request<FindFirstRequest>)
        -> Result<Response<Self::FindFirstStream>, Status>
    {
        let req = req.into_inner();
        let (resp_tx, resp_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            // List of all the directories to read starting from the queried
            // root-dir and covering all sub-directories in the file-system
            // sub-tree under the root-dir.
            let mut dirs_found = VecDeque::new();
            dirs_found.push_back((0, String::from(req.rootdir)));

            // TODO:
            // This loop can be further parallelized by having a thread-pool
            // share the work of reading all the sub-directories within
            // the queried root-dir.
            let mut dir_count = 0;
            while let Some(item) = dirs_found.pop_front() {
                let dir_id = item.0;
                let dir_to_read = item.1;
                println!("Walking dir: {}", &dir_to_read);
                let mut dir_items = Vec::new();
                for entry in WalkDir::new(&dir_to_read).max_depth(1)
                    .into_iter()
                    .filter_map(Result::ok) {
                        if entry.depth() == 0 || entry.file_name().to_string_lossy().starts_with(".") {
                            continue;
                        }
                        match std::fs::metadata(&entry.path()) {
                            Ok(metadata) => {
                                println!("dirs_found: {:?},\n entry: {}, is_dir: {}",
                                         &dirs_found,
                                         entry.path().to_string_lossy(), metadata.is_dir());
                                if metadata.is_dir() {
                                    dir_count += 1;
                                    dirs_found.push_back((dir_count,
                                                         String::from(entry.path().to_string_lossy())));
                                    dir_items.push(FsItem {
                                        name: String::from(entry.file_name().to_string_lossy()),
                                        attr: Some(fs_item::Attr::DirId(dir_count))
                                    });
                                } else {
                                    dir_items.push(FsItem {
                                        name: String::from(entry.file_name().to_string_lossy()),
                                        attr: Some(fs_item::Attr::FileSize(metadata.len()))
                                    });
                                }
                            },
                            Err(e) => {
                                println!("Failed to walk_dir: {}", e);
                                resp_tx.send(Err(Status::new(tonic::Code::NotFound,
                                                             std::format!("Err: {e}")))).await.unwrap();
                                break;
                            }
                        }
                    }
                if dir_items.len() > 0 {
                    let mut dirs = Vec::new();
                    dirs.push(FsDirContents {
                        parent_id: dir_id,
                        items: dir_items,
                    });
                    if resp_tx.send(Ok(FindResponse {fs_items: dirs})).await.is_err() {
                        break;
                    }
                }
                // thread::sleep(time::Duration::from_secs(1));
            }
        });
        Ok(Response::new(ReceiverStream::new(resp_rx)))
    }
}
