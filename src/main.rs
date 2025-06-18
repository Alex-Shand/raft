#[tokio::main]
async fn main() {
    // SAFETY: This is the first thing the program does, there is nothing
    // interesting happening at this point
    unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    match raft::main(argh::from_env()).await {
        Ok(()) => (),
        Err(e) => {
            eprintln!("{:?}", e);
            std::process::exit(1);
        }
    }
}
