fn main() -> Result<(), Box<dyn std::error::Error>>
{
    tonic_build::compile_protos("./protos/fs_walker_svc.proto")?;
    Ok(())
}
