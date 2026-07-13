fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("cumulus_descriptor.bin"))
        .compile_protos(
            &[
                "protos/cumulus/v1/objects.proto",
                "protos/cumulus/v1/ops.proto",
                "protos/cumulus/v1/service.proto",
            ],
            &["protos"],
        )?;
    Ok(())
}
