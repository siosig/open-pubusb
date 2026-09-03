use protox::prost::Message;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("open_pubusb_descriptor.bin");

    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../third_party/googleapis");
    let protos = [
        proto_root.join("google/pubsub/v1/pubsub.proto"),
        proto_root.join("google/pubsub/v1/schema.proto"),
        proto_root.join("google/iam/v1/iam_policy.proto"),
        proto_root.join("google/iam/v1/policy.proto"),
    ];
    let includes = [proto_root.clone()];

    let file_descriptor_set = protox::compile(&protos, &includes)?;
    std::fs::write(&descriptor_path, file_descriptor_set.encode_to_vec())?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .file_descriptor_set_path(&descriptor_path)
        .skip_protoc_run()
        .compile_fds(file_descriptor_set.clone())?;

    pbjson_build::Builder::new()
        .register_descriptors(&file_descriptor_set.encode_to_vec())?
        .build(&[".google.pubsub"])?;

    Ok(())
}
