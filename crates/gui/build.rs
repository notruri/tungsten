use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=assets/icons/tungsten.ico");

    #[cfg(target_os = "windows")]
    {
        let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
        let icon_path = manifest_dir.join("assets/icons/tungsten.ico");
        let icon_path = icon_path.to_string_lossy().into_owned();

        let mut resource = winresource::WindowsResource::new();
        resource.set_icon(&icon_path);
        resource.compile()?;
    }

    Ok(())
}
