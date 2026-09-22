use std::process::Command;

#[test]
fn the_binary_prints_the_composed_sdl_without_touching_infra() {
    let output = Command::new(env!("CARGO_BIN_EXE_br-drive-example"))
        .arg("schema")
        .env_clear()
        .output()
        .expect("the example binary runs");
    assert!(
        output.status.success(),
        "schema exits zero: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sdl = String::from_utf8(output.stdout).expect("utf-8 sdl");
    assert!(
        sdl.contains("workspaceRequestUpload"),
        "the SDL carries the library slice under the host prefix: {sdl}"
    );
    assert!(
        sdl.contains("workspaceCreate"),
        "the SDL carries the host's own slice: {sdl}"
    );
}
