use async_graphql::Schema;

fn check(slice: &str, sdl: String) {
    let path = format!(
        "{}/src/slices/{}/schema.graphql",
        env!("CARGO_MANIFEST_DIR"),
        slice
    );
    if std::env::var("BLESS_SCHEMA_FRAGMENTS").is_ok() {
        std::fs::write(&path, &sdl).expect("write the per-slice SDL golden");
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("missing per-slice SDL at {path}: {error}; regenerate with BLESS_SCHEMA_FRAGMENTS=1")
    });
    assert_eq!(
        sdl, committed,
        "the composed per-slice SDL drifted from {path}; regenerate with BLESS_SCHEMA_FRAGMENTS=1"
    );
}

#[cfg(feature = "workspace")]
#[test]
fn workspace_slice_sdl_matches_its_committed_fragment() {
    use br_drive_example::slices::workspace::graphql::{
        WorkspaceMutation, WorkspaceQuery, WorkspaceSubscription,
    };
    check(
        "workspace",
        Schema::build(WorkspaceQuery, WorkspaceMutation, WorkspaceSubscription)
            .finish()
            .sdl(),
    );
}

#[cfg(feature = "drive")]
#[test]
fn drive_slice_sdl_matches_its_committed_fragment() {
    use br_drive_example::slices::drive::{DriveMutation, DriveQuery, DriveSubscription};
    check(
        "drive",
        Schema::build(DriveQuery, DriveMutation, DriveSubscription)
            .finish()
            .sdl(),
    );
}
