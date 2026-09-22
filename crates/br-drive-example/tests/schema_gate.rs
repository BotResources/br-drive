use async_graphql::Schema;
use br_drive_example::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use service_engine::graphql::{RootPrefix, SchemaSlices, SliceFragment};

fn fragments() -> Vec<SliceFragment> {
    let mut fragments = Vec::new();

    #[cfg(feature = "workspace")]
    {
        use br_drive_example::slices::workspace::graphql::{
            WorkspaceMutation, WorkspaceQuery, WorkspaceSubscription,
        };
        fragments.push(SliceFragment::derive::<
            WorkspaceQuery,
            WorkspaceMutation,
            WorkspaceSubscription,
        >("workspace"));
    }

    #[cfg(feature = "drive")]
    {
        use async_graphql::{EmptyMutation, EmptySubscription};
        use br_drive_example::slices::drive::DriveQuery;
        fragments.push(SliceFragment::derive::<
            DriveQuery,
            EmptyMutation,
            EmptySubscription,
        >("drive"));
    }

    fragments
}

#[test]
fn the_composed_host_schema_is_fully_claimed_by_its_slice_fragments() {
    let sdl = Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .finish()
    .sdl();

    let prefix = RootPrefix::from_snake("workspace").expect("`workspace` is a valid root prefix");
    SchemaSlices::assemble(&fragments(), Some(&prefix))
        .expect("the host slices assemble without a collision")
        .verify(&sdl)
        .expect("every root field and object type the composed schema exposes is claimed");
}
