service_engine::compose_service! {
    principal = crate::kernel::AppPrincipal;
    prefix = workspace;
    slice workspace ["workspace"] { query = workspace::graphql::WorkspaceQuery, mutation = workspace::graphql::WorkspaceMutation, subscription = workspace::graphql::WorkspaceSubscription }
    slice drive ["drive"] from br_drive::drive_slice { query = drive::DriveQuery, mutation = drive::DriveMutation, subscription = drive::DriveSubscription }
}
