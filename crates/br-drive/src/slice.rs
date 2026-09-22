#[macro_export]
macro_rules! drive_slice {
    (prefix = $prefix:ident ; principal = $p:ty) => {
        ::service_engine::pastey::paste! {
            #[derive(::core::default::Default)]
            pub struct DriveQuery;

            #[::async_graphql::Object]
            impl DriveQuery {
                async fn [<$prefix _file>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::core::option::Option<$crate::DriveFile>> {
                    ::service_engine::Query::<$p>::new(ctx)?
                        .fetch_view::<$crate::DriveFiles<$p>>(&file_id)
                        .await
                }

                async fn [<$prefix _drive_files>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    drive_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::std::vec::Vec<$crate::DriveFile>> {
                    ::service_engine::Query::<$p>::new(ctx)?
                        .fetch_view_window::<$crate::DriveFiles<$p>>(
                            &$crate::DriveWindow::of(drive_id),
                        )
                        .await
                }

                async fn [<$prefix _file_access>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    name: ::core::option::Option<::std::string::String>,
                ) -> ::async_graphql::Result<::core::option::Option<::std::string::String>> {
                    let query = ::service_engine::Query::<$p>::new(ctx)?;
                    let ::core::option::Option::Some(file) =
                        query.fetch_view::<$crate::DriveFiles<$p>>(&file_id).await?
                    else {
                        return ::core::result::Result::Ok(::core::option::Option::None);
                    };
                    if let ::core::result::Result::Err(reason) = file
                        .affordances
                        .require(::service_engine::gate::ActionName::from_static(
                            $crate::DOWNLOAD_ACTION,
                        ))
                    {
                        return ::core::result::Result::Err(::service_engine::coded_error(
                            reason.code(),
                            "file access refused",
                        ));
                    }
                    if name.is_some() {
                        return ::core::result::Result::Ok(::core::option::Option::None);
                    }
                    ::core::result::Result::Ok(
                        query
                            .download::<$crate::DriveFiles<$p>>(
                                &file_id,
                                ::service_engine::BlobRef(file.source),
                                ::service_engine::blobs::Disposition::Attachment,
                            )
                            .await?
                            .map(|url| url.into_string()),
                    )
                }
            }

            #[derive(::core::default::Default)]
            pub struct DriveMutation;

            #[::async_graphql::Object]
            impl DriveMutation {
                #[allow(clippy::too_many_arguments)]
                async fn [<$prefix _request_upload>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    drive_id: ::uuid::Uuid,
                    path: ::std::string::String,
                    name: ::std::string::String,
                    media_type: ::std::string::String,
                    size: u64,
                    sha256: ::std::string::String,
                ) -> ::async_graphql::Result<::service_engine::JsonScalar> {
                    let ticket = ::service_engine::execute::<$p, $crate::RequestUpload>(
                        ctx,
                        $crate::RequestUpload {
                            file_id,
                            drive_id,
                            path,
                            name,
                            media_type,
                            size,
                            sha256_hex: sha256,
                        },
                    )
                    .await?
                    .into_inner();
                    let (url, fields) = ticket.upload.into_parts();
                    let fields: ::serde_json::Map<::std::string::String, ::serde_json::Value> =
                        fields
                            .into_iter()
                            .map(|(k, v)| (k, ::serde_json::Value::String(v)))
                            .collect();
                    ::core::result::Result::Ok(::async_graphql::Json(::serde_json::json!({
                        "fileId": ticket.file_id,
                        "url": url,
                        "fields": fields,
                    })))
                }

                async fn [<$prefix _commit_upload>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::CommitUpload>(
                        ctx,
                        $crate::CommitUpload { file_id },
                    )
                    .await
                }

                async fn [<$prefix _update_file>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    name: ::core::option::Option<::std::string::String>,
                    path: ::core::option::Option<::std::string::String>,
                    drive_id: ::core::option::Option<::uuid::Uuid>,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::UpdateFile>(
                        ctx,
                        $crate::UpdateFile {
                            file_id,
                            name,
                            path,
                            drive_id,
                        },
                    )
                    .await
                }

                async fn [<$prefix _delete_file>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::DeleteFile>(
                        ctx,
                        $crate::DeleteFile { file_id },
                    )
                    .await
                }

                async fn [<$prefix _move_folder>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    drive_id: ::uuid::Uuid,
                    old_prefix: ::std::string::String,
                    new_prefix: ::std::string::String,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::MoveFolder>(
                        ctx,
                        $crate::MoveFolder {
                            drive_id,
                            old_prefix,
                            new_prefix,
                        },
                    )
                    .await
                }

                async fn [<$prefix _delete_folder>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    drive_id: ::uuid::Uuid,
                    prefix: ::std::string::String,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::DeleteFolder>(
                        ctx,
                        $crate::DeleteFolder { drive_id, prefix },
                    )
                    .await
                }
            }

            #[derive(::core::default::Default)]
            pub struct DriveSubscription;

            #[::async_graphql::Subscription]
            impl DriveSubscription {
                async fn [<$prefix _drive_changed>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    drive_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<
                    impl ::futures_util::Stream<
                        Item = ::async_graphql::Result<$crate::DriveDelta>,
                    >,
                > {
                    use ::futures_util::StreamExt;
                    let stream = ::service_engine::attach::<$p>(
                        ctx,
                        ::std::vec![::service_engine::session::WindowSpec::view::<
                            $crate::DriveFiles<$p>,
                        >(&$crate::DriveWindow::of(drive_id), false)?],
                    )
                    .await?;
                    ::core::result::Result::Ok(
                        stream.map(|delta| $crate::DriveDelta::from_delta::<$p>(&delta)),
                    )
                }
            }

            pub fn register(
                engine: &mut ::service_engine::Engine<$p>,
            ) -> ::core::result::Result<(), ::service_engine::error::EngineError> {
                $crate::register::<$p>(engine)?;
                engine.register_schema_slice(
                    ::service_engine::graphql::SliceFragment::derive::<
                        DriveQuery,
                        DriveMutation,
                        DriveSubscription,
                    >("drive"),
                )?;
                ::core::result::Result::Ok(())
            }
        }
    };
}
