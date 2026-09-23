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

                async fn [<$prefix _pages>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::std::vec::Vec<$crate::DrivePage>> {
                    ::service_engine::Query::<$p>::new(ctx)?
                        .fetch_view_window::<$crate::DrivePages<$p>>(
                            &$crate::PageWindow::of(file_id),
                        )
                        .await
                }

                async fn [<$prefix _rulesets>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                ) -> ::async_graphql::Result<::std::vec::Vec<$crate::DriveRuleset>> {
                    ::service_engine::Query::<$p>::new(ctx)?
                        .fetch_view_window::<$crate::DriveRulesets<$p>>(
                            &::core::default::Default::default(),
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
                    let (reference, disposition) = match name {
                        ::core::option::Option::Some(name) => match file.image(&name) {
                            ::core::option::Option::Some(image) => (
                                image.source,
                                ::service_engine::blobs::Disposition::Inline,
                            ),
                            ::core::option::Option::None => {
                                return ::core::result::Result::Ok(::core::option::Option::None);
                            }
                        },
                        ::core::option::Option::None => (
                            file.source,
                            ::service_engine::blobs::Disposition::Attachment,
                        ),
                    };
                    ::core::result::Result::Ok(
                        query
                            .download::<$crate::DriveFiles<$p>>(
                                &file_id,
                                ::service_engine::BlobRef(reference),
                                disposition,
                            )
                            .await?
                            .map(|url| url.into_string()),
                    )
                }

                async fn [<$prefix _runner_context>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    job_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<$crate::RunnerContext> {
                    let principal = ctx.data::<$p>()?;
                    let state = ctx
                        .data::<::std::sync::Arc<::service_engine::GraphqlState<$p>>>()?;
                    let mut context = $crate::runner_context::<$p>(
                        state.pg(),
                        principal,
                        file_id,
                        job_id,
                    )
                    .await
                    .map_err($crate::DriveFault::into_graphql)?;
                    let url = ::service_engine::Query::<$p>::new(ctx)?
                        .download::<$crate::RunnerSources<$p>>(
                            &file_id,
                            ::service_engine::BlobRef(context.source),
                            ::service_engine::blobs::Disposition::Inline,
                        )
                        .await?;
                    let ::core::option::Option::Some(url) = url else {
                        return ::core::result::Result::Err(::service_engine::coded_error(
                            $crate::codes::SOURCE_NOT_AVAILABLE.code(),
                            "the source is not yet available for download",
                        ));
                    };
                    context.source_url = ::core::option::Option::Some(url.into_string());
                    ::core::result::Result::Ok(context)
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
                    size: $crate::ByteCount,
                    sha256: ::std::string::String,
                ) -> ::async_graphql::Result<$crate::UploadTicket> {
                    ::core::result::Result::Ok(
                        ::service_engine::execute::<$p, $crate::RequestUpload>(
                            ctx,
                            $crate::RequestUpload {
                                file_id,
                                drive_id,
                                path,
                                name,
                                media_type,
                                size: size.0,
                                sha256_hex: sha256,
                            },
                        )
                        .await?
                        .into_inner(),
                    )
                }

                async fn [<$prefix _commit_upload>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    ruleset_id: ::core::option::Option<::uuid::Uuid>,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::CommitUpload>(
                        ctx,
                        $crate::CommitUpload { file_id, ruleset_id },
                    )
                    .await
                }

                async fn [<$prefix _process>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    ruleset_id: ::core::option::Option<::uuid::Uuid>,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::Process>(
                        ctx,
                        $crate::Process { file_id, ruleset_id },
                    )
                    .await
                }

                async fn [<$prefix _regenerate_page>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    number: i32,
                    comment: ::core::option::Option<::std::string::String>,
                    ruleset_id: ::core::option::Option<::uuid::Uuid>,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::RegeneratePage>(
                        ctx,
                        $crate::RegeneratePage {
                            file_id,
                            number,
                            comment,
                            ruleset_id,
                        },
                    )
                    .await
                }

                #[allow(clippy::too_many_arguments)]
                async fn [<$prefix _create_ruleset>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    id: ::uuid::Uuid,
                    name: ::std::string::String,
                    trigger: $crate::Trigger,
                    media_types: ::std::vec::Vec<::std::string::String>,
                    steps: ::std::vec::Vec<$crate::RulesetStepInput>,
                    #[graphql(default = false)] is_default: bool,
                ) -> ::async_graphql::Result<$crate::RulesetSaved> {
                    ::core::result::Result::Ok(
                        ::service_engine::execute::<$p, $crate::CreateRuleset>(
                            ctx,
                            $crate::CreateRuleset {
                                id,
                                name,
                                trigger,
                                media_types,
                                steps: steps.into_iter().map(::core::convert::Into::into).collect(),
                                is_default,
                            },
                        )
                        .await?
                        .into_inner(),
                    )
                }

                async fn [<$prefix _update_ruleset>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    id: ::uuid::Uuid,
                    name: ::core::option::Option<::std::string::String>,
                    media_types: ::core::option::Option<::std::vec::Vec<::std::string::String>>,
                    steps: ::core::option::Option<::std::vec::Vec<$crate::RulesetStepInput>>,
                    is_default: ::core::option::Option<bool>,
                ) -> ::async_graphql::Result<$crate::RulesetSaved> {
                    ::core::result::Result::Ok(
                        ::service_engine::execute::<$p, $crate::UpdateRuleset>(
                            ctx,
                            $crate::UpdateRuleset {
                                id,
                                name,
                                media_types,
                                steps: steps.map(|steps| {
                                    steps.into_iter().map(::core::convert::Into::into).collect()
                                }),
                                is_default,
                            },
                        )
                        .await?
                        .into_inner(),
                    )
                }

                async fn [<$prefix _delete_ruleset>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::DeleteRuleset>(
                        ctx,
                        $crate::DeleteRuleset { id },
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

                async fn [<$prefix _edit_page>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    number: i32,
                    markdown: ::std::string::String,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::EditPage>(
                        ctx,
                        $crate::EditPage {
                            file_id,
                            number,
                            markdown,
                        },
                    )
                    .await
                }

                #[allow(clippy::too_many_arguments)]
                async fn [<$prefix _runner_request_image_upload>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    job_id: ::uuid::Uuid,
                    name: ::std::string::String,
                    media_type: ::std::string::String,
                    size: $crate::ByteCount,
                    sha256: ::std::string::String,
                ) -> ::async_graphql::Result<$crate::UploadTicket> {
                    ::core::result::Result::Ok(
                        ::service_engine::execute::<$p, $crate::RunnerRequestImageUpload>(
                            ctx,
                            $crate::RunnerRequestImageUpload {
                                file_id,
                                job_id,
                                name,
                                media_type,
                                size: size.0,
                                sha256_hex: sha256,
                            },
                        )
                        .await?
                        .into_inner(),
                    )
                }

                #[allow(clippy::too_many_arguments)]
                async fn [<$prefix _runner_report>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                    job_id: ::uuid::Uuid,
                    #[graphql(default)] pages: ::std::vec::Vec<$crate::ReportedPageInput>,
                    origin: ::core::option::Option<$crate::PageOrigin>,
                    summary: ::core::option::Option<::std::string::String>,
                    page_count: ::core::option::Option<i32>,
                    estimated_tokens: ::core::option::Option<i64>,
                    #[graphql(default = false)] done: bool,
                ) -> ::async_graphql::Result<::service_engine::MutationAck> {
                    ::service_engine::ack::<$p, $crate::RunnerReport>(
                        ctx,
                        $crate::RunnerReport {
                            file_id,
                            job_id,
                            pages: pages.into_iter().map(::core::convert::Into::into).collect(),
                            origin: origin.unwrap_or_default(),
                            summary,
                            page_count,
                            estimated_tokens,
                            done,
                        },
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
                    ::service_engine::ack_bulk::<$p, $crate::MoveFolder>(
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
                    ::service_engine::ack_bulk::<$p, $crate::DeleteFolder>(
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

                async fn [<$prefix _file_pages>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    file_id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<
                    impl ::futures_util::Stream<
                        Item = ::async_graphql::Result<$crate::DriveDelta>,
                    >,
                > {
                    use ::futures_util::StreamExt;
                    let stream = ::service_engine::attach::<$p>(
                        ctx,
                        ::std::vec![::service_engine::session::WindowSpec::view::<
                            $crate::DrivePages<$p>,
                        >(&$crate::PageWindow::of(file_id), false)?],
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
                $crate::register::<$p>(engine, ::core::stringify!($prefix))?;
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
