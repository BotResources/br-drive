#[macro_export]
macro_rules! drive_slice {
    (prefix = $prefix:ident ; principal = $p:ty) => {
        ::service_engine::pastey::paste! {
            #[derive(::core::default::Default)]
            pub struct DriveQuery;

            #[::async_graphql::Object]
            impl DriveQuery {
                async fn [<$prefix _drive_version>](&self) -> &'static str {
                    $crate::VERSION
                }
            }

            pub fn register(
                engine: &mut ::service_engine::Engine<$p>,
            ) -> ::core::result::Result<(), ::service_engine::error::EngineError> {
                fn host_bound<H: $crate::DriveHost>() {}
                host_bound::<$p>();
                engine.register_schema_slice(
                    ::service_engine::graphql::SliceFragment::derive::<
                        DriveQuery,
                        ::async_graphql::EmptyMutation,
                        ::async_graphql::EmptySubscription,
                    >("drive"),
                )?;
                ::core::result::Result::Ok(())
            }
        }
    };
}
