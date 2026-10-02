use std::time::Duration;

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::gate::Gate;
use service_engine::impact::Deps;
use service_engine::pipeline::Ops;
use service_engine::principal::Principal;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::erase::EraseMode;
use crate::facts::DriveFact;
use crate::file::FileRow;
use crate::media::MediaType;
use crate::owner::DriveOwnerNoun;
use crate::path::{DrivePath, FileName};

pub const DRIVE_DIM: &str = "drive";
pub const SCOPES_CLAIM: &str = "scopes";

pub enum DriveRequest<'a, H> {
    CreateFile {
        drive: Uuid,
        path: &'a DrivePath,
        name: &'a FileName,
        media_type: &'a MediaType,
        size: u64,
    },
    /// Confirming a pending upload (`<p>CommitUpload`): the row carries its
    /// uploader (`created_by`), so a host can reserve the commit to them.
    CommitUpload {
        file: &'a FileRow<H>,
    },
    ReadFile {
        file: &'a FileRow<H>,
    },
    UpdateFile {
        file: &'a FileRow<H>,
        target_drive: Uuid,
    },
    DeleteFile {
        file: &'a FileRow<H>,
    },
    /// Changing a file's title; never its name, path or drive.
    RetitleFile {
        file: &'a FileRow<H>,
    },
    MoveFolder {
        drive: Uuid,
        old_prefix: &'a DrivePath,
        new_prefix: &'a DrivePath,
    },
    DeleteFolder {
        drive: Uuid,
        prefix: &'a DrivePath,
    },
    /// Starting a chain on a READY or FAILED file (`<p>ProcessFile`), its first
    /// processing included: a commit never processes.
    Process {
        file: &'a FileRow<H>,
    },
    /// Cancelling the job running on a PROCESSING file (`<p>CancelProcessing`).
    CancelProcessing {
        file: &'a FileRow<H>,
    },
    EditPage {
        file: &'a FileRow<H>,
    },
    RegeneratePage {
        file: &'a FileRow<H>,
        number: i32,
    },
    ManageRulesets,
    ReadRulesets,
    ManageLabels,
    SetFileLabels {
        file: &'a FileRow<H>,
    },
    /// Reading the host's label catalogue, live included.
    ReadLabels,
    /// Writing a file's free `metadata` through `br_drive::set_metadata`.
    SetMetadata {
        file: &'a FileRow<H>,
    },
}

impl<H> DriveRequest<'_, H> {
    pub fn drive(&self) -> Option<Uuid> {
        match self {
            Self::CreateFile { drive, .. }
            | Self::MoveFolder { drive, .. }
            | Self::DeleteFolder { drive, .. } => Some(*drive),
            Self::CommitUpload { file }
            | Self::ReadFile { file }
            | Self::SetMetadata { file }
            | Self::UpdateFile { file, .. }
            | Self::DeleteFile { file }
            | Self::RetitleFile { file }
            | Self::Process { file }
            | Self::CancelProcessing { file }
            | Self::EditPage { file }
            | Self::RegeneratePage { file, .. }
            | Self::SetFileLabels { file } => Some(file.drive_id),
            Self::ManageRulesets | Self::ReadRulesets | Self::ManageLabels | Self::ReadLabels => {
                None
            }
        }
    }
}

pub trait DriveHost: Principal {
    const SERVICE: &'static str;

    const RUNNER_SCOPE: &'static str;

    const VISIBILITY_DEPS: Deps = Deps::ALL;

    const SOURCE_MAX_BYTES: u64 = 1 << 30;

    const SOURCE_ORPHAN_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

    const BULK_RESET_THRESHOLD: usize = 256;

    const IMAGE_MAX_BYTES: u64 = 64 << 20;

    const IMAGE_ORPHAN_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

    /// The host's own noun whose objects are keyed by the drive's id (a drive
    /// is created from its `DriveOwnerNoun::Object` and takes its key), or
    /// `br_drive::NoDriveOwner`. Every file change the library stages also
    /// impacts that key, so a host view bound to its own noun — an object
    /// showing file counts, say — recomputes and republishes. The noun is a
    /// type: its name and its UUID key are checked by the compiler.
    type DriveOwner: DriveOwnerNoun;

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate;

    /// Keeps `facts` in the host's fact table, on `conn` — the transaction of
    /// the gesture that changed the library's state: insert, never commit.
    /// Every change of a library state row reaches this hook, in `seq` order
    /// per `(noun, key)`, gap-free. An error rolls the whole gesture back. The
    /// README gives the reference table a host is expected to create.
    fn record_facts<'a>(
        conn: &'a mut PgConnection,
        facts: &'a [DriveFact],
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    fn visible_drives(&self) -> Vec<Uuid>;

    /// The host's caller admission: whether this principal may use the drive
    /// slice at all. Every GraphQL root of the slice asks it FIRST, before it
    /// looks anything up — the file, page, file access, drive files, label,
    /// ruleset and runner type queries, every non-runner mutation, and (through
    /// [`DriveHost::admit_subscription`]) the four subscriptions. A blocked gate
    /// refuses the root with its reason as the error's `code`; nothing is
    /// looked up, recorded or attached, so the answer is the same whether the
    /// id the caller named exists or not.
    ///
    /// The gateway authenticates; admitting is the host's job, read from its
    /// own data — a person deactivated or unknown in its roster, a service
    /// account on a surface meant for people, a person without the host's
    /// access scope. The default admits everyone, the behaviour before the
    /// hook existed.
    ///
    /// The runner operations (`<p>RunnerContext`, `<p>RunnerReport`,
    /// `<p>RunnerReportFailure`, `<p>RunnerRequestImageUpload`) do not ask it:
    /// they are the runner's surface, refused to any caller without the host's
    /// [`DriveHost::RUNNER_SCOPE`] (`RUNNER_SCOPE_REQUIRED`) before any lookup,
    /// so a host that refuses service accounts here keeps its runners working.
    fn admit(&self) -> Gate {
        Gate::allowed()
    }

    /// The admission asked at the open of a subscription of the drive slice
    /// (`<p>DriveChanged`, `<p>FilePages`, `<p>LabelsChanged`,
    /// `<p>RulesetsChanged`), before the stream is attached. A blocked gate
    /// refuses the subscription at open with its reason as the error's `code`,
    /// and no stream is attached.
    ///
    /// Kept for hosts written against 0.6.1; its default is
    /// [`DriveHost::admit`], so a host implements `admit` only and every root,
    /// subscriptions included, gets the same answer. A host that overrides it
    /// replaces the subscription admission with its own.
    #[deprecated(
        since = "0.6.2",
        note = "implement `DriveHost::admit`, which every root of the drive slice asks; \
                `admit_subscription` defaults to it"
    )]
    fn admit_subscription(&self) -> Gate {
        self.admit()
    }

    fn is_runner(&self) -> bool {
        let passport = self.passport();
        passport.service_account_id().is_some()
            && passport
                .claim::<Vec<String>>(SCOPES_CLAIM)
                .is_some_and(|scopes| scopes.iter().any(|scope| scope == Self::RUNNER_SCOPE))
    }

    fn display_name(&self) -> Option<String> {
        None
    }

    /// What the engine's erase pipeline does with the rows a person created.
    fn erase_mode() -> EraseMode {
        EraseMode::Anonymise
    }

    fn upload_window(&self) -> Duration {
        Duration::from_secs(15 * 60)
    }

    /// Whether `CommitUpload` starts the chain of the file's default `upload`
    /// rule in its own transaction (default `true`): a file with a matching
    /// rule goes from PENDING to PROCESSING, never READY in between; one with
    /// none stays READY (stored only). `false` keeps the two-gesture flow —
    /// the commit stores, `ProcessFile` processes.
    fn process_on_commit(&self) -> bool {
        true
    }

    fn folder_moved<'a, 'o>(
        _ops: &'a mut Ops<'o>,
        _drive: Uuid,
        _old_prefix: &'a DrivePath,
        _new_prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async { Ok(()) })
    }

    fn folder_deleted<'a, 'o>(
        _ops: &'a mut Ops<'o>,
        _drive: Uuid,
        _prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use br_core_auth::{AuthMethod, Passport, PassportClaims};
    use service_engine::gate::Reason;
    use service_engine::principal::PrincipalId;

    use super::*;
    use crate::owner::NoDriveOwner;

    /// A host that implements only what the trait requires.
    #[derive(Clone)]
    struct BareHost {
        id: PrincipalId,
        passport: Passport,
    }

    impl BareHost {
        fn deactivated() -> Self {
            let user = Uuid::now_v7();
            Self {
                id: PrincipalId::from(user),
                passport: Passport::human(
                    user,
                    false,
                    false,
                    AuthMethod::Jwt,
                    None,
                    PassportClaims::new(),
                ),
            }
        }
    }

    impl Principal for BareHost {
        fn id(&self) -> PrincipalId {
            self.id
        }

        fn passport(&self) -> &Passport {
            &self.passport
        }
    }

    impl DriveHost for BareHost {
        const SERVICE: &'static str = "bare";
        const RUNNER_SCOPE: &'static str = "bare:runner";
        type DriveOwner = NoDriveOwner;

        fn drive_gate(&self, _request: &DriveRequest<'_, Self>) -> Gate {
            Gate::allowed()
        }

        fn record_facts<'a>(
            _conn: &'a mut PgConnection,
            _facts: &'a [DriveFact],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }

        fn visible_drives(&self) -> Vec<Uuid> {
            Vec::new()
        }
    }

    /// A 0.6.1 host: it implements only the subscription admission. It builds
    /// under `-D warnings` with no `allow`: implementing a deprecated method
    /// is not a use of it, so the deprecation warns no 0.6.1 host.
    #[derive(Clone)]
    struct SubscriptionOnlyHost(BareHost);

    impl Principal for SubscriptionOnlyHost {
        fn id(&self) -> PrincipalId {
            self.0.id
        }

        fn passport(&self) -> &Passport {
            &self.0.passport
        }
    }

    const CLOSED: Reason = Reason::new("CLOSED");

    impl DriveHost for SubscriptionOnlyHost {
        const SERVICE: &'static str = "subscription-only";
        const RUNNER_SCOPE: &'static str = "subscription-only:runner";
        type DriveOwner = NoDriveOwner;

        fn drive_gate(&self, _request: &DriveRequest<'_, Self>) -> Gate {
            Gate::allowed()
        }

        fn record_facts<'a>(
            _conn: &'a mut PgConnection,
            _facts: &'a [DriveFact],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }

        fn visible_drives(&self) -> Vec<Uuid> {
            Vec::new()
        }

        fn admit_subscription(&self) -> Gate {
            Gate::blocked(CLOSED)
        }
    }

    /// A host that implements only `admit`.
    #[derive(Clone)]
    struct AdmitOnlyHost(BareHost);

    impl Principal for AdmitOnlyHost {
        fn id(&self) -> PrincipalId {
            self.0.id
        }

        fn passport(&self) -> &Passport {
            &self.0.passport
        }
    }

    impl DriveHost for AdmitOnlyHost {
        const SERVICE: &'static str = "admit-only";
        const RUNNER_SCOPE: &'static str = "admit-only:runner";
        type DriveOwner = NoDriveOwner;

        fn drive_gate(&self, _request: &DriveRequest<'_, Self>) -> Gate {
            Gate::allowed()
        }

        fn record_facts<'a>(
            _conn: &'a mut PgConnection,
            _facts: &'a [DriveFact],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }

        fn visible_drives(&self) -> Vec<Uuid> {
            Vec::new()
        }

        fn admit(&self) -> Gate {
            Gate::blocked(CLOSED)
        }
    }

    #[test]
    fn a_host_that_does_not_implement_admission_admits_every_caller() {
        assert_eq!(BareHost::deactivated().admit(), Gate::allowed());
    }

    #[test]
    #[allow(deprecated)]
    fn a_host_that_does_not_implement_admission_admits_every_subscription() {
        assert_eq!(
            BareHost::deactivated().admit_subscription(),
            Gate::allowed()
        );
    }

    #[test]
    #[allow(deprecated)]
    fn the_subscription_admission_defaults_to_the_host_admission() {
        let host = AdmitOnlyHost(BareHost::deactivated());
        assert_eq!(host.admit(), Gate::blocked(CLOSED));
        assert_eq!(host.admit_subscription(), Gate::blocked(CLOSED));
    }

    #[test]
    #[allow(deprecated)]
    fn a_host_written_for_0_6_1_still_refuses_subscriptions_and_admits_the_rest() {
        let host = SubscriptionOnlyHost(BareHost::deactivated());
        assert_eq!(host.admit_subscription(), Gate::blocked(CLOSED));
        assert_eq!(host.admit(), Gate::allowed());
    }
}
