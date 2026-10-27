use super::check_postgres_connection;
use std::{env, fs, os::unix::fs::PermissionsExt};

#[tokio::test]
async fn invalid_database_configuration_is_not_certified_by_a_host_utility() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("pg_isready");
    let report = directory.path().join("arguments");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$RINDEXER_TEST_READINESS_ARGUMENTS\"\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    env::set_var("PATH", directory.path());
    env::set_var("RINDEXER_TEST_READINESS_ARGUMENTS", &report);
    env::set_var("DATABASE_URL", "not a postgres connection string");

    let result = check_postgres_connection("not a postgres connection string", 1);
    assert!(result.is_err(), "readiness must validate the configured database connection");
    assert!(
        !report.exists(),
        "database readiness must not delegate to a host pg_isready executable"
    );
}

#[tokio::test]
#[ignore = "requires the isolated RINDEXER_TEST_POSTGRES_URL fixture"]
async fn ready_postgres_does_not_require_host_pg_isready() {
    let url = env::var("RINDEXER_TEST_POSTGRES_URL").expect("explicit isolated PostgreSQL fixture");
    let directory = tempfile::tempdir().unwrap();
    env::set_var("PATH", directory.path());
    env::set_var("DATABASE_URL", &url);

    check_postgres_connection(&url, 1)
        .expect("a ready configured database must work without a host pg_isready utility");
}
