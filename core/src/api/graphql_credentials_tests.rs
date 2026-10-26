//! Exercise the real child launcher with a harmless credential and an executable reporter.
use super::*;

#[tokio::test]
async fn graphql_child_receives_credentials_without_exposing_them_in_arguments() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("graphql-reporter");
    let report = directory.path().join("report.json");
    let sentinel = "postgresql://test_user:FAKE_PASSWORD_SENTINEL@127.0.0.1:1/test_db";
    let script = format!(
        "#!/usr/bin/env python3\nimport json, os, sys\nwith open({}, 'w') as output:\n    json.dump({{'arguments': sys.argv[1:], 'credential_matches': os.environ.get('RINDEXER_GRAPHQL_DATABASE_URL') == {}}}, output)\n",
        serde_json::to_string(report.to_str().unwrap()).unwrap(),
        serde_json::to_string(sentinel).unwrap(),
    );
    fs::write(&executable, script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let parent_before = env::var_os("RINDEXER_GRAPHQL_DATABASE_URL");
    let mut child = start_server(&executable, sentinel, "first,second", &43210, true, false, true)
        .await
        .unwrap();
    assert!(child.wait().unwrap().success());
    let report: Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
    let arguments = report["arguments"].as_array().unwrap();
    assert!(
        arguments
            .iter()
            .all(|argument| !argument.as_str().unwrap().contains("FAKE_PASSWORD_SENTINEL")),
        "database credentials must not appear in the child command line"
    );
    assert_eq!(
        report["credential_matches"], true,
        "the child must receive the URL supplied to this launch"
    );
    assert_eq!(
        report["arguments"],
        serde_json::json!(["first,second", "43210", "1000", "10000", "true", "false", "true"])
    );
    assert!(
        env::var_os("RINDEXER_GRAPHQL_DATABASE_URL") == parent_before,
        "launching a child must not modify the parent environment"
    );
}
