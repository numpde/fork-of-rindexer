#[cfg(test)]
mod live_cron_supervision_regressions {
    use super::*;

    #[test]
    fn exhausted_live_cron_retries_are_terminal() {
        let error = report_exhausted_live_cron_retries(
            "samples",
            "test",
            MAX_RETRIES,
            "injected SQL failure",
        )
        .expect_err("exhausted retries cannot be logged and ignored");
        assert!(error.contains("samples") && error.contains("injected SQL failure"));
    }
}
