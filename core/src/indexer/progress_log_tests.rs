//! Progress words must survive the real tracing formatter without embedded ANSI codes.
use super::IndexingEventProgressStatus;
use std::{
    io,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn capture(ansi: bool, emit: impl FnOnce()) -> String {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = Capture(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_target(false)
        .with_ansi(ansi)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, emit);
    let rendered = bytes.lock().unwrap().clone();
    String::from_utf8(rendered).unwrap()
}

struct ForcedColor;
impl ForcedColor {
    fn new() -> Self {
        // Make source-side coloring deterministic even when CI has no terminal.
        colored::control::set_override(true);
        Self
    }
}
impl Drop for ForcedColor {
    fn drop(&mut self) {
        colored::control::unset_override();
    }
}

fn render_statuses(ansi: bool) -> String {
    let _color = ForcedColor::new();
    capture(ansi, || {
        for status in [
            IndexingEventProgressStatus::syncing_log(),
            IndexingEventProgressStatus::live_log(),
            IndexingEventProgressStatus::completed_log(),
        ] {
            tracing::warn!("stream - {} - block 100", status);
        }
    })
}

fn assert_plain_statuses(output: &str) {
    for word in ["SYNCING", "LIVE", "COMPLETED"] {
        assert!(
            output.contains(&format!("stream - {word} - block 100")),
            "status must be plain message text in rendered output: {output:?}"
        );
    }
    assert!(!output.contains(r"\x1b"), "status helpers must not inject escaped ANSI: {output:?}");
}

#[test]
fn progress_log_status_words_render_plainly_with_colored_levels() {
    let output = render_statuses(true);
    assert_plain_statuses(&output);
    assert!(output.contains('\x1b'), "the formatter should still color its level labels");
}

#[test]
fn progress_log_status_words_render_plainly_with_ansi_disabled() {
    let output = render_statuses(false);
    assert_plain_statuses(&output);
    assert!(!output.contains('\x1b'), "an ANSI-disabled writer must emit no escape bytes");
}

#[test]
fn progress_log_formatter_keeps_escaping_control_sequences_in_event_text() {
    for ansi in [false, true] {
        let output = capture(ansi, || tracing::warn!("payload: {}", "\x1b[31mexternal\x1b[0m"));
        assert!(
            output.contains(r"\x1b[31mexternal\x1b[0m"),
            "event text must retain the formatter's control-sequence escaping: {output:?}"
        );
        assert!(!output.contains("\x1b[31m"), "event text must not inject terminal commands");
    }
}
