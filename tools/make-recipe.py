"""Bound a whole Make recipe while preserving cleanup, recursion and terminal input."""
import errno
import os
import re
import signal
import subprocess
import sys


def main():
    limit = os.environ.get('MAKE_TIMEOUT', '')
    if not re.fullmatch(r'[1-9][0-9]*[smh]?', limit):
        print(f'Cannot run Make recipe: MAKE_TIMEOUT must be a positive integer with an optional s, m or h suffix; received {limit!r}.', file=sys.stderr)
        return 2
    shell = ['/bin/bash', '-euo', 'pipefail', *sys.argv[1:]]
    # A nested timeout would create a separate process group that could survive
    # its caller's cancellation. Reuse the caller's timer and process group.
    if os.environ.get('RINDEXER_MAKE_TIMEOUT_ACTIVE') == '1':
        os.execv(shell[0], shell)
    environment = dict(os.environ, RINDEXER_MAKE_TIMEOUT_ACTIVE='1')
    terminal = sys.stdin.fileno()
    foreground = None
    if os.isatty(terminal):
        try:
            foreground = os.tcgetpgrp(terminal)
        except OSError as error:
            if error.errno != errno.ENOTTY:
                raise
    if foreground is not None:
        previous_ttou = signal.signal(signal.SIGTTOU, signal.SIG_IGN)

    def recipe_process_group():
        os.setpgrp()
        if foreground is not None:
            # Transfer the terminal before exec, so even an immediate CLI read
            # cannot stop the new process group as a background job.
            os.tcsetpgrp(terminal, os.getpgrp())

    try:
        try:
            job = subprocess.Popen(['timeout', '--verbose', '--signal=TERM', '--kill-after=60s', limit,
                                    *shell], env=environment, preexec_fn=recipe_process_group)
        except FileNotFoundError:
            print('Cannot run Make recipe: install GNU coreutils so the timeout command is available.', file=sys.stderr)
            return 127

        def forward(number, _frame):
            try:
                os.killpg(job.pid, number)
            except ProcessLookupError:
                pass  # The recipe can finish just before its caller sends a signal.

        for number in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
            signal.signal(number, forward)
        result = job.wait()
    finally:
        if foreground is not None:
            os.tcsetpgrp(terminal, foreground)
            signal.signal(signal.SIGTTOU, previous_ttou)
    return 128 - result if result < 0 else result



if __name__ == '__main__':
    sys.exit(main())
