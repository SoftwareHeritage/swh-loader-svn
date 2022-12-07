# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import logging

from subvertpy import SubversionException
from tenacity import retry
from tenacity.before_sleep import before_sleep_log
from tenacity.retry import retry_if_exception
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

logger = logging.getLogger(__name__)

SVN_RETRY_WAIT_EXP_BASE = 10
SVN_RETRY_MAX_ATTEMPTS = 3


def is_retryable_svn_exception(exception):
    if isinstance(exception, SubversionException):
        return exception.args[0].startswith(
            (
                "Connection timed out",
                "Unable to connect to a repository at URL",
                "Error running context: The server unexpectedly closed the connection",
                "ra_serf: The server sent a truncated HTTP response body",
            )
        )
    return isinstance(exception, (ConnectionResetError, TimeoutError))


def svn_retry():
    return retry(
        retry=retry_if_exception(is_retryable_svn_exception),
        wait=wait_exponential(exp_base=SVN_RETRY_WAIT_EXP_BASE),
        stop=stop_after_attempt(max_attempt_number=SVN_RETRY_MAX_ATTEMPTS),
        before_sleep=before_sleep_log(logger, logging.DEBUG),
        reraise=True,
    )
