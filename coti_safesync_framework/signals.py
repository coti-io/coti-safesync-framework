import signal
import logging
from typing import Callable


def install_termination_handlers(stop_callback: Callable[[], None]) -> None:
    """
    Optional utility to install SIGTERM and SIGINT handlers that call stop_callback exactly once.
    
    Note: coti_safesync_framework does NOT automatically install signal handlers. This is an optional
    utility that users can call if they want. Users may also prefer to handle signals
    themselves or rely on their framework (e.g., FastAPI shutdown events).
    """
    def handler(signum, frame):
        logging.info("Received signal %s, initiating graceful shutdown", signum)
        stop_callback()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

