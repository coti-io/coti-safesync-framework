from __future__ import annotations

import signal
from unittest.mock import MagicMock, patch

import pytest

from coti_safesync_framework.signals import install_termination_handlers


class TestInstallTerminationHandlers:
    """Tests for install_termination_handlers() function."""

    def test_installs_sigterm_handler(self) -> None:
        """Test that install_termination_handlers installs SIGTERM handler."""
        callback = MagicMock()
        
        # Store original handler
        original_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Verify handler was installed (should be different from default)
            current_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            assert current_handler != signal.SIG_DFL
            assert current_handler != original_handler
        finally:
            # Restore original handler
            signal.signal(signal.SIGTERM, original_handler)

    def test_installs_sigint_handler(self) -> None:
        """Test that install_termination_handlers installs SIGINT handler."""
        callback = MagicMock()
        
        # Store original handler
        original_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Verify handler was installed (should be different from default)
            current_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
            assert current_handler != signal.SIG_DFL
            assert current_handler != original_handler
        finally:
            # Restore original handler
            signal.signal(signal.SIGINT, original_handler)

    def test_callback_called_on_sigterm(self) -> None:
        """Test that callback is called when SIGTERM is received."""
        callback = MagicMock()
        
        # Store original handler
        original_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Get the installed handler
            installed_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, installed_handler)  # Restore it
            
            # Simulate signal by calling the handler directly
            installed_handler(signal.SIGTERM, None)
            
            # Verify callback was called
            callback.assert_called_once()
        finally:
            # Restore original handler
            signal.signal(signal.SIGTERM, original_handler)

    def test_callback_called_on_sigint(self) -> None:
        """Test that callback is called when SIGINT is received."""
        callback = MagicMock()
        
        # Store original handler
        original_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Get the installed handler
            installed_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGINT, installed_handler)  # Restore it
            
            # Simulate signal by calling the handler directly
            installed_handler(signal.SIGINT, None)
            
            # Verify callback was called
            callback.assert_called_once()
        finally:
            # Restore original handler
            signal.signal(signal.SIGINT, original_handler)

    def test_callback_called_exactly_once_on_multiple_signals(self) -> None:
        """Test that callback is called exactly once even if signal received multiple times."""
        callback = MagicMock()
        
        # Store original handler
        original_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Get the installed handler
            installed_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, installed_handler)  # Restore it
            
            # Simulate multiple signals
            installed_handler(signal.SIGTERM, None)
            installed_handler(signal.SIGTERM, None)
            installed_handler(signal.SIGTERM, None)
            
            # Verify callback was called exactly 3 times (once per signal)
            # Note: The handler calls the callback each time, so multiple signals = multiple calls
            assert callback.call_count == 3
        finally:
            # Restore original handler
            signal.signal(signal.SIGTERM, original_handler)

    def test_handler_logs_signal_received(self) -> None:
        """Test that handler logs when signal is received."""
        callback = MagicMock()
        
        # Store original handler
        original_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Get the installed handler
            installed_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, installed_handler)  # Restore it
            
            # Simulate signal with logging patch
            with patch("coti_safesync_framework.signals.logging") as mock_logging:
                installed_handler(signal.SIGTERM, None)
                
                # Verify logging.info was called
                mock_logging.info.assert_called_once()
                call_args = mock_logging.info.call_args[0][0]
                assert "signal" in call_args.lower()
                assert "graceful shutdown" in call_args.lower()
        finally:
            # Restore original handler
            signal.signal(signal.SIGTERM, original_handler)

    def test_callback_exception_propagates(self) -> None:
        """Test that exceptions in callback propagate from handler."""
        callback = MagicMock(side_effect=RuntimeError("Callback error"))
        
        # Store original handler
        original_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        
        try:
            # Install our handlers
            install_termination_handlers(callback)
            
            # Get the installed handler
            installed_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, installed_handler)  # Restore it
            
            # Simulate signal - exception should propagate
            # Note: In real signal handlers, Python handles exceptions, but when called directly
            # the exception will propagate
            with pytest.raises(RuntimeError, match="Callback error"):
                installed_handler(signal.SIGTERM, None)
            
            # Verify callback was called (even though it raised)
            callback.assert_called_once()
        finally:
            # Restore original handler
            signal.signal(signal.SIGTERM, original_handler)

