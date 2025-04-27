import sys
import traceback
import logging
from PySide6.QtCore import QObject, Signal, QRunnable, Slot

logger = logging.getLogger(__name__)

# --- Worker Thread Setup --- #
class WorkerSignals(QObject):
    """
    Signals for worker thread communication.
    
    Signals:
        finished: Emitted when worker completes
        error: Emitted on exception with tuple (exctype, value, traceback)
        result: Emitted with the result data from the worker
    """
    finished = Signal()
    error = Signal(tuple)
    result = Signal(object)

class Worker(QRunnable):
    """
    Worker thread for background task execution.
    
    Inherits from QRunnable to handle worker thread setup, signals and wrap-up.
    
    Args:
        fn: The function to run in the worker thread
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
    """

    def __init__(self, fn, *args, **kwargs):
        super(Worker, self).__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        """Execute the target function with provided arguments."""
        logger.debug(f"Worker started for function: {self.fn.__name__} with args: {self.args} kwargs: {self.kwargs}")
        
        try:
            result = self.fn(*self.args, **self.kwargs)
        except Exception as e:
            logger.exception(f"Worker error in function {self.fn.__name__}")
            traceback_str = traceback.format_exc()
            self.signals.error.emit((type(e), e, traceback_str))
        else:
            logger.debug(f"Worker success for function: {self.fn.__name__}. Emitting result.")
            self.signals.result.emit(result)
        finally:
            logger.debug(f"Worker finished for function: {self.fn.__name__}")
            self.signals.finished.emit()
# --- End Worker Setup --- # 