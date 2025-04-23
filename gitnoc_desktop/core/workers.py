import sys
import traceback
import logging
from PySide6.QtCore import QObject, Signal, QRunnable, Slot

logger = logging.getLogger(__name__)

# --- Worker Thread Setup --- #
class WorkerSignals(QObject):
    '''
    Defines the signals available from a running worker thread.
    Supported signals are:
    finished
        No data
    error
        tuple (exctype, value, traceback.format_exc())
    result
        object data returned from processing, anything
    '''
    finished = Signal() # Signal emitted when thread finishes
    error = Signal(tuple) # Signal emitted with error info (type, value, traceback)
    result = Signal(object) # Signal emitted with the result object

class Worker(QRunnable):
    '''
    Worker thread
    Inherits from QRunnable to handler worker thread setup, signals and wrap-up.
    :param fn: The function callback to run on this worker thread. Supplied args and
                     kwargs will be passed through to the runner.
    :param args: Arguments to pass to the callback function
    :param kwargs: Keywords to pass to the callback function
    '''

    def __init__(self, fn, *args, **kwargs):
        super(Worker, self).__init__()
        # Store constructor arguments (re-used for processing)
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @Slot() # QtCore.Slot
    def run(self):
        '''
        Initialise the runner function with passed args, kwargs.
        '''
        logger.debug(f"Worker started for function: {self.fn.__name__} with args: {self.args} kwargs: {self.kwargs}")
        # Retrieve args/kwargs here; and fire processing using them
        try:
            result = self.fn(*self.args, **self.kwargs) # Pass args and kwargs
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