from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout
from PySide6.QtCore import Qt

class LoadingOverlay(QWidget):
    """Semi-transparent overlay with loading message."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Configure transparency
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Create layout
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Add loading label
        loading_label = QLabel("Loading...")
        loading_label.setStyleSheet("""
            QLabel {
                color: #666;
                background-color: rgba(255, 255, 255, 0.8);
                padding: 10px 20px;
                border-radius: 5px;
                font-weight: bold;
            }
        """)
        layout.addWidget(loading_label)
        
    def show(self):
        """Show and resize overlay to match parent widget."""
        if self.parentWidget():
            self.resize(self.parentWidget().size())
        super().show()
        
    def hide(self):
        """Hide the overlay."""
        super().hide() 